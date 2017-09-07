import logging
import os
import random
import socket
import threading
import time
import boto3
import errno

STORAGE = "/dev/shm/cache/"


class FIOStream():
  def __init__(self, istream, ostream):
    self.input_stream = istream
    self.output_stream = ostream

  def __iter__(self):
    return self

  def __next__(self):
    l = self.readline()
    if l == "":
      raise StopIteration
    return l.strip()

  def next(self):
    return self.__next__()

  def read(self, amt = None):
    return self.input_stream.read(amt)

  def readline(self):
    return self.input_stream.readline() 

  def write(self, s):
    self.output_stream.write(s)

  def close(self):
    self.output_stream.close()

class FOutputStream():
  def __init__(self, bucket, key, client, consistency = False, s3 = False):
    self.bucket = bucket
    self.key = key
    self.client = client
    self.shm_name = self.client.shm_name(bucket, key, consistency)
    self.fn = STORAGE + self.shm_name
    self.tmp_fn = STORAGE + "~~tmp~" + self.shm_name + "~" + str(self.client.seq)
    self.fd = open(self.tmp_fn, "wb")
    self.consistency = consistency
    self.modified = False
    self.snap_iso = False
    self.closed = False
    self.s3_uploaded = False
    self.s3 = s3

  def write(self, s):
    self.modified = True
    self.fd.write(s)

  def upload_s3(self):
    self.client.s3.upload_file(self.fn, self.bucket, self.key + ".%s" % self.client.seq)
    self.s3_uploaded = True
    self.client.log.debug("%s synced to s3." % self.fn)

  def close(self):
    if self.closed:
      return False
    self.closed = True
    self.fd.close()
    tmp_link = STORAGE + "lnk" + str(random.randint(0,1000000))
    os.symlink(self.tmp_fn, tmp_link)
    os.rename(tmp_link, self.fn)
    if self.client.replay_inputs is not None:
      msg = "0|failover_write_update|%s|%s|%s\n" % (self.shm_name, self.client.lambda_id[6:], self.client.lambda_id)
      self.client.master.sendall(msg)
      self.client.master.recv(1024)      
      return True 
    #normal execution
    else:
      if self.consistency:
        if not self.snap_iso:
          self.client.direct_unlock(self.bucket, self.key, True, self.modified)
      else:
        self.client.send_put(self.bucket, self.key, self.consistency)
        ack = self.client.master.recv(1024)
      if self.s3:
        threading.Thread(target=self.upload_s3).start()   
      return True

class FInputStream:
  def __init__(self, fn, bucket, key, client, size = None, consistency = False, s3 = False):
    self.name = None if fn is None else fn.replace(STORAGE, "")
    self.replay_stream = False
    self.client = client
    if self.client.replay_inputs is not None and self.name in self.client.replay_inputs:
      self.client.log.debug("name %s in replay inputs" % self.name)
      fn = STORAGE + "~~tmp~" + self.name + "~" + str(self.client.replay_inputs[self.name].version)
      self.replay_stream = True
    self.fn = fn
    self.closed = False
    self.bucket = bucket
    self.key = key
    self.consistency = consistency
    self.s3 = s3
    self.line_buf = None
    self.write_done = False
    if fn is not None:
      self.f = open(os.path.realpath(fn), "rb")
      self.size = size
      self.has_read = 0

  def read(self, amt = None):
    if self.fn is None:
      return ""
    if self.size is None:
      if amt is None:
        return self.f.read()
      else:
        return self.f.read(amt)
    else:
      buf = []
      curr = 0
      can_read = self.size - self.has_read
      amt = can_read if amt is None else min(amt, can_read)
      while curr < amt:
        data = self.f.read(amt - curr)
        if len(data) > 0:
          buf.append(data)
          curr += len(data)
        else:
          time.sleep(0.001)
      assert curr == amt
      self.has_read += curr
      return b''.join(buf) 

  def readline(self):
    if self.size is None:
      return self.f.readline()
    else:
      tmp = ""
      while True:
        rd = self.f.readline()
        if len(rd) > 0:
          tmp += rd
          if tmp[-1] == '\n' or self.has_read + len(tmp) == self.size:
              self.has_read += len(tmp)
              return tmp
        else:
          time.sleep(0.001)

  def get_file_name(self):
    rp = os.path.realpath(self.fn)
    if self.size is not None:
      while os.stat(rp).st_size != self.size:
        pass
    return rp

  def __iter__(self):
    return self

  def __next__(self):
    l = self.readline()
    if l == "":
      raise StopIteration
    return l.strip()

  def next(self):
    return self.__next__()

  def close(self):
    if self.replay_stream:
      return
    if not self.closed:
      self.closed = True
      if self.fn is not None:
        self.f.close()
      if self.consistency:
        self.client.direct_unlock(self.bucket, self.key, write = False, modified = True, s3 = self.s3)
      else:
        self.client.cache_reg(self.bucket, self.key, consistency = False)

  def set_socket_timeout(self, timeout):
    pass

  def __delete__(self):
    self.close()

class LockException(Exception):
  pass

class CacheClient:
  def __init__(self, master_ip = None, extra = {}):
    self.bg_read = False

    #loglevel = logging.DEBUG
    loglevel = logging.CRITICAL
    self.log = logging.getLogger("CacheClient")
    self.log.setLevel(loglevel)
    if not self.log.handlers:
      ch = logging.StreamHandler()
      ch.setLevel(loglevel)
      ch.setFormatter(logging.Formatter('%(funcName)s:%(lineno)s - %(message)s'))
      self.log.addHandler(ch)

    os.system("mkdir -p %s" % STORAGE)
    self.master_ip = master_ip if master_ip is not None else self.get_master_ip()
    self.s3 = boto3.client("s3")
    self.sockets = {}
    self.closed = False
    self.commit_write_done = False

    self.master = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    self.master.connect((self.master_ip, 1988))
    self.master.sendall("0|new_server|1222\n")
    self.seq = int(self.master.recv(1024).split("|")[3])
    self.lambda_id = "lambda" + str(self.seq) if "lambda_id" not in extra else extra["lambda_id"]
    self.replay_inputs = None if "replay_inputs" not in extra else extra["replay_inputs"]
    if self.replay_inputs is not None: print "replay inputs:", self.replay_inputs
    self.read_obj = []
    self.write_obj = []
    self.log.info("CacheClient Initialized id:%s" % self.lambda_id)

  def __del__(self):
    self.shutdown()


  def shutdown(self):
    if not self.closed:
      [f.close() for f in self.read_obj]
      [f.close() for f in self.write_obj]
      self.commit_write()
      for k,v in self.sockets.iteritems():
        v.close()
      self.master.close()
      self.closed = True
      self.log.info("CacheClient deleted")

  def get_master_ip(self):
    self.log.debug("Getting master ip address")
    with open('/dev/shm/master', 'r') as content_file:
      return content_file.read().strip()
    

  def shm_name(self, bucket, key, consistency):
    return ("~" if consistency else "") + bucket + "~" + key.replace("/", "~")

  def send_put(self, bucket, key, consistency = False):
    msg = "0|reg|" + self.shm_name(bucket, key, consistency)
    self.log.debug("sending msg %s" % msg)
    self.master.sendall(msg + "\n")


  def send_miss(self, bucket, key, consistency):
    msg = "0|lookup|" + self.shm_name(bucket, key, consistency)
    self.log.debug("sending msg %s" % msg)
    self.master.sendall(msg + "\n")

  def get_socket(self, server):
    if server not in self.sockets:
      conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
      conn.connect((server.split(":")[0], int(server.split(":")[1])))
      self.sockets[server] = conn
    return self.sockets[server]

  def peer_recv(self, conn, f, recvd, size, fn, tmp_fn):
    self.log.debug("receiving data from peer")
    while(recvd < size):
      data = conn.recv(size - recvd) #TODO use recv_into() may speedup transfer
      if len(data) == 0:
        raise Exception("conn err")
      if f is not None:
        f.write(data)
      recvd += len(data)
      #self.log.debug("%s/%s" % (recvd,size))
    #conn.close()
    self.log.debug("all data received, renaming")
    if f is not None:
      f.close()
      if not (self.replay_inputs is not None and fn.replace(STORAGE, "") in self.replay_inputs):
        tmp_link = STORAGE + "lnk" + str(random.randint(0,1000000))
        os.symlink(tmp_fn, tmp_link)
        os.rename(tmp_link, fn)
    self.log.debug("peer recv done")

  def peer_read(self, server, key):
    self.log.debug("peer_read from lambda %s, server %s, key %s" % (self.lambda_id, server, key))
    conn = self.get_socket(server)
    #print "socket connected"
    if self.replay_inputs is not None and key in self.replay_inputs:
      fetch_key = "~~tmp~" + key + "~" + str(self.replay_inputs[key].version)
      self.log.debug("key %s in replay inputs, fetch with %s" % (key, fetch_key))
    else:
      fetch_key = key
    fn = STORAGE + key
    self.log.debug("send get %s" % fetch_key)
    conn.sendall("get|%s;" % (fetch_key))
    recvd = 0
    while True:
      data = conn.recv(256)
      #print "got msg", data
      if data.find(";"):
        received = data.split(";")
        header = received[0].split("|")
        tmp_key = header[1]
        size = int(header[2])
        tmp_fn = STORAGE + tmp_key
        try:
          fp = os.open(tmp_fn, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except OSError as e:
          if e.errno == errno.EEXIST:
            self.log.debug("file already opened")
            f = None
          else:
            raise
        else:
          f = os.fdopen(fp, "wb")
          f.write(received[1])
        recvd += len(received[1])
        break
    self.log.debug("header received: %s" % header)
    if self.bg_read and size > 100 * 1024:
      threading.Thread(target=self.peer_recv, args=(conn, f, recvd, size, fn, tmp_fn )).start()
    else:
      self.peer_recv(conn, f, recvd, size, fn, tmp_fn)
    self.log.debug("peer read done. size %s" % size)
    return (size, tmp_key)

  def s3_recv(self, f, obj, bucket, key, consistency, size):
    self.log.debug("s3 receive")
    total_read = 0
    while True:
      data = obj["Body"].read(min(100 * 1024, size - total_read))
      if len(data) == 0:
        break
      total_read += len(data)
      f.write(data)
    f.close()
    self.log.debug("all data received")
    if not consistency:
      self.log.debug("sending put to master")
      self.send_put(bucket, key, False)
      self.log.debug("waiting master to ack")
      ack = self.master.recv(1024)
      self.log.debug("master acked")
    self.log.debug("s3_recv done")

  def s3_read(self, name, bucket, key, consistency):
    self.log.debug("s3_read name %s, bucket %s, key %s, consist %s" % (name, bucket, key, consistency))
    fn = STORAGE + name
    tmp_fn = STORAGE + "~~tmp~" +name + "~" + str(self.seq)
    obj = self.s3.get_object(Bucket = bucket, Key = key)
    size = obj['ContentLength']
    f = open(tmp_fn, "wb")
    if size < 100 * 1024:
      data = obj["Body"].read()
      f.write(data)
      f.close()
    else:
      threading.Thread(target=self.s3_recv, args=(f, obj, bucket, key, consistency, size)).start()
    self.log.debug("creating symlink")
    tmp_link = STORAGE + "lnk" + str(random.randint(0,1000000))
    os.symlink(tmp_fn, tmp_link)
    os.rename(tmp_link, fn)
    self.log.debug("s3_read done")
    return size

  def recv_miss_ret_direct(self, fn):
    self.log.debug("waiting for miss ack")
    ack = self.master.recv(1024).strip()
    self.log.debug("miss ack received: %s" % ack)
    addrs = ack.split("|")[2].split(";")
    size = None
    tmp_key = fn
    if "" == addrs[0]:
      return None
    elif "use_local" in addrs[0]:
      return_msg = "success:use_local"
    else:
      (size, tmp_key) = self.peer_read(random.choice(addrs), fn)
      return_msg = "success:from_peer"  
    return ["miss_ret", "/host", return_msg, size, tmp_key]    


  def read_file(self, name, bucket, key, size = None, consistency = False, s3 = False):
    if name is None:
      return FInputStream(None, bucket, key, self, size = size, consistency = consistency, s3 = s3)
    else:
      return FInputStream(STORAGE + name, bucket, key, self, size = size, consistency = consistency, s3 = s3)

  def direct_lock(self, bucket, key, write = False, max_duration = 300, s3 = False, snap = False, read_write = False):
    name = self.shm_name(bucket, key, True)
    rw = "write" if write else "read"
    check_loc = "check_loc" if read_write else "no_check_loc"
    use_s3 = "s3" if s3 else "nos3"
    snap_iso = "snap" if snap else "no_snap"
    version = "recent" if self.replay_inputs is None or name not in self.replay_inputs else self.replay_inputs[name].version
    msg = "0|consistent_lock|%s|%s|%s|%s|%s|%s|%s|%s\n" % (rw, name, self.lambda_id, max_duration, use_s3, snap_iso, check_loc, version)
    while True:
      self.log.debug("sending direct lock: %s" % msg[0:-1])
      self.master.sendall(msg)
      ack = self.master.recv(1024).strip().split("|")
      self.log.debug("direct lock ack: %s" % ack)
      if ack[2].startswith("success"):
        return (True, ack[4], rw if not s3 else ack[3])
      elif ack[2].startswith("exception"):
        if "key_seq_num_err" in ack[2]:
          raise LockException(ack[2])
        else:
          raise Exception(ack[2])
      elif ack[2].startswith("fail") and snap:
        raise LockException(ack[2])
      time.sleep(0.1)
    return (False, "", rw)

  def direct_unlock(self, bucket, key, write = False, modified = True, s3 = False):
    rw = "write" if write or s3 else "read"
    msg = "0|consistent_unlock|%s|%s|%s|%s\n" % (rw, self.shm_name(bucket, key, True), self.lambda_id, "1" if modified else "0")
    self.log.debug("sending direct unlock: %s" % msg[0:-1])
    self.master.sendall(msg)
    ack = self.master.recv(1024).strip().split("|")
    self.log.debug("direct unlock ack %s" % ack)
    return ack[2] != "fail"

  def commit_write(self):
    if self.commit_write_done:
      return
    def get_cmd(os):
      return "0|consistent_unlock|write|%s|%s|%s" % (os.shm_name, os.client.lambda_id, "1" if os.modified else "0")
    cmds = [get_cmd(os) for os in self.write_obj if os.snap_iso]
    if len(cmds) > 0:
      for i in range(0, len(cmds), 100):
        curr_cmds = cmds[i:i+100]
        msg = "/".join(curr_cmds)
        self.log.debug("sending write unlock: %s" % msg)
        self.master.sendall(msg + "\n")
        ack = self.master.recv(1024 * 1024)
        assert ack[-1] == '\n'
        acks = ack.split("/")
        self.log.debug("write unlock ack %s" % ack[0:-1])
        self.log.debug("len(curr_cmds) = %s, len(acks) = %s" % (len(curr_cmds), len(acks)))
        assert len(curr_cmds) == len(acks)
    else:
      self.log.debug("Nothing to commit")
    self.commit_write_done = True

  def cache_reg(self, bucket, key, consistency):
    fn = self.shm_name(bucket, key, consistency)
    msg = "0|cache|%s\n" % fn
    self.log.debug("sending cache reg: %s" % msg)
    self.master.sendall(msg)
    ack = self.master.recv(1024).strip().split("|")
    self.log.debug("cache ack: %s" % ack)
    return ack[3] == "success"

  def get_name_type(self, bucket, key, body, consistency):
    assert type(bucket) is str
    assert type(key) is str
    if isinstance(body, bytes):# or isinstance(body, bytearray)
      body_type = "bytes"
    elif hasattr(body, 'read'):
      body_type = "fd"
    else:
      assert False
    name = self.shm_name(bucket, key, consistency)
    return (name, body_type)


  def get(self, bucket, key, consistency = False, loc_hint = None, s3 = False, lock_type = "read"):
    assert type(bucket) is str
    assert type(key) is str
    name = self.shm_name(bucket, key, consistency)
    self.log.debug("get bucket %s, key %s, name %s" % (bucket, key, name))
    if s3 and (lock_type is not None and lock_type == "write"):
      self.log.debug("calling s3_read")
      size = self.s3_read(name, bucket, key, consistency)
      return self.read_file(name, bucket, key, size, consistency, s3 = True)
    elif loc_hint is None:
      always_query_master = True
      for i in range(1):
        ret = None if (consistency and i == 0) or always_query_master else self.read_file(name, bucket, key) 
        self.log.debug("ret = %s" % ret)
        if ret is not None:
          return ret
        else:
          self.send_miss(bucket, key, consistency)
          size = None
          parts = self.recv_miss_ret_direct(name)
          if parts is None:
            if s3:
              size = self.s3_read(name, bucket, key, consistency)
            else:
              return self.read_file(None, bucket, key, size, consistency)
          else:
            size = parts[3]
            name = parts[4] #tmp_key
      return self.read_file(name, bucket, key, size, consistency, s3 = s3)
    else:
      self.log.debug("loc_hint = %s" % loc_hint)
      addrs = loc_hint.split(";")
      size = None
      if "" == addrs[0]:
        return self.read_file(None, bucket, key, size, consistency)
      elif "use_local" in addrs[0]:
        pass
      else:
        (size, tmp_key) = self.peer_read(addrs[0], name)
        name = tmp_key
      return self.read_file(name, bucket, key, size, consistency)


  def open(self, bucket, key, mode, s3 = False, consistency = True, snap_iso = False):
    assert mode in ["r", "w", "rw"]
    self.log.debug("open bucket %s, key %s, mode %s, s3 %s, consist %s, snap %s" % (bucket, key, mode, s3, consistency, snap_iso))
    if mode == "r":
      if consistency:
        (state, loc, lock_type) = self.direct_lock(bucket, key, write = False, max_duration = 1000, s3 = s3, snap = snap_iso)
        self.log.debug("direct lock result: state %s, loc %s, lock_type %s" % (state, loc, lock_type))
      else:
        loc = None
        lock_type = None
      ret = self.get(bucket, key, consistency, loc_hint = loc, s3 = s3, lock_type = lock_type)
      self.read_obj.append(ret)
      return ret
    elif mode == "w":
      if consistency:
        self.direct_lock(bucket, key, write = True, max_duration = 1000, snap = snap_iso)
      ret = FOutputStream(bucket, key, self, consistency = consistency, s3 = s3)
      ret.snap_iso = snap_iso
      self.write_obj.append(ret)
      return ret
    else:
      if consistency:
        (state, loc, lock_type) = self.direct_lock(bucket, key, write = True, max_duration = 1000, s3 = s3, snap = snap_iso, read_write = True)
        self.log.debug("direct lock result: state %s, loc %s, lock_type %s" % (state, loc, lock_type))
      else:
        loc = None
        lock_type = None
      istream = self.get(bucket, key, consistency, loc_hint = loc, s3 = s3, lock_type = lock_type)
      ostream = FOutputStream(bucket, key, self, consistency = consistency, s3 = s3)
      ostream.snap_iso = snap_iso
      self.write_obj.append(ostream)
      return FIOStream(istream, ostream)
      

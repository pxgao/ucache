import logging
import os
import random
import socket
import threading
import time
import boto3


STORAGE = "/dev/shm/cache/"


class FOutputStream():
  def __init__(self, bucket, key, write_s3, client, consistency = False, s3 = False):
    self.bucket = bucket
    self.key = key
    self.write_s3 = write_s3
    self.client = client
    key_name = self.client.shm_name(bucket, key, consistency)
    self.fn = STORAGE + key_name
    self.tmp_fn = STORAGE + "~~tmp~" +key_name + "~" + str(random.randint(0,1000000))
    self.fd = open(self.tmp_fn, "wb")
    self.consistency = consistency
    self.modified = False
    self.closed = False

  def write(self, s):
    self.modified = True
    self.fd.write(s)

  def close(self):
    if self.closed:
      return False
    self.closed = True
    self.fd.close()
    tmp_link = STORAGE + "lnk" + str(random.randint(0,1000000))
    os.symlink(self.tmp_fn, tmp_link)
    os.rename(tmp_link, self.fn)
    if self.consistency:
      self.client.direct_unlock(self.bucket, self.key, True, self.modified)
    else:
      self.client.send_put(self.bucket, self.key, self.write_s3, self.consistency)
      ack = self.client.master.recv(1024)
    return True

class FInputStream:
  def __init__(self, fn, bucket, key, client, size = None, consistency = False, s3 = False):
   self.fn = fn
   self.closed = False
   self.bucket = bucket
   self.key = key
   self.consistency = consistency
   self.client = client
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
      assert curr == amt
      self.has_read += curr
      return b''.join(buf) 

  def readline(self):
    if self.size is None:
      return self.f.readline()
    else:
      tmp = ""
      while True:
        tmp += self.f.readline()
        if (len(tmp) > 0 and tmp[-1] == '\n') or self.has_read + len(tmp) == self.size:
            self.has_read += len(tmp)
            return tmp

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

class CacheClient:
  def __init__(self, master_ip = None):
    self.bg_read = False

    loglevel = logging.DEBUG
    #loglevel = logging.CRITICAL
    self.log = logging.getLogger("CacheClient")
    self.log.setLevel(loglevel)
    ch = logging.StreamHandler()
    ch.setLevel(loglevel)
    ch.setFormatter(logging.Formatter('%(funcName)s:%(lineno)s - %(message)s'))
    self.log.addHandler(ch)

    os.system("mkdir -p %s" % STORAGE)
    self.master_ip = master_ip if master_ip is not None else self.get_master_ip()
    self.lambda_id = "lambda" + str(random.randint(0,1000000))
    self.s3 = boto3.client("s3")
    self.sockets = {}
    self.closed = False

    self.master = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    self.master.connect((self.master_ip, 1988))
    self.master.send("0|new_server|1222\n")
    self.master.recv(1024)
    self.log.info("CacheClient Initialized id:%s" % self.lambda_id)

  def __del__(self):
    self.shutdown()


  def shutdown(self):
    if not self.closed:
      for k,v in self.sockets.iteritems():
        v.close()
      self.master.close()
      self.closed = True
      self.log.info("CacheClient deleted")

  def get_master_ip(self):
    self.log.debug("Getting master ip address")
    with open('/home/ubuntu/conf/master', 'r') as content_file:
      return content_file.read().strip()

  def shm_name(self, bucket, key, consistency):
    return ("~" if consistency else "") + bucket + "~" + key.replace("/", "~")

  def send_put(self, bucket, key, write_s3, consistency = False):
    msg = "0|reg|" + self.shm_name(bucket, key, consistency)
    self.log.debug("sending msg %s" % msg)
    self.master.send(msg + "\n")


  def send_miss(self, bucket, key, consistency):
    msg = "0|lookup|" + self.shm_name(bucket, key, consistency)
    self.log.debug("sending msg %s" % msg)
    self.master.send(msg + "\n")

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
      f.write(data)
      recvd += len(data)
      #print recvd,"/",size
    #conn.close()
    self.log.debug("all data received, renaming")
    f.close()
    tmp_link = STORAGE + "lnk" + str(random.randint(0,1000000))
    os.symlink(tmp_fn, tmp_link)
    os.rename(tmp_link, fn)
    self.log.debug("peer recv done")

  def peer_read(self, server, key):
    self.log.debug("peer_read from lambda %s, server %s, key %s" % (self.lambda_id, server, key))
    conn = self.get_socket(server)
    #print "socket connected"
    fn = STORAGE + key
    tmp_fn = STORAGE + "~~tmp~" +key + "~" + str(random.randint(0,1000000))
    f = open(tmp_fn, "w")
    self.log.debug("send get %s" % key)
    conn.sendall("get|%s;" % (key))
    recvd = 0
    while True:
      data = conn.recv(256)
      #print "got msg", data
      if data.find(";"):
        received = data.split(";")
        header = received[0]
        size = int(header.split("|")[2])
        f.write(received[1])
        recvd += len(received[1])
        break
    if self.bg_read and size > 100 * 1024:
      threading.Thread(target=self.peer_recv, args=(conn, f, recvd, size, fn, tmp_fn )).start()
    else:
      self.peer_recv(conn, f, recvd, size, fn, tmp_fn)
    self.log.debug("peer read done. size %s" % size)
    return size

  def s3_recv(self, f, obj, bucket, key, consistency):
    self.log.debug("s3 receive")
    while True:
      data = obj["Body"].read(1024 * 1024)
      if len(data) == 0:
        break
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
    tmp_fn = STORAGE + "~~tmp~" +name + "~" + str(random.randint(0,1000000))
    obj = self.s3.get_object(Bucket = bucket, Key = key)
    size = obj['ContentLength']
    f = open(tmp_fn, "w")
    if size < 100 * 1024:
      data = obj["Body"].read()
      f.write(data)
      f.close()
    else:
      threading.Thread(target=self.s3_recv, args=(f, obj, bucket, key, consistency)).start()
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
    if "" == addrs[0]:
      return None
    elif "use_local" in addrs[0]:
      return_msg = "success:use_local"
    else:
      size = self.peer_read(addrs[0], fn)
      return_msg = "success:from_peer"  
    return ["miss_ret", "/host", return_msg, size]    

  def send_write_s3(self, bucket, key, consistency):
    self.send("write_s3|%s|%s|%s|%s" % (self.self_msg_queue, bucket, key, "1" if consistency else "0"))


  def read_file(self, name, bucket, key, size = None, consistency = False, s3 = False):
    if name is None:
      return FInputStream(None, bucket, key, self, size = size, consistency = consistency, s3 = s3)
    else:
      return FInputStream(STORAGE + name, bucket, key, self, size = size, consistency = consistency, s3 = s3)

  def direct_lock(self, bucket, key, write = False, max_duration = 300, s3 = False):
    rw = "write" if write else "read"
    use_s3 = "s3" if s3 else "nos3"
    msg = "0|consistent_lock|%s|%s|%s|%s|%s\n" % (rw, self.shm_name(bucket, key, True), self.lambda_id, max_duration, use_s3)
    while True:
      self.log.debug("sending direct lock: %s" % msg)
      self.master.send(msg)
      ack = self.master.recv(1024).strip().split("|")
      self.log.debug("direct lock ack: %s" % ack)
      if ack[2].startswith("success"):
        return (True, ack[-1], rw if not s3 else ack[3])
      elif ack[2].startswith("exception"):
        raise Exception(ack[2])
      time.sleep(0.1)
    return (False, "", rw)

  def direct_unlock(self, bucket, key, write = False, modified = True, s3 = False):
    rw = "write" if write or s3 else "read"
    msg = "0|consistent_unlock|%s|%s|%s|%s\n" % (rw, self.shm_name(bucket, key, True), self.lambda_id, "1" if modified else "0")
    self.log.debug("sending direct unlock: %s" % msg)
    self.master.send(msg)
    ack = self.master.recv(1024).strip().split("|")
    self.log.debug("direct unlock ack %s" % msg)
    return ack[2] != "fail"

  def cache_reg(self, bucket, key, consistency):
    fn = self.shm_name(bucket, key, consistency)
    msg = "0|cache|%s\n" % fn
    self.log.debug("sending cache reg: %s" % msg)
    self.master.send(msg)
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
        size = self.peer_read(addrs[0], name)
      return self.read_file(name, bucket, key, size, consistency)


  def open(self, bucket, key, mode, s3 = False, consistency = False):
    assert mode == "w" or mode == "r"
    self.log.debug("open bucket %s, key %s, mode %s, s3 %s, consist %s" % (bucket, key, mode, s3, consistency))
    if mode == "r":
      if consistency:
        (state, loc, lock_type) = self.direct_lock(bucket, key, write = False, max_duration = 1000, s3 = s3)
        self.log.debug("direct lock result: state %s, loc %s, lock_type %s" % (state, loc, lock_type))
      else:
        loc = None
        lock_type = None
      return self.get(bucket, key, consistency, loc_hint = loc, s3 = s3, lock_type = lock_type)
    else:
      assert type(bucket) is str
      assert type(key) is str
      if consistency:
        self.direct_lock(bucket, key, write = True, max_duration = 1000)
      return FOutputStream(bucket, key, s3, self, consistency)



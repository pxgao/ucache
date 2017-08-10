
#include "masterworker.h"
#include "master.h"
#include "log.h"
#include <ctime>
#include <string>
#include <iomanip>
#include <sys/socket.h>
#include <unistd.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <boost/algorithm/string.hpp>
#include <vector>
#include <netinet/tcp.h>

using namespace std;

MasterWorker::MasterWorker(Master &master, int socket)
  : master(master)
  , socket(socket)
{
}


void MasterWorker::init()
{
  ++master.threads_counter;
  int yes = 1;
  if (setsockopt(socket, IPPROTO_TCP, TCP_NODELAY, &yes, sizeof(int)))
    LOG_ERROR << "error: unable to set socket option";
  // We want non-blocking reads
  //fcntl(socket, F_SETFL, fcntl(socket, F_GETFL, 0) | O_NONBLOCK);
  struct sockaddr_in addr;
  socklen_t addr_size = sizeof(struct sockaddr_in);
  int res = getpeername(socket, (struct sockaddr *)&addr, &addr_size);
  char ip_str[20];
  strcpy(ip_str, inet_ntoa(addr.sin_addr));
  ip = ip_str;
  LOG_DEBUG << "Connection from " << ip;
}

void MasterWorker::exit()
{
  close(socket);
  --master.threads_counter;
  LOG_INFO << "client lost (total = " << master.threads_counter << ")";
  pthread_exit(nullptr);
}


void MasterWorker::run() {
  init();
  char buffer[1024*1024];

  int read_size;
  while ((read_size = read(socket, buffer, sizeof(buffer))) > 0) {
    for (int i = 0; i < read_size; ++i)
      if (buffer[i] == '\n') {
        buffer[i] = '\0';
        string msg(buffer);
        LOG_DEBUG << "Received msg: " << msg;
        string ret = handle_msg(msg);
        LOG_DEBUG << "Sending msg: " << ret;
        string response = ret + "\n";
        assert(response.size() < 1024 * 1024);
        if (write(socket, response.c_str(), response.size()) < 0) {
          std::cerr << "error: unable to write socket " << socket << std::endl;
        }
      }
  }
  LOG_DEBUG << "Connection disconnected";
  exit();
}

string MasterWorker::handle_msg(string msg) {
  string ret;
  vector<string> parts;
  boost::split(parts, msg, boost::is_any_of("|"));
  string id = parts[0];
  parts.erase(parts.begin());
  if(parts[0] == "new_server")
    ret = handle_new_server(parts);
  else if(parts[0] == "reg")
    ret = handle_reg(parts);
  else if(parts[0] == "cache")
    ret = handle_cache(parts);
  else if(parts[0] == "uncache")
    ret = handle_uncache(parts);
  else if(parts[0] == "lookup")
    ret = handle_lookup(parts);
  else if(parts[0] == "delete")
    ret = handle_delete(parts);
  else if(parts[0] == "consistent_lock")
    ret = handle_consistent_lock(parts);
  else if(parts[0] == "consistent_unlock")
    ret = handle_consistent_unlock(parts);
  else if(parts[0] == "consistent_delete")
    ret = handle_consistent_delete(parts);
  else {
    LOG_ERROR << "error msg type";
    ret = string("");
  }
  return id + "|" + ret;
}

string MasterWorker::handle_new_server(vector<string> parts) {
  port = parts[1];
  addr = ip + ":" + port; //TODO addr should be cacheserver addr, not lambda addr
  return "new_server_ack|" + parts[1];
}

string MasterWorker::handle_reg(vector<string> parts){
  bool ret = master.registry.reg_key(parts[1], addr);
  return "reg_ack|" + parts[1] + "|" + (ret?"success":"fail");
}

string MasterWorker::handle_cache(vector<string> parts){
  bool ret = master.registry.cache_key(parts[1], addr);
  return "cache_ack|" + parts[1] + "|" + (ret?"success":"fail");
}

string MasterWorker::handle_uncache(vector<string> parts){
  
  bool ret = master.registry.uncache_key(parts[1], addr);
  return "uncache_ack|" + parts[1] + "|" + (ret?"success":"fail");
}

string MasterWorker::handle_lookup(vector<string> parts) {
  vector<string> keys, result;
  boost::split(keys, parts[1], boost::is_any_of("/"));
  for (string k : keys){
    result.push_back( master.registry.get_location(k, addr) );
  }
  return "lookup_ack|" + boost::algorithm::join(result, "/");
} 

string MasterWorker::handle_consistent_lock(vector<string> parts) {
  //consistent_lock|read/write|key|lambda|duration_in_sec|use_s3
  string pre_check_loc = "";
  if (parts[5] == "s3")
    pre_check_loc = master.registry.get_location(parts[2], addr);

  if (parts[1] == "write" || (parts[5] == "s3" && pre_check_loc == "")) {
    string ret = master.registry.consistent_write_lock(parts[2], addr, parts[3], atoi(parts[4].c_str()));
    return "consistent_lock_ack|" + ret + "|write";
  } else if (parts[1] == "read") {
    string ret = master.registry.consistent_read_lock(parts[2], addr, parts[3], atoi(parts[4].c_str()));
    string loc = pre_check_loc==""?master.registry.get_location(parts[2], addr):pre_check_loc;
    return "consistent_lock_ack|" + ret + "|read|" + loc;
  } else {
    return "consistent_lock_ack|wrong_cmd";
  }
}

string MasterWorker::handle_consistent_unlock(vector<string> parts) {
  //consistent_unlock|read/write|key|lambda|modified
  if (parts[1] == "write") {
    string ret = master.registry.consistent_write_unlock(parts[2], addr, parts[3], parts[4][0] == '1');
    return "consistent_unlock_ack|" + ret;
  } else if (parts[1] == "read") {
    string ret = master.registry.consistent_read_unlock(parts[2], addr, parts[3], parts[4][0] == '1');
    return "consistent_unlock_ack|" + ret;
  } else {
    return "consistent_unlock_ack|wrong_cmd";
  }
}

string MasterWorker::handle_consistent_delete(vector<string> parts) {
  //consistent_delete|key
  string ret = master.registry.consistent_delete(parts[1]);
  return "consistent_delete_ack|" + ret;
}

string MasterWorker::handle_delete(vector<string> parts) {
  //delete|key
  string ret = master.registry.delete_key(parts[1]);
  return "delete_ack|" + ret;
}

void *MasterWorker::pthread_helper(void * worker) {
  static_cast<MasterWorker *>(worker)->run();
  return nullptr;
}


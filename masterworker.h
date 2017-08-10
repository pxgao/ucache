#ifndef MASTERWORKER_H
# define MASTERWORKER_H
#include <iostream>
#include <string>
#include <vector>

using namespace std;

class Master;

class MasterWorker
{
public:
  MasterWorker(Master &master, int socket);
  void run();
  static void *pthread_helper(void * worker);

protected:
  void init();
  void exit();
  string handle_msg(string);
  string handle_new_server(vector<string>);
  string handle_cache(vector<string>);
  string handle_uncache(vector<string>);
  string handle_reg(vector<string>);
  string handle_lookup(vector<string>);
  string handle_consistent_lock(vector<string>);
  string handle_consistent_unlock(vector<string>);
  string handle_consistent_delete(vector<string>);
  string handle_delete(vector<string>);

  Master &master;
  int socket;
  string ip;
  string port;
  string addr;
private:
  MasterWorker(const MasterWorker &); // No copies!
};


#endif // !WORKER_HH

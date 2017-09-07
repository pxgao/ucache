#ifndef OBJWORKER_H
# define OBJWORKER_H
#include <iostream>
#include <string>
#include <vector>

using namespace std;

class ObjServer;

class ObjWorker
{
public:
  ObjWorker(ObjServer &cacheserver, int socket);
  void run();
  static void *pthread_helper(void * worker);

protected:
  void exit();
  ObjServer &objserver;
  int socket;
  string remote_ip;
private:
  ObjWorker(const ObjWorker &); // No copies!
  int reply(int socket, const char* msg, int size, int retry = 1, bool silent = false);
  void handle_get(vector<string> parts);
  void handle_put(vector<string> parts, char* remaining_start, int remaining_size);
  string get_remote_ip(int socket);

};


#endif

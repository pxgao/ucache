#ifndef READERWRITERLOCK_H
#define READERWRITERLOCK_H

#include <chrono>
#include <ctime>
#include <string>
#include <map>
#include <mutex>
#include <boost/thread.hpp>
#include <boost/thread/shared_mutex.hpp>

using namespace std;

class ReaderWriterLock {

public:
  string reader_lock(string, int duration);
  string reader_unlock(string);
  string writer_lock(string, int duration);
  string writer_unlock(string);
  //TODO: implement stale object collection
private:
  boost::shared_mutex lock;
  map<string, chrono::time_point<std::chrono::system_clock>> owners;
  bool write_mode;
};

#endif

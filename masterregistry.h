#ifndef MASTERREGISTRY_H
#define MASTERREGISTRY_H

#include <mutex>
#include <set>
#include <map>
#include "readerwriterlock.h"
#include <boost/thread.hpp>
#include <boost/thread/shared_mutex.hpp>

using namespace std;

class KeyEntry {
public:
  KeyEntry(string key, bool consistency = false);
  bool cache_key(string location);
  bool uncache_key(string location);
  void clear();
  string get_location();
  bool is_cached(string location);
  const bool consistency;
  const string key;
  ReaderWriterLock consistent_lock;
 
private:
  boost::shared_mutex lock;
  set<string> locations;
};


class MasterRegistry {
public: 
  MasterRegistry();
  ~MasterRegistry();
  bool reg_key(string key, string location);
  bool cache_key(string key, string location);
  bool uncache_key(string key, string location);
  void clear_key(string key);
  string get_location(string key, string from);

  string consistent_write_lock(string key, string location, string lambda_id, int max_duration);
  string consistent_write_unlock(string key, string location, string lambda_id, bool modified);

  string consistent_read_lock(string key, string location, string lambda_id, int max_duration);
  string consistent_read_unlock(string key, string location, string lambda_id, bool modified);
  
  string consistent_delete(string key);
  string delete_key(string key);

private:
  map<string, KeyEntry*> keys;
  boost::shared_mutex lock;
};

#endif

#include "masterregistry.h"
#include "log.h"

KeyEntry::KeyEntry(string key, bool consistency):
  key(key),
  consistency(consistency)
{
}

bool KeyEntry::cache_key(string location) {
  lock.lock();
  LOG_DEBUG << "Key " << key << " cached at " << location;
  locations.insert(location);
  lock.unlock();
  return true;
}

bool KeyEntry::uncache_key(string location) {
  lock.lock();
  LOG_DEBUG << "Key " << key << " removed from " << location;
  locations.erase(location);
  lock.unlock();
  return true;
}

void KeyEntry::clear() {
  lock.lock();
  LOG_DEBUG << "Key " << key << " cleared";
  locations.clear();
  lock.unlock();
}

string KeyEntry::get_location() {
  lock.lock_shared();
  string ret("");
  int x = 0;
  for (auto& i : locations) {
    ret += i + ";";
    x++;
    if (x >=3)
      break;
  }
  lock.unlock_shared();
  LOG_DEBUG << "Key " << key << " is cached at " << ret;
  return ret;
}

bool KeyEntry::is_cached(string location) {
  lock.lock_shared();
  bool res = locations.find(location) != locations.end();
  lock.unlock_shared();
  LOG_DEBUG << "Key " << key << " is cache at " << location << "? " << res;
  return res;
}

MasterRegistry::MasterRegistry() {
  LOG_INFO << "Init MasterRegistry";
}

MasterRegistry::~MasterRegistry() {
  LOG_INFO << "Deleting MasterRegistry";
  for (auto& kvp : keys)
    delete kvp.second;
}



bool MasterRegistry::reg_key(string key, string location) {
  assert(key.at(0) != '~');
  bool ret = false;
  LOG_DEBUG << "reg_key " << key << " at location " << location;
  lock.lock_shared();
  auto key_entry = keys.find(key);
  lock.unlock_shared();
  if (key_entry != keys.end()){
    if(key_entry->second->consistency) {
      LOG_DEBUG << key << " is a consistent key";
      ret = false;
    } else {
      lock.lock();
      LOG_DEBUG << key << " exist, clear key location cache";
      key_entry->second->clear();
      lock.unlock();
      ret = true;
    }
  } else {
    LOG_DEBUG << key << " does not exist, creating new entry";
    lock.lock();
    keys[key] = new KeyEntry(key);
    LOG_DEBUG << key << " entry created";
    lock.unlock();
    ret = true;
  }

  if (ret) {
    lock.lock_shared();
    auto key_entry_2 = keys.find(key);
    lock.unlock_shared();
    LOG_DEBUG << key << " location " << location << " to be cached";
    key_entry_2->second->cache_key(location);
  }
  return ret;
}

bool MasterRegistry::cache_key(string key, string location) {
  lock.lock_shared();
  auto key_entry = keys.at(key);
  lock.unlock_shared();
  LOG_DEBUG << key << " location to be cached";
  return key_entry->cache_key(location);
}

bool MasterRegistry::uncache_key(string key, string location) {
  lock.lock_shared();
  auto key_entry = keys.at(key);
  lock.unlock_shared();
  LOG_DEBUG << key << " location to be uncached";
  return key_entry->uncache_key(location);
}

void MasterRegistry::clear_key(string key) {
  lock.lock();
  LOG_DEBUG << key << " entry to be erased";
  keys.erase(key);
  lock.unlock();
}

string MasterRegistry::get_location(string input_key, string from) {
  bool consistency = input_key[0] == '~';
  string key = consistency?input_key.substr(1):input_key;
  lock.lock_shared();
  auto key_entry = keys.find(key);
  lock.unlock_shared();
  if (key_entry == keys.end()) {
    LOG_DEBUG << "input_key " << input_key << " key " << key << ", key is not found";
    return "";
  } else {
    if (key_entry->second->is_cached(from)) {
      LOG_DEBUG << "input_key " << input_key << " key " << key << ", key exist on local machien, returning use_local";
      return "use_local";
    } else {
      LOG_DEBUG << "input_key " << input_key << " key " << key << ", query key entry for location";
      return key_entry->second->get_location();
    }
  }
}

string MasterRegistry::consistent_read_lock(string input_key, string location, string lambda_id, int max_duration) {
  assert(input_key.at(0) == '~');
  string key = input_key.substr(1);
  string uri = location + "/" + lambda_id;
  string ret;
  lock.lock_shared();
  auto key_entry = keys.find(key);
  lock.unlock_shared();
  if (key_entry != keys.end()) {
    if(key_entry->second->consistency) {
      ret = key_entry->second->consistent_lock.reader_lock(uri, max_duration);
    } else {
      ret = "exception: not_consistent_key";
    }
  } else {
    ret = "exception: key_not_found";
  }
  LOG_DEBUG << "Return: " << ret;
  return ret;
}

string MasterRegistry::consistent_read_unlock(string input_key, string location, string lambda_id, bool modified){
  assert(input_key.at(0) == '~');
  string key = input_key.substr(1);
  string uri = location + "/" + lambda_id;
  string ret;
  lock.lock_shared();
  auto key_entry = keys.find(key);
  lock.unlock_shared();
  if (key_entry == keys.end()) {
    LOG_ERROR << input_key << " does not exsit when unlock";
    ret ="exception: key_not_exist";
  } else {
    if (modified){
      LOG_DEBUG << "key modified, after lock, caching key location";
      key_entry->second->cache_key(location);
    }
    ret = key_entry->second->consistent_lock.reader_unlock(uri);
  }
  LOG_DEBUG << "Return: " << ret;
  return ret;
} 




string MasterRegistry::consistent_write_lock(string input_key, string location, string lambda_id, int max_duration) {
  assert(input_key.at(0) == '~');
  string key = input_key.substr(1);
  string uri = location + "/" + lambda_id;
  string ret;
  lock.lock_shared();
  auto key_entry = keys.find(key);
  lock.unlock_shared();
  if (key_entry != keys.end()) {
    if(key_entry->second->consistency) {
      ret = key_entry->second->consistent_lock.writer_lock(uri, max_duration);
    } else {
      ret = "exception: not_consistent_key";
    }
  } else {
    //TODO: possible that keys[key] is not empty now....
    //solution, must hold lock when delete
    lock.lock();
    auto value = new KeyEntry(key, true);
    keys[key] = value;
    value->consistent_lock.writer_lock(uri, max_duration);
    lock.unlock();
    ret = "success";
  }
  LOG_DEBUG << "Return: " << ret;
  return ret;
}

string MasterRegistry::consistent_delete(string input_key) {
  assert(input_key.at(0) == '~');
  string key = input_key.substr(1);
  string ret;
  lock.lock_shared();
  auto key_entry = keys.find(key);
  lock.unlock_shared();
  if (key_entry != keys.end()) {
    if(key_entry->second->consistency) {
      ret = key_entry->second->consistent_lock.writer_lock("null", 65536);
      if (ret == "success") {
        lock.lock();
        delete keys[key];
        keys.erase(key);
        lock.unlock();
      }
    } else {
      ret = "exception: not_consistent_key";
    }
  } else {
    ret = "exception: key_not_found";
  }
  LOG_DEBUG << "Return: " << ret;
  return ret;
}

string MasterRegistry::delete_key(string key) {
  assert(key.at(0) != '~');
  string ret;
  lock.lock_shared();
  auto key_entry = keys.find(key);
  lock.unlock_shared();
  if (key_entry != keys.end()) {
    if(key_entry->second->consistency) {
      ret = "exception: key_is_consistent";
    } else {
      lock.lock();
      keys.erase(key);
      lock.unlock();
      ret = "success";
    }
  } else {
    ret = "exception: key_not_found";
  }
  LOG_DEBUG << "Return: " << ret;
  return ret;
}



string MasterRegistry::consistent_write_unlock(string input_key, string location, string lambda_id, bool modified){
  assert(input_key.at(0) == '~');
  string key = input_key.substr(1);
  string uri = location + "/" + lambda_id;
  string ret;
  lock.lock_shared();
  auto key_entry = keys.find(key);
  lock.unlock_shared();
  if (key_entry == keys.end()) {
    LOG_ERROR << input_key << " does not exsit when unlock";
    ret = "exception: key_not_exist";
  } else {
    if(modified) {
      LOG_DEBUG << "key modified, after lock, caching key location";
      key_entry->second->clear();
      key_entry->second->cache_key(location);
    }
    ret = key_entry->second->consistent_lock.writer_unlock(uri);
  }
  LOG_DEBUG << "return: " << ret;
  return ret;
} 

















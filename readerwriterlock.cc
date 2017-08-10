#include "readerwriterlock.h"
#include "log.h"

string ReaderWriterLock::reader_lock(string reader, int max_duration) {
  string ret = "fail";
  lock.lock();
  LOG_DEBUG << "reader " << reader << " num owner = " << owners.size() << " write = " << write_mode;
  if (owners.size() == 0) {
    owners[reader] = chrono::system_clock::now() + chrono::seconds(max_duration);
    write_mode = false;
    ret = "success";
  } else if (!write_mode) {
    if (owners.find(reader) == owners.end()) {
      owners[reader] = chrono::system_clock::now() + chrono::seconds(max_duration);
      ret = "success";
    } else {
      ret = "exception: you already have the lock";
    }
  }
  lock.unlock();
  return ret;
}

string ReaderWriterLock::reader_unlock(string reader) {
  string ret = "fail";
  lock.lock();
  LOG_DEBUG << "reader " << reader << " num owner = " << owners.size() << " write = " << write_mode;
  if (owners.size() == 0) {
    LOG_DEBUG << reader << " attempts to unlock, but owner.size() == 0";
    ret = "exception: empty owner list";
  } else if (write_mode) {
    LOG_DEBUG << reader << " attempts to unlock, but the lock is in write mode";
    ret = "exception: lock in write mode";
  } else if (owners.find(reader) == owners.end()) {
    LOG_DEBUG << reader << " attempts to unlock, but could not find reader";
    ret = "exception: can't find reader";
  } else {
    owners.erase(reader);
    ret = "success";
  }
  lock.unlock();
  return ret;
}

string ReaderWriterLock::writer_lock(string writer, int max_duration) {
  string ret = "fail";
  lock.lock();
  LOG_DEBUG << "writer " << writer << " num owner = " << owners.size() << " write = " << write_mode;
  if (owners.size() == 0) {
    owners[writer] = chrono::system_clock::now() + chrono::seconds(max_duration);
    write_mode = true;
    ret = "success";
  }
  lock.unlock();
  return ret;
}

string ReaderWriterLock::writer_unlock(string writer) {
  string ret = "fail";
  lock.lock();
  LOG_DEBUG << "writer " << writer << " num owner = " << owners.size() << " write = " << write_mode;
  if (owners.size() == 0) {
    LOG_DEBUG << writer << " attempts to unlock, but owner.size() == 0";
    ret = "exception: empty owner list";
  } else if (!write_mode) {
    LOG_DEBUG << writer << " attempts to unlock, but the lock is in read mode";
    ret = "exception: lock in read mode";
  } else if (owners.find(writer) == owners.end()) {
    LOG_DEBUG << writer << " attempts to unlock, but could not find writer";
    ret = "exception: can't find writer";
  } else {
    owners.erase(writer);
    ret = "success";
  }
  lock.unlock();
  return ret;
}

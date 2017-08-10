#ifndef LOG_H
#define LOG_H

#define BOOST_LOG_DYN_LINK 1 // necessary when linking the boost_log library dynamically

#include <boost/log/trivial.hpp>
#include <boost/log/sources/global_logger_storage.hpp>
#include <stdio.h>
#include <string.h>
// the logs are also written to LOGFILE
#define LOGFILE "logfile.log"

// just log messages with severity >= SEVERITY_THRESHOLD are written
//#define SEVERITY_THRESHOLD logging::trivial::trace
#define SEVERITY_THRESHOLD logging::trivial::debug
//#define SEVERITY_THRESHOLD logging::trivial::fatal

// register a global logger
BOOST_LOG_GLOBAL_LOGGER(logger, boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>)

// just a helper macro used by the macros below - don't use it in your code
#define LOG(severity) BOOST_LOG_SEV(logger::get(),boost::log::trivial::severity)

// ===== log macros =====
#define __FILENAME__ (strrchr(__FILE__, '/') ? strrchr(__FILE__, '/') + 1 : __FILE__)
#define PREFIX __FILENAME__ << ":" << __LINE__ << " (" << __FUNCTION__ << ") - "
#define LOG_TRACE   LOG(trace) << PREFIX
#define LOG_DEBUG   LOG(debug) << PREFIX
#define LOG_INFO    LOG(info) << PREFIX
#define LOG_WARNING LOG(warning) << PREFIX
#define LOG_ERROR   LOG(error) << PREFIX
#define LOG_FATAL   LOG(fatal) << PREFIX

# define DIE(M, ...) do { \
  printf("DIE %s:%d (%s) -- ", __FILENAME__, __LINE__, __FUNCTION__); \
  printf(M,##__VA_ARGS__); \
  printf("\n"); \
  exit(1); \
} while(0);

#endif

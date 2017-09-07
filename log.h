#ifndef LOG_H
#define LOG_H


#include <stdio.h>
#include <string.h>
#include <ostream>
#include <iostream>
#include <sstream>

#define TRACE 5
#define DEBUG 4
#define INFO 3
#define WARNING 2
#define ERROR 1
#define FATAL 0

#define SEVERITY_THRESHOLD ERROR

extern std::ostream null_stream;

class NullBuffer : public std::streambuf
{
public:
  int overflow(int c);
};

class LogStream : public std::stringstream
{
public:
  ~LogStream() {operator<<(std::endl); std::cout << str(); }
};

// ===== log macros =====
#define __FILENAME__ (strrchr(__FILE__, '/') ? strrchr(__FILE__, '/') + 1 : __FILE__)
#define PREFIX __FILENAME__ << ":" << __LINE__ << " (" << __FUNCTION__ << ") - "

#define LOG_X LogStream() << PREFIX

#if SEVERITY_THRESHOLD >= TRACE
# define LOG_TRACE LOG_X
#else
# define LOG_TRACE null_stream
#endif

#if SEVERITY_THRESHOLD >= DEUBG
# define LOG_DEBUG LOG_X
#else
# define LOG_DEBUG null_stream
#endif

#if SEVERITY_THRESHOLD >= INFO
# define LOG_INFO LOG_X
#else
# define LOG_INFO null_stream
#endif

#if SEVERITY_THRESHOLD >= WARNING
# define LOG_WARNING LOG_X
#else
# define LOG_WARNING null_stream
#endif

#if SEVERITY_THRESHOLD >= ERROR
# define LOG_ERROR LOG_X
#else
# define LOG_ERROR null_stream
#endif

#if SEVERITY_THRESHOLD >= FATAL
# define LOG_FATAL LOG_X
#else
# define LOG_FATAL null_stream
#endif

# define DIE(M, ...) do { \
  printf("DIE %s:%d (%s) -- ", __FILENAME__, __LINE__, __FUNCTION__); \
  printf(M,##__VA_ARGS__); \
  printf("\n"); \
  exit(1); \
} while(0);

#endif

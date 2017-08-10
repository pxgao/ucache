#include "cacheserver.h"

int main(int argc, char** argv) {
  CacheServer c(argv[1]);
  c.run();
  return 0;
}

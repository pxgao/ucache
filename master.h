#ifndef MASTER_H
# define MASTER_H

#include <cstddef>
#include <cstdint>
#include <atomic>
#include <list>
#include <iostream>
#include "masterworker.h"
#include "masterregistry.h"
#include <unistd.h>
#include <vector>
#include "epollmasterworker.h"
#define USE_EPOLL 1
#define EPOLL_THREADS 128

class MasterWorker;

class Master
{
public:

    Master(std::uint16_t port);
    ~Master(); // No virtual needed since no inheritance as of now.

    void run();
    MasterRegistry registry;

protected:
    bool init();
    void cleanup();
    int make_socket_non_blocking(int);

    vector<EpollMasterWorker*> epoll_master_workers;
    std::uint16_t port;
    std::list<MasterWorker *> workers; // One worker per client
    int socket_fd;
};


#endif // !SERVER_HH

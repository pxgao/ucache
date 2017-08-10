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


class MasterWorker;

class Master
{
public:

    Master(std::uint16_t port);
    ~Master(); // No virtual needed since no inheritance as of now.

    void run();
    volatile std::atomic<unsigned int> threads_counter;
    MasterRegistry registry;

protected:
    bool init();
    void cleanup();

    std::uint16_t port;
    std::list<MasterWorker *> workers; // One worker per client
    int socket_fd;
};


#endif // !SERVER_HH

#include "master.h"
#include "masterworker.h"
#include "log.h"

#include <iostream>
#include <cerrno>
#include <ctime>
#include <pthread.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/tcp.h>

Master::Master(unsigned short port)
    : threads_counter(0)
    , port(port)
    , workers()
    , socket_fd(-1)
{
}

Master::~Master()
{
    for (auto i = workers.begin(); i != workers.end(); ++i)
        delete *i;
}

bool Master::init()
{
    if ((socket_fd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP)) < 0)
      DIE("error: cannot create socket");

    int yes = 1;
    if (setsockopt(socket_fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(int)))
      DIE("error: unable to set socket option");
    if (setsockopt(socket_fd, IPPROTO_TCP, TCP_NODELAY, &yes, sizeof(int)))
      DIE("error: unable to set socket option");

    sockaddr_in address;
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(port);
    if (bind(socket_fd, (sockaddr *)(&address), sizeof(sockaddr_in)) < 0)
      DIE("error: cannot bind socket to port ");

    if (listen(socket_fd, port) < 0)
      DIE("error: cannot listen on port ");

    LOG_INFO << "listening on port " << port;

    return true;
}

void Master::cleanup() {
  if (socket_fd >= 0) {
    close(socket_fd);
    socket_fd = -1;
    sleep(2);
  }
}

void Master::run() {

    if (!init()) {
        cleanup();
        return;
    }

    for (;;) {
        int worker_socket = accept(socket_fd, nullptr, nullptr);
        if (worker_socket < 0) {
            if (errno == EINTR) {
                LOG_ERROR << "stopping server, waiting for threads to terminate";
                break;
            } else {
                LOG_ERROR << "error: unable to accept client " << strerror(errno);
            }
        } else {
            LOG_DEBUG << "new client (total = " << (threads_counter + 1) << ")";
            MasterWorker * worker = new MasterWorker(*this, worker_socket);
            workers.push_back(worker);

            pthread_t thread;
            if (pthread_create(&thread, 0, &MasterWorker::pthread_helper, worker)) {
                LOG_ERROR << "error: unable to create thread";
                continue;
            }
            if (pthread_detach(thread)) {
                LOG_ERROR << "error: unable to detach thread";
                continue;
            }
        }
    }
    cleanup();
}


int main() {
  Master m(1988);
  m.run();
}

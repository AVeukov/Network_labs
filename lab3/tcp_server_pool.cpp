#include "protocol.hpp"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <pthread.h>
#include <unistd.h>

#include <cstdio>
#include <deque>
#include <iostream>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

static std::string peer_to_string(const sockaddr_in &addr) {
  char ip[INET_ADDRSTRLEN] = {0};
  inet_ntop(AF_INET, &addr.sin_addr, ip, sizeof(ip));
  std::ostringstream oss;
  oss << ip << ":" << ntohs(addr.sin_port);
  return oss.str();
}

struct ClientInfo {
  int fd = -1;
  std::string nick;
  std::string peer;
};

static pthread_mutex_t g_clients_mtx = PTHREAD_MUTEX_INITIALIZER;
static std::unordered_map<int, ClientInfo> g_clients;

static pthread_mutex_t g_queue_mtx = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t g_queue_cv = PTHREAD_COND_INITIALIZER;
static std::deque<int> g_queue;

static void broadcast_text(const std::string &text, int sender_fd) {
  pthread_mutex_lock(&g_clients_mtx);
  std::vector<int> fds;
  fds.reserve(g_clients.size());
  for (const auto &kv : g_clients)
    fds.push_back(kv.first);
  pthread_mutex_unlock(&g_clients_mtx);

  for (const auto &fd : fds) {
    if (fd == sender_fd)
      continue;
    (void)send_message(fd, MSG_TEXT, text);
  }
}

static void remove_client(int fd) {
  pthread_mutex_lock(&g_clients_mtx);
  auto it = g_clients.find(fd);
  if (it != g_clients.end()) {
    g_clients.erase(it);
  }
  pthread_mutex_unlock(&g_clients_mtx);
}

static void *worker_main(void *) {
  while (true) {
    int fd = -1;
    pthread_mutex_lock(&g_queue_mtx);
    while (g_queue.empty()) {
      pthread_cond_wait(&g_queue_cv, &g_queue_mtx);
    }
    fd = g_queue.front();
    g_queue.pop_front();
    pthread_mutex_unlock(&g_queue_mtx);

    sockaddr_in peer{};
    socklen_t peer_len = sizeof(peer);
    if (getpeername(fd, reinterpret_cast<sockaddr *>(&peer), &peer_len) != 0) {
      ::close(fd);
      continue;
    }
    const std::string peer_str = peer_to_string(peer);

    Message hello{};
    if (!recv_message(fd, hello) || hello.type != MSG_HELLO) {
      ::close(fd);
      continue;
    }

    ClientInfo info;
    info.fd = fd;
    info.peer = peer_str;
    info.nick = hello.payload;

    std::string welcome = "Welcome " + peer_str;
    if (!send_message(fd, MSG_WELCOME, welcome)) {
      ::close(fd);
      continue;
    }

    pthread_mutex_lock(&g_clients_mtx);
    g_clients.emplace(fd, info);
    pthread_mutex_unlock(&g_clients_mtx);

    while (true) {
      Message in{};
      if (!recv_message(fd, in)) {
        std::cerr << "Client disconnected: " << info.nick << " [" << info.peer
                  << "]\n";
        break;
      }

      if (in.type == MSG_TEXT) {
        std::string line =
            info.nick + " [" + info.peer + "]: " + std::string(in.payload);
        broadcast_text(line, fd);
      } else if (in.type == MSG_PING) {
        (void)send_message(fd, MSG_PONG, "");
      } else if (in.type == MSG_BYE) {
        (void)send_message(fd, MSG_BYE, "");
        std::cerr << "Client disconnected: " << info.nick << " [" << info.peer
                  << "]\n";
        break;
      } else {
        std::cout << "Unknown message type\n";
      }
    }

    remove_client(fd);
    ::close(fd);
  }
  return nullptr;
}

int main(int argc, char **argv) {
  uint16_t port = 5000;
  if (argc >= 2)
    port = static_cast<uint16_t>(std::stoi(argv[1]));

  int srv = ::socket(AF_INET, SOCK_STREAM, 0);
  if (srv < 0) {
    std::perror("socket");
    return 1;
  }

  int yes = 1;
  ::setsockopt(srv, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));

  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_ANY);
  addr.sin_port = htons(port);
  if (::bind(srv, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) < 0) {
    std::perror("bind");
    ::close(srv);
    return 1;
  }

  if (::listen(srv, 64) < 0) {
    std::perror("listen");
    ::close(srv);
    return 1;
  }

  constexpr int kPoolSize = 10;
  std::vector<pthread_t> threads(kPoolSize);
  for (int i = 0; i < kPoolSize; i++) {
    if (pthread_create(&threads[i], nullptr, &worker_main, nullptr) != 0) {
      std::perror("pthread_create");
      ::close(srv);
      return 1;
    }
    pthread_detach(threads[i]);
  }

  std::cerr << "Server listening on port " << port << " (pool=" << kPoolSize
            << ")\n";

  while (true) {
    sockaddr_in peer{};
    socklen_t peer_len = sizeof(peer);
    int cli = ::accept(srv, reinterpret_cast<sockaddr *>(&peer), &peer_len);
    if (cli < 0) {
      if (errno == EINTR)
        continue;
      std::perror("accept");
      continue;
    }

    pthread_mutex_lock(&g_queue_mtx);
    g_queue.push_back(cli);
    pthread_mutex_unlock(&g_queue_mtx);
    pthread_cond_signal(&g_queue_cv);
  }

  ::close(srv);
  return 0;
}

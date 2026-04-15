#include "protocol.hpp"

#include <pthread.h>
#include <unistd.h>

#include <atomic>
#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <string>
#include <sys/select.h>

struct SharedState {
    std::atomic<int> sock_fd{-1};
    std::atomic<bool> stop{false};
};

static bool connect_to(const std::string& host, uint16_t port, int& out_fd) {
    out_fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (out_fd < 0) return false;

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    if (inet_pton(AF_INET, host.c_str(), &addr.sin_addr) != 1) {
        ::close(out_fd);
        out_fd = -1;
        return false;
    }

    if (::connect(out_fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        ::close(out_fd);
        out_fd = -1;
        return false;
    }
    return true;
}

static void close_socket(std::atomic<int>& sock_fd) {
    int fd = sock_fd.exchange(-1);
    if (fd >= 0) ::close(fd);
}

static void* recv_thread_main(void* arg) {
    auto* st = static_cast<SharedState*>(arg);
    while (!st->stop.load()) {
        int fd = st->sock_fd.load();
        if (fd < 0) {
            ::usleep(50 * 1000);
            continue;
        }

        Message in{};
        if (!recv_message(fd, in)) {
            close_socket(st->sock_fd);
            continue;
        }

        if (in.type == MSG_TEXT) {
            std::cout << in.payload << "\n";
            std::cout.flush();
        } else if (in.type == MSG_PONG) {
            std::cout << "PONG\n";
            std::cout.flush();
        } else if (in.type == MSG_BYE) {
            close_socket(st->sock_fd);
        }
    }
    return nullptr;
}

int main(int argc, char** argv) {
    std::string host = "127.0.0.1";
    uint16_t port = 5000;
    std::string nick = "user";

    if (argc >= 2) host = argv[1];
    if (argc >= 3) port = static_cast<uint16_t>(std::stoi(argv[2]));
    if (argc >= 4) nick = argv[3];

    SharedState st;
    pthread_t recv_th{};
    if (pthread_create(&recv_th, nullptr, &recv_thread_main, &st) != 0) {
        std::perror("pthread_create");
        return 1;
    }
    pthread_detach(recv_th);

    while (!st.stop.load()) {
        if (st.sock_fd.load() < 0) {
            int fd = -1;
            if (!connect_to(host, port, fd)) {
                std::cerr << "Reconnect failed; retrying in 2s...\n";
                ::sleep(2);
                continue;
            }

            if (!send_message(fd, MSG_HELLO, nick)) {
                ::close(fd);
                ::sleep(2);
                continue;
            }

            Message msg{};
            if (!recv_message(fd, msg) || msg.type != MSG_WELCOME) {
                ::close(fd);
                ::sleep(2);
                continue;
            }
            std::cout << "Connected\n" << msg.payload << "\n";
            std::cout.flush();

            st.sock_fd.store(fd);
        }

        std::string line;
        if (!std::getline(std::cin, line)) {
            st.stop.store(true);
            int fd = st.sock_fd.load();
            if (fd >= 0) (void)send_message(fd, MSG_BYE, "");
            close_socket(st.sock_fd);
            break;
        }

        int fd = st.sock_fd.load();
        if (fd < 0) {
            std::cerr << "Not connected yet...\n";
            continue;
        }

        if (line == "/ping") {
            (void)send_message(fd, MSG_PING, "");
        } else if (line == "/quit") {
            (void)send_message(fd, MSG_BYE, "");
            st.stop.store(true);
            close_socket(st.sock_fd);
            break;
        } else {
            (void)send_message(fd, MSG_TEXT, line);
        }
    }

    return 0;
}


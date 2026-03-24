#include <iostream>
#include <vector>
#include <queue>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <cstring>
#include <unistd.h>
#include <arpa/inet.h>

#define PORT 12345
#define THREAD_POOL_SIZE 10
#define MAX_PAYLOAD 1024

struct Message {
    uint32_t length;
    uint8_t type;
    char payload[MAX_PAYLOAD];
};

enum {
    MSG_HELLO = 1,
    MSG_WELCOME,
    MSG_TEXT,
    MSG_PING,
    MSG_PONG,
    MSG_BYE
};

std::queue<int> clientQueue;
std::mutex queueMutex;
std::condition_variable queueCV;

std::vector<int> clients;
std::mutex clientsMutex;

void enqueue(int fd) {
    std::lock_guard<std::mutex> lock(queueMutex);
    clientQueue.push(fd);
    queueCV.notify_one();
}

int dequeue() {
    std::unique_lock<std::mutex> lock(queueMutex);
    queueCV.wait(lock, [] { return !clientQueue.empty(); });

    int fd = clientQueue.front();
    clientQueue.pop();
    return fd;
}

void addClient(int fd) {
    std::lock_guard<std::mutex> lock(clientsMutex);
    clients.push_back(fd);
}

void removeClient(int fd) {
    std::lock_guard<std::mutex> lock(clientsMutex);
    clients.erase(std::remove(clients.begin(), clients.end(), fd), clients.end());
}

void broadcast(const Message& msg, int sender) {
    std::lock_guard<std::mutex> lock(clientsMutex);
    for (int fd : clients) {
        if (fd != sender) {
            send(fd, &msg, sizeof(msg), 0);
        }
    }
}

void worker() {
    while (true) {
        int client_fd = dequeue();
        Message msg{};

        // HELLO
        if (recv(client_fd, &msg, sizeof(msg), 0) <= 0) {
            close(client_fd);
            continue;
        }

        if (msg.type == MSG_HELLO) {
            std::cout << "Client: " << msg.payload << " connected\n";

            Message welcome{};
            welcome.type = MSG_WELCOME;
            send(client_fd, &welcome, sizeof(welcome), 0);
        }

        addClient(client_fd);

        while (true) {
            int bytes = recv(client_fd, &msg, sizeof(msg), 0);
            if (bytes <= 0) break;

            switch (msg.type) {
                case MSG_TEXT:
                    broadcast(msg, client_fd);
                    break;

                case MSG_PING: {
                    Message pong{};
                    pong.type = MSG_PONG;
                    send(client_fd, &pong, sizeof(pong), 0);
                    break;
                }

                case MSG_BYE:
                    close(client_fd);
                    removeClient(client_fd);
                    return;
            }
        }

        std::cout << "Client disconnected\n";
        close(client_fd);
        removeClient(client_fd);
    }
}

int main() {
    int server_fd;
    sockaddr_in addr{};

    server_fd = socket(AF_INET, SOCK_STREAM, 0);

    addr.sin_family = AF_INET;
    addr.sin_port = htons(PORT);
    addr.sin_addr.s_addr = INADDR_ANY;

    bind(server_fd, (sockaddr*)&addr, sizeof(addr));
    listen(server_fd, 10);

    std::vector<std::thread> threads;
    for (int i = 0; i < THREAD_POOL_SIZE; i++) {
        threads.emplace_back(worker);
        threads.back().detach();
    }

    std::cout << "Server started\n";

    while (true) {
        int client_fd = accept(server_fd, nullptr, nullptr);
        enqueue(client_fd);
    }

    return 0;
}
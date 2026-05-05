#include <arpa/inet.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <vector>
#include <string>
#include <string.h>
#include "protocol.h"

#define PORT 8080

struct Client {
    int sock;
    char name[32];
};

std::vector<Client> clients;
pthread_mutex_t mtx = PTHREAD_MUTEX_INITIALIZER;

void osi_trace(const char* action) {
    printf("{L7-App} %s\n", action);
    printf("{L4-Trans} segment ok\n");
    printf("{L3-Net} routing stable\n");
    printf("{L2-Link} frame delivered\n");
    printf("{L1-Phys} bits transferred\n");
}

int find_client(const char* name) {
    for (size_t i = 0; i < clients.size(); i++) {
        if (strcmp(clients[i].name, name) == 0)
            return clients[i].sock;
    }
    return -1;
}

void broadcast(Message &m) {
    pthread_mutex_lock(&mtx);
    for (auto &c : clients) {
        msg_send(c.sock, &m);
    }
    pthread_mutex_unlock(&mtx);
}

void* client_thread(void *arg) {
    int sock = *(int*)arg;
    free(arg);

    Message m{};
    msg_recv(sock, &m);

    if (m.type != MSG_AUTH) return NULL;

    pthread_mutex_lock(&mtx);
    clients.push_back({sock, ""});
    strcpy(clients.back().name, m.payload);
    pthread_mutex_unlock(&mtx);

    printf("(sys) user connected: %s\n", m.payload);

    while (1) {
        if (msg_recv(sock, &m) < 0) break;

        osi_trace("incoming packet");

        if (m.type == MSG_TEXT) {
            broadcast(m);
        }
        else if (m.type == MSG_PRIVATE) {
            char nick[32], text[256];
            sscanf(m.payload, "%s %[^\n]", nick, text);

            int s = find_client(nick);
            if (s != -1) {
                Message out{};
                out.type = MSG_PRIVATE;
                snprintf(out.payload, sizeof(out.payload),
                         "(pm) %s => %s: %s", m.payload, nick, text);
                msg_send(s, &out);
            }
        }
        else if (m.type == MSG_PING) {
            Message pong{};
            pong.type = MSG_PONG;
            strcpy(pong.payload, "pong");
            msg_send(sock, &pong);
        }
    }

    close(sock);
    return NULL;
}

int main() {
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(PORT);
    addr.sin_addr.s_addr = INADDR_ANY;

    bind(server_fd, (sockaddr*)&addr, sizeof(addr));
    listen(server_fd, 5);

    printf("Server started (OSI demo)\n");

    while (1) {
        int client = accept(server_fd, NULL, NULL);

        int *p = (int*)malloc(sizeof(int));
        *p = client;

        pthread_t t;
        pthread_create(&t, NULL, client_thread, p);
        pthread_detach(t);
    }
}
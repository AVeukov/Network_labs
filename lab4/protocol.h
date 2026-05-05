#pragma once
#include <stdint.h>
#include <unistd.h>
#include <string.h>

#define MAX_PAYLOAD 1024

typedef struct {
    uint8_t type;
    uint16_t length;
    char payload[MAX_PAYLOAD];
} Message;

enum {
    MSG_HELLO = 1,
    MSG_WELCOME,
    MSG_TEXT,
    MSG_PING,
    MSG_PONG,
    MSG_BYE,
    MSG_AUTH,
    MSG_PRIVATE,
    MSG_ERROR,
    MSG_SERVER_INFO
};

static inline int send_all(int sock, const void *buf, size_t len) {
    size_t sent = 0;
    while (sent < len) {
        ssize_t n = send(sock, (char*)buf + sent, len - sent, 0);
        if (n <= 0) return -1;
        sent += n;
    }
    return 0;
}

static inline int recv_all(int sock, void *buf, size_t len) {
    size_t rec = 0;
    while (rec < len) {
        ssize_t n = recv(sock, (char*)buf + rec, len - rec, 0);
        if (n <= 0) return -1;
        rec += n;
    }
    return 0;
}

static inline int msg_send(int sock, Message *m) {
    return send_all(sock, m, sizeof(Message));
}

static inline int msg_recv(int sock, Message *m) {
    return recv_all(sock, m, sizeof(Message));
}
#include <arpa/inet.h>
#include <pthread.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include "protocol.h"

int sock;

void* recv_thread(void*) {
    Message m{};
    while (msg_recv(sock, &m) > 0) {
        if (m.type == MSG_TEXT) {
            printf("%s", m.payload);
        } else if (m.type == MSG_PRIVATE) {
            printf("%s\n", m.payload);
        } else if (m.type == MSG_PONG) {
            printf("(server) pong\n");
        }
    }
    return NULL;
}

int main() {
    sock = socket(AF_INET, SOCK_STREAM, 0);

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(8080);
    inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);

    connect(sock, (sockaddr*)&addr, sizeof(addr));

    char name[32];
    printf("Enter nickname: ");
    scanf("%s", name);
    getchar();

    Message auth{};
    auth.type = MSG_AUTH;
    strcpy(auth.payload, name);
    msg_send(sock, &auth);

    pthread_t t;
    pthread_create(&t, NULL, recv_thread, NULL);

    while (1) {
        char input[256];
        fgets(input, sizeof(input), stdin);

        Message m{};

        if (strncmp(input, "/ping", 5) == 0) {
            m.type = MSG_PING;
        }
        else if (strncmp(input, "/w ", 3) == 0) {
            m.type = MSG_PRIVATE;
            strcpy(m.payload, input + 3);
        }
        else {
            m.type = MSG_TEXT;
            snprintf(m.payload, sizeof(m.payload), "%s >> %s", name, input);
        }

        msg_send(sock, &m);
    }
}
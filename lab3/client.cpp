#include <iostream>
#include <thread>
#include <cstring>
#include <unistd.h>
#include <arpa/inet.h>

#define PORT 12345
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

int sock;
std::string nickname;

void receiver() {
    Message msg{};

    while (true) {
        int bytes = recv(sock, &msg, sizeof(msg), 0);
        if (bytes <= 0) {
            std::cout << "Disconnected. Reconnecting...\n";
            close(sock);
            exit(0);
        }

        switch (msg.type) {
            case MSG_TEXT:
                std::cout << msg.payload;
                break;

            case MSG_PONG:
                std::cout << "PONG\n";
                break;
        }
    }
}

void connectToServer() {
    sockaddr_in addr{};

    sock = socket(AF_INET, SOCK_STREAM, 0);

    addr.sin_family = AF_INET;
    addr.sin_port = htons(PORT);
    inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);

    while (connect(sock, (sockaddr*)&addr, sizeof(addr)) < 0) {
        sleep(2);
    }

    Message hello{};
    hello.type = MSG_HELLO;
    strncpy(hello.payload, nickname.c_str(), MAX_PAYLOAD);
    send(sock, &hello, sizeof(hello), 0);
}

// ===== MAIN =====
int main() {
    std::cout << "Enter nickname: ";
    std::getline(std::cin, nickname);

    connectToServer();

    std::thread recvThread(receiver);

    while (true) {
        std::string input;
        std::getline(std::cin, input);

        Message msg{};

        if (input == "/ping") {
            msg.type = MSG_PING;
        } else if (input == "/quit") {
            msg.type = MSG_BYE;
            send(sock, &msg, sizeof(msg), 0);
            close(sock);
            break;
        } else {
            msg.type = MSG_TEXT;
            std::string full = nickname + ": " + input + "\n";
            strncpy(msg.payload, full.c_str(), MAX_PAYLOAD);
        }

        send(sock, &msg, sizeof(msg), 0);
    }

    recvThread.join();
    return 0;
}
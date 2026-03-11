#include <iostream>
#include <arpa/inet.h>
#include <unistd.h>
#include "messages.h"

#define PORT 54000
#define SERVER_IP "127.0.0.1"

int main(){
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0){
        perror("socket");
        return 1;
    }

    sockaddr_in serv_addr{};
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(PORT);
    inet_pton(AF_INET, SERVER_IP, &serv_addr.sin_addr);

    if (connect(sock, (sockaddr *)&serv_addr, sizeof(serv_addr)) < 0){
        perror("connect");
        return 1;
    }

    std::cout << "Connected" << std::endl;
    Message msg{};
    std::string nick;
    std::cout << "Enter nickname: ";
    std::getline(std::cin, nick);

    prepareMessage(msg, MSG_HELLO, nick);
    send(sock, &msg, sizeof(msg), 0);

    recv(sock, &msg, sizeof(msg), 0);
    if (msg.type == MSG_WELCOME){
        std::cout << msg.payload << std::endl;
    }

    while (true){
        std::string input;
        std::cout << "> ";
        std::getline(std::cin, input);

        if (input == "/ping"){
            prepareMessage(msg, MSG_PING, "");
        }
        else if (input == "/quit"){
            prepareMessage(msg, MSG_BYE, "");
            send(sock, &msg, sizeof(msg), 0);
            break;
        }
        else{
            prepareMessage(msg, MSG_TEXT, input);
        }

        send(sock, &msg, sizeof(msg), 0);

        if (msg.type == MSG_PING){
            recv(sock, &msg, sizeof(msg), 0);
            if (msg.type == MSG_PONG)
                std::cout << "PONG" << std::endl;
        }
    }

    close(sock);
    std::cout << "Disconnected" << std::endl;
    return 0;
}

#include <iostream>
#include <arpa/inet.h>
#include <unistd.h>
#include "messages.h"

#define PORT 54000

int main(){
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0){
        perror("socket");
        return 1;
    }

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(PORT);

    if (bind(server_fd, (sockaddr *)&address, sizeof(address)) < 0){
        perror("bind");
        return 1;
    }

    listen(server_fd, 1);
    std::cout << "Server started on port " << PORT << std::endl;

    sockaddr_in client_addr{};
    socklen_t addrlen = sizeof(client_addr);

    int client_socket = accept(server_fd, (sockaddr *)&client_addr, &addrlen);
    if (client_socket < 0){
        perror("accept");
        return 1;
    }

    std::cout << "Client connected" << std::endl;

    Message msg{};

    int bytes = recv(client_socket, &msg, sizeof(msg), 0);
    if (bytes <= 0){
        std::cout << "Connection error" << std::endl;
        close(client_socket);
        return 1;
    }

    if (msg.type == MSG_HELLO){
        std::cout << "Client: " << msg.payload << std::endl;

        Message reply{};
        prepareMessage(reply, MSG_WELCOME, "Welcome!");
        send(client_socket, &reply, sizeof(reply), 0);
    }

    while (true){
        bytes = recv(client_socket, &msg, sizeof(msg), 0);

        if (bytes == 0){
            std::cout << "Client disconnected" << std::endl;
            break;
        }
        if (bytes < 0){
            perror("recv");
            break;
        }

        if (msg.type == MSG_TEXT){
            std::cout << "Client: " << msg.payload << std::endl;
        }
        else if (msg.type == MSG_PING){
            Message pong{};
            prepareMessage(pong, MSG_PONG, "");
            send(client_socket, &pong, sizeof(pong), 0);
        }
        else if (msg.type == MSG_BYE){
            std::cout << "Client disconnected" << std::endl;
            break;
        }
    }

    close(client_socket);
    close(server_fd);
    return 0;
}

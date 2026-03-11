#pragma once

#include <cstdint>
#include <cstring>
#define MAX_PAYLOAD 1024

struct Message {
    uint32_t length;
    uint8_t type;
    char payload[MAX_PAYLOAD];
};

enum{
    MSG_HELLO   = 1,
    MSG_WELCOME = 2,
    MSG_TEXT    = 3,
    MSG_PING    = 4,
    MSG_PONG    = 5,
    MSG_BYE     = 6
};

inline void prepareMessage(Message &msg, uint8_t type, const std::string &text){
    msg.type = type;
    std::memset(msg.payload, 0, MAX_PAYLOAD);
    std::strncpy(msg.payload, text.c_str(), MAX_PAYLOAD - 1);
    msg.length = sizeof(msg.type) + std::strlen(msg.payload);
}


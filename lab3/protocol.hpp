#pragma once

#include <arpa/inet.h>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <string>
#include <sys/socket.h>
#include <unistd.h>

constexpr std::size_t MAX_PAYLOAD = 1024;

enum MessageType : uint8_t {
    MSG_HELLO = 1,
    MSG_WELCOME = 2,
    MSG_TEXT = 3,
    MSG_PING = 4,
    MSG_PONG = 5,
    MSG_BYE = 6,
};

struct Message {
    uint32_t length; 
    uint8_t type;
    char payload[MAX_PAYLOAD];
};

inline bool write_all(int fd, const void* buf, std::size_t n) {
    const uint8_t* p = static_cast<const uint8_t*>(buf);
    while (n > 0) {
        ssize_t w = ::send(fd, p, n, 0);
        if (w < 0) {
            if (errno == EINTR) continue;
            return false;
        }
        if (w == 0) return false;
        p += static_cast<std::size_t>(w);
        n -= static_cast<std::size_t>(w);
    }
    return true;
}

inline bool read_all(int fd, void* buf, std::size_t n) {
    uint8_t* p = static_cast<uint8_t*>(buf);
    while (n > 0) {
        ssize_t r = ::recv(fd, p, n, 0);
        if (r < 0) {
            if (errno == EINTR) continue;
            return false;
        }
        if (r == 0) return false;
        p += static_cast<std::size_t>(r);
        n -= static_cast<std::size_t>(r);
    }
    return true;
}

inline bool send_message(int fd, uint8_t type, const std::string& payload) {
    if (payload.size() > MAX_PAYLOAD) return false;
    uint32_t length = static_cast<uint32_t>(1 + payload.size());
    uint32_t net_len = htonl(length);
    if (!write_all(fd, &net_len, sizeof(net_len))) return false;
    if (!write_all(fd, &type, sizeof(type))) return false;
    if (!payload.empty() && !write_all(fd, payload.data(), payload.size())) return false;
    return true;
}

inline bool recv_message(int fd, Message& out_msg) {
    uint32_t net_len = 0;
    if (!read_all(fd, &net_len, sizeof(net_len))) return false;
    uint32_t length = ntohl(net_len);
    if (length < 1) return false;

    uint8_t type = 0;
    if (!read_all(fd, &type, sizeof(type))) return false;

    std::size_t payload_len = static_cast<std::size_t>(length - 1);
    if (payload_len > MAX_PAYLOAD) return false;

    std::memset(out_msg.payload, 0, MAX_PAYLOAD);
    if (payload_len > 0) {
        if (!read_all(fd, out_msg.payload, payload_len)) return false;
        if (payload_len < MAX_PAYLOAD) out_msg.payload[payload_len] = '\0';
        else out_msg.payload[MAX_PAYLOAD - 1] = '\0';
    }

    out_msg.length = length;
    out_msg.type = type;
    return true;
}


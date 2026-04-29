#pragma once

#include <algorithm>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <ctime>

#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
typedef SOCKET socket_t;
static inline int socket_init() {
  WSADATA data;
  return WSAStartup(MAKEWORD(2, 2), &data);
}
static inline void socket_cleanup() { WSACleanup(); }
static inline void socket_close(socket_t sock) { closesocket(sock); }
static inline int socket_is_interrupted() { return WSAGetLastError() == WSAEINTR; }
#else
#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>
typedef int socket_t;
static inline int socket_init() { return 0; }
static inline void socket_cleanup() {}
static inline void socket_close(socket_t sock) { close(sock); }
static inline int socket_is_interrupted() { return errno == EINTR; }
#endif

#define PORT 8080
#define MAX_NAME 32
#define MAX_PAYLOAD 256

enum MessageType : uint8_t {
  MSG_HELLO = 1,
  MSG_WELCOME = 2,
  MSG_TEXT = 3,
  MSG_PING = 4,
  MSG_PONG = 5,
  MSG_BYE = 6,
  MSG_AUTH = 7,
  MSG_PRIVATE = 8,
  MSG_ERROR = 9,
  MSG_SERVER_INFO = 10,
  MSG_LIST = 11,
  MSG_HISTORY = 12,
  MSG_HISTORY_DATA = 13,
  MSG_HELP = 14
};

#pragma pack(push, 1)
typedef struct {
  uint32_t length;
  uint8_t type;
  uint32_t msg_id;
  char sender[MAX_NAME];
  char receiver[MAX_NAME];
  int64_t timestamp;
  char payload[MAX_PAYLOAD];
} MessageEx;
#pragma pack(pop)

static inline const char *message_type_name(uint8_t type) {
  switch (type) {
  case MSG_HELLO:
    return "MSG_HELLO";
  case MSG_WELCOME:
    return "MSG_WELCOME";
  case MSG_TEXT:
    return "MSG_TEXT";
  case MSG_PING:
    return "MSG_PING";
  case MSG_PONG:
    return "MSG_PONG";
  case MSG_BYE:
    return "MSG_BYE";
  case MSG_AUTH:
    return "MSG_AUTH";
  case MSG_PRIVATE:
    return "MSG_PRIVATE";
  case MSG_ERROR:
    return "MSG_ERROR";
  case MSG_SERVER_INFO:
    return "MSG_SERVER_INFO";
  case MSG_LIST:
    return "MSG_LIST";
  case MSG_HISTORY:
    return "MSG_HISTORY";
  case MSG_HISTORY_DATA:
    return "MSG_HISTORY_DATA";
  case MSG_HELP:
    return "MSG_HELP";
  default:
    return "MSG_UNKNOWN";
  }
}

static inline uint64_t htonll(uint64_t value) {
  static const int num = 1;
  if (*(const char *)&num == 1) {
    uint32_t high = htonl((uint32_t)(value >> 32));
    uint32_t low = htonl((uint32_t)(value & 0xffffffffu));
    return ((uint64_t)low << 32) | high;
  }
  return value;
}

static inline uint64_t ntohll(uint64_t value) { return htonll(value); }

static inline void clear_message(MessageEx *msg) {
  memset(msg, 0, sizeof(*msg));
}

static inline int write_all(socket_t sock, const void *buf, size_t size) {
  const unsigned char *ptr = (const unsigned char *)buf;
  while (size > 0) {
#ifdef _WIN32
    int written = send(sock, (const char *)ptr, (int)size, 0);
#else
    ssize_t written = send(sock, ptr, size, 0);
#endif
    if (written < 0) {
      if (socket_is_interrupted())
        continue;
      return -1;
    }
    if (written == 0)
      return -1;
    ptr += (size_t)written;
    size -= (size_t)written;
  }
  return 0;
}

static inline int read_all(socket_t sock, void *buf, size_t size) {
  unsigned char *ptr = (unsigned char *)buf;
  while (size > 0) {
#ifdef _WIN32
    int readed = recv(sock, (char *)ptr, (int)size, 0);
#else
    ssize_t readed = recv(sock, ptr, size, 0);
#endif
    if (readed < 0) {
      if (socket_is_interrupted())
        continue;
      return -1;
    }
    if (readed == 0)
      return 0;
    ptr += (size_t)readed;
    size -= (size_t)readed;
  }
  return 1;
}

static inline int msg_send_ex(socket_t sock, const MessageEx *msg) {
  MessageEx wire;
  clear_message(&wire);
  wire.length = htonl(msg->length);
  wire.type = msg->type;
  wire.msg_id = htonl(msg->msg_id);
  memcpy(wire.sender, msg->sender, MAX_NAME);
  memcpy(wire.receiver, msg->receiver, MAX_NAME);
  wire.timestamp = (int64_t)htonll((uint64_t)msg->timestamp);
  if (msg->length > 0) {
    if (msg->length > MAX_PAYLOAD)
      return -1;
    memcpy(wire.payload, msg->payload, msg->length);
  }

  size_t total = sizeof(MessageEx) - MAX_PAYLOAD + msg->length;
  return write_all(sock, &wire, total) == 0 ? (int)total : -1;
}

static inline int msg_recv_ex(socket_t sock, MessageEx *msg) {
  clear_message(msg);

  size_t header_size = sizeof(MessageEx) - MAX_PAYLOAD;
  int header_rc = read_all(sock, msg, header_size);
  if (header_rc <= 0)
    return header_rc;

  uint32_t length = ntohl(msg->length);
  if (length > MAX_PAYLOAD)
    return -1;

  if (length > 0) {
    int payload_rc = read_all(sock, msg->payload, length);
    if (payload_rc <= 0)
      return payload_rc;
  }

  msg->length = length;
  msg->msg_id = ntohl(msg->msg_id);
  msg->timestamp = (int64_t)ntohll((uint64_t)msg->timestamp);
  if (msg->length < MAX_PAYLOAD)
    msg->payload[msg->length] = '\0';
  else
    msg->payload[MAX_PAYLOAD - 1] = '\0';
  return (int)(header_size + msg->length);
}

static inline void log_tcpip_send(const char *app, const char *type_name,
                                  size_t bytes, const char *dst_ip) {
  printf("[Application] %s -> %s\n", app, type_name);
  printf("[Transport] send() %llu bytes via TCP\n",
         (unsigned long long)bytes);
  printf("[Internet] destination ip = %s\n", dst_ip);
  printf("[Network Access] frame sent to network interface\n");
  fflush(stdout);
}

static inline void log_tcpip_recv(const char *app, const char *type_name,
                                  size_t bytes, const char *src_ip) {
  printf("[Network Access] frame received via network interface\n");
  printf("[Internet] src=%s dst=127.0.0.1 proto=TCP\n", src_ip);
  printf("[Transport] recv() %llu bytes via TCP\n",
         (unsigned long long)bytes);
  printf("[Application] %s -> %s\n", app, type_name);
  fflush(stdout);
}

static inline const char *format_time(time_t ts, char *buf, size_t buf_size) {
  struct tm tm_value;
  struct tm *tmp = localtime(&ts);
  if (tmp)
    tm_value = *tmp;
  strftime(buf, buf_size, "%Y-%m-%d %H:%M:%S", &tm_value);
  return buf;
}

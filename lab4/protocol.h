#pragma once

#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>

#define PORT 8080
#define MAX_PAYLOAD 1024
#define MAX_CLIENTS 32
#define MAX_NICK 32

typedef enum {
  MSG_HELLO = 1,       /* сервер → клиент: приветствие          */
  MSG_WELCOME = 2,     /* клиент → сервер: подтверждение связи  */
  MSG_TEXT = 3,        /* широковещательное сообщение           */
  MSG_PING = 4,        /* проверка связи                        */
  MSG_PONG = 5,        /* ответ на PING                         */
  MSG_BYE = 6,         /* разрыв соединения                     */
  MSG_AUTH = 7,        /* аутентификация (никнейм)              */
  MSG_PRIVATE = 8,     /* личное сообщение (payload: nick:msg)  */
  MSG_ERROR = 9,       /* сообщение об ошибке                   */
  MSG_SERVER_INFO = 10 /* системное сообщение от сервера        */
} MessageType;

#pragma pack(push, 1)
typedef struct {
  uint8_t type;
  uint16_t length;
  char payload[MAX_PAYLOAD];
} Message;
#pragma pack(pop)

#define MSG_HEADER_SIZE (sizeof(uint8_t) + sizeof(uint16_t))

static inline int msg_send(int sock, uint8_t type, const char *payload) {
  Message msg;
  memset(&msg, 0, sizeof(msg));
  msg.type = type;
  msg.length = (uint16_t)(payload ? (uint16_t)strlen(payload) : 0);
  if (payload && msg.length > 0)
    memcpy(msg.payload, payload, msg.length);

  size_t total = MSG_HEADER_SIZE + msg.length;
  return (int)send(sock, &msg, total, 0);
}

static inline int msg_recv(int sock, Message *msg) {
  memset(msg, 0, sizeof(*msg));

  int n = (int)recv(sock, msg, (int)MSG_HEADER_SIZE, MSG_WAITALL);
  if (n <= 0)
    return n;

  if (msg->length > 0) {
    if (msg->length > MAX_PAYLOAD)
      return -1;
    n = (int)recv(sock, msg->payload, msg->length, MSG_WAITALL);
    if (n <= 0)
      return n;
  }
  msg->payload[msg->length] = '\0';
  return (int)(MSG_HEADER_SIZE + msg->length);
}

static inline void osi_log(int layer, const char *layer_name,
                           const char *message) {
  printf("[Layer %d - %-14s] %s\n", layer, layer_name, message);
  fflush(stdout);
}

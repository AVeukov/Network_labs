#include <arpa/inet.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "protocol.h"

static volatile int running = 1;

static void *recv_thread(void *arg) {
  int sock = *(int *)arg;
  Message msg;

  while (running) {
    int n = msg_recv(sock, &msg);
    if (n <= 0) {
      if (running)
        printf("\n[!] Соединение с сервером разорвано\n");
      running = 0;
      break;
    }

    switch (msg.type) {
    case MSG_TEXT:
      printf("%s\n", msg.payload);
      break;

    case MSG_PRIVATE:
      printf("%s\n", msg.payload);
      break;

    case MSG_SERVER_INFO:
      printf("[SERVER]: %s\n", msg.payload);
      break;

    case MSG_PONG:
      printf("[SERVER]: pong (связь в норме)\n");
      break;

    case MSG_ERROR:
      printf("[ERROR]: %s\n", msg.payload);
      break;

    case MSG_BYE:
      printf("[SERVER]: соединение закрыто сервером\n");
      running = 0;
      break;

    default:
      break;
    }
    fflush(stdout);
  }
  return NULL;
}

static void trim_newline(char *s) {
  size_t len = strlen(s);
  while (len > 0 && (s[len - 1] == '\n' || s[len - 1] == '\r'))
    s[--len] = '\0';
}

int main(int argc, char *argv[]) {
  const char *host = (argc >= 2) ? argv[1] : "127.0.0.1";
  int port = (argc >= 3) ? atoi(argv[2]) : PORT;

  int sock = socket(AF_INET, SOCK_STREAM, 0);
  if (sock < 0) {
    perror("socket");
    return 1;
  }

  struct sockaddr_in server_addr;
  memset(&server_addr, 0, sizeof(server_addr));
  server_addr.sin_family = AF_INET;
  server_addr.sin_port = htons((uint16_t)port);

  if (inet_pton(AF_INET, host, &server_addr.sin_addr) <= 0) {
    fprintf(stderr, "Неверный адрес: %s\n", host);
    return 1;
  }
  if (connect(sock, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
    perror("connect");
    return 1;
  }
  printf("Подключено к серверу %s:%d\n", host, port);

  Message msg;
  if (msg_recv(sock, &msg) <= 0 || msg.type != MSG_HELLO) {
    fprintf(stderr, "Ожидался MSG_HELLO от сервера\n");
    close(sock);
    return 1;
  }
  printf("[SERVER]: %s\n", msg.payload);
  msg_send(sock, MSG_WELCOME, "ok");

  char nickname[MAX_NICK];
  printf("Введите никнейм: ");
  fflush(stdout);
  if (!fgets(nickname, sizeof(nickname), stdin)) {
    close(sock);
    return 1;
  }
  trim_newline(nickname);

  if (strlen(nickname) == 0) {
    fprintf(stderr, "Никнейм не может быть пустым\n");
    close(sock);
    return 1;
  }

  msg_send(sock, MSG_AUTH, nickname);

  if (msg_recv(sock, &msg) <= 0) {
    fprintf(stderr, "Сервер закрыл соединение\n");
    close(sock);
    return 1;
  }
  if (msg.type == MSG_ERROR) {
    printf("[ERROR]: %s\n", msg.payload);
    close(sock);
    return 1;
  }
  if (msg.type == MSG_SERVER_INFO)
    printf("[SERVER]: %s\n", msg.payload);

  printf("\nДобро пожаловать, %s!\n", nickname);
  printf("Команды:\n");
  printf("  /w <nick> <сообщение>  — личное сообщение\n");
  printf("  /ping                  — проверить связь\n");
  printf("  /quit                  — выйти\n\n");

  pthread_t tid;
  pthread_create(&tid, NULL, recv_thread, &sock);
  pthread_detach(tid);

  char input[MAX_PAYLOAD + 64];
  while (running) {
    printf("> ");
    fflush(stdout);

    if (!fgets(input, sizeof(input), stdin))
      break;
    if (!running)
      break;

    trim_newline(input);
    if (strlen(input) == 0)
      continue;

    if (strcmp(input, "/quit") == 0) {
      msg_send(sock, MSG_BYE, "bye");
      running = 0;
      break;
    }

    if (strcmp(input, "/ping") == 0) {
      msg_send(sock, MSG_PING, "ping");
      continue;
    }

    if (strncmp(input, "/w ", 3) == 0) {
      char *rest = input + 3;
      char *space = strchr(rest, ' ');
      if (!space) {
        printf("[!] Использование: /w <nick> <сообщение>\n");
        continue;
      }
      *space = '\0';
      char *target = rest;
      char *text = space + 1;

      if (strlen(text) == 0) {
        printf("[!] Пустое сообщение\n");
        continue;
      }

      char payload[MAX_PAYLOAD];
      snprintf(payload, sizeof(payload), "%s:%s", target, text);
      msg_send(sock, MSG_PRIVATE, payload);
      continue;
    }

    msg_send(sock, MSG_TEXT, input);
  }

  close(sock);
  printf("Отключено от сервера\n");
  return 0;
}

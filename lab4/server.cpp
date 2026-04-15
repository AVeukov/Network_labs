#include <arpa/inet.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "protocol.h"

typedef struct {
  int sock;
  char nickname[MAX_NICK];
  int authenticated;
} Client;

static Client clients[MAX_CLIENTS];
static int client_count = 0;
static pthread_mutex_t clients_mutex = PTHREAD_MUTEX_INITIALIZER;

static int client_add(int sock) {
  pthread_mutex_lock(&clients_mutex);
  if (client_count >= MAX_CLIENTS) {
    pthread_mutex_unlock(&clients_mutex);
    return -1;
  }
  clients[client_count].sock = sock;
  clients[client_count].nickname[0] = '\0';
  clients[client_count].authenticated = 0;
  client_count++;
  pthread_mutex_unlock(&clients_mutex);
  return 0;
}

static void client_remove(int sock) {
  pthread_mutex_lock(&clients_mutex);
  for (int i = 0; i < client_count; i++) {
    if (clients[i].sock == sock) {
      clients[i] = clients[client_count - 1];
      client_count--;
      break;
    }
  }
  pthread_mutex_unlock(&clients_mutex);
}

static int client_find_sock(const char *nick) {
  pthread_mutex_lock(&clients_mutex);
  int result = -1;
  for (int i = 0; i < client_count; i++) {
    if (clients[i].authenticated && strcmp(clients[i].nickname, nick) == 0) {
      result = clients[i].sock;
      break;
    }
  }
  pthread_mutex_unlock(&clients_mutex);
  return result;
}

static int nick_is_unique(const char *nick) {
  for (int i = 0; i < client_count; i++) {
    if (clients[i].authenticated && strcmp(clients[i].nickname, nick) == 0)
      return 0;
  }
  return 1;
}

static void broadcast(int sender_sock, uint8_t type, const char *text) {
  pthread_mutex_lock(&clients_mutex);
  for (int i = 0; i < client_count; i++) {
    if (clients[i].authenticated && clients[i].sock != sender_sock) {
      msg_send(clients[i].sock, type, text);
    }
  }
  pthread_mutex_unlock(&clients_mutex);
}

static void broadcast_server_info(const char *text) {
  pthread_mutex_lock(&clients_mutex);
  for (int i = 0; i < client_count; i++) {
    if (clients[i].authenticated)
      msg_send(clients[i].sock, MSG_SERVER_INFO, text);
  }
  pthread_mutex_unlock(&clients_mutex);
}

static void *handle_client(void *arg) {
  int sock = *(int *)arg;
  free(arg);

  Message msg;
  char buf[MAX_PAYLOAD + 64];
  char sender_nick[MAX_NICK] = "";

  osi_log(7, "Application", "prepare MSG_HELLO");
  osi_log(6, "Presentation", "serialize Message");
  osi_log(4, "Transport", "send()");
  msg_send(sock, MSG_HELLO, "Welcome to the chat server");

  osi_log(4, "Transport", "recv()");
  if (msg_recv(sock, &msg) <= 0 || msg.type != MSG_WELCOME) {
    fprintf(stderr, "[SERVER] Handshake failed (sock %d)\n", sock);
    close(sock);
    client_remove(sock);
    return NULL;
  }
  osi_log(6, "Presentation", "deserialize Message");
  osi_log(7, "Application", "handshake complete");

  osi_log(4, "Transport", "recv()");
  if (msg_recv(sock, &msg) <= 0) {
    close(sock);
    client_remove(sock);
    return NULL;
  }
  osi_log(6, "Presentation", "deserialize Message");

  if (msg.type != MSG_AUTH || strlen(msg.payload) == 0) {
    osi_log(5, "Session", "authentication FAILED (missing nick)");
    osi_log(6, "Presentation", "serialize Message");
    osi_log(4, "Transport", "send()");
    msg_send(sock, MSG_ERROR,
             "Authentication required: send MSG_AUTH with your nickname");
    close(sock);
    client_remove(sock);
    return NULL;
  }

  strncpy(sender_nick, msg.payload, MAX_NICK - 1);
  sender_nick[MAX_NICK - 1] = '\0';

  pthread_mutex_lock(&clients_mutex);
  int unique = nick_is_unique(sender_nick);
  if (unique) {

    for (int i = 0; i < client_count; i++) {
      if (clients[i].sock == sock) {
        strncpy(clients[i].nickname, sender_nick, MAX_NICK - 1);
        clients[i].authenticated = 1;
        break;
      }
    }
  }
  pthread_mutex_unlock(&clients_mutex);

  if (!unique) {
    osi_log(5, "Session", "authentication FAILED (nick taken)");
    osi_log(6, "Presentation", "serialize Message");
    osi_log(4, "Transport", "send()");
    msg_send(sock, MSG_ERROR, "Nickname already taken");
    close(sock);
    client_remove(sock);
    return NULL;
  }

  osi_log(5, "Session", "client authenticated");
  snprintf(buf, sizeof(buf), "User [%s] connected", sender_nick);
  printf("%s\n", buf);

  osi_log(7, "Application", "prepare MSG_SERVER_INFO (user joined)");
  osi_log(6, "Presentation", "serialize Message");
  osi_log(4, "Transport", "send()");
  msg_send(sock, MSG_SERVER_INFO, "Authentication successful");

  broadcast(sock, MSG_SERVER_INFO, buf);

  while (1) {
    osi_log(4, "Transport", "recv()");
    int n = msg_recv(sock, &msg);
    if (n <= 0) {
      snprintf(buf, sizeof(buf), "User [%s] disconnected", sender_nick);
      printf("%s\n", buf);
      broadcast_server_info(buf);
      break;
    }
    osi_log(6, "Presentation", "deserialize Message");

    switch (msg.type) {

    case MSG_TEXT:
      osi_log(5, "Session", "client authenticated ✓");
      osi_log(7, "Application", "handle MSG_TEXT → broadcast");
      snprintf(buf, sizeof(buf), "[%s]: %s", sender_nick, msg.payload);
      printf("%s\n", buf);
      broadcast(sock, MSG_TEXT, buf);
      break;

    case MSG_PRIVATE: {
      osi_log(5, "Session", "client authenticated ✓");
      osi_log(7, "Application", "handle MSG_PRIVATE → route to target");

      char target_nick[MAX_NICK] = "";
      char private_text[MAX_PAYLOAD] = "";

      char *colon = strchr(msg.payload, ':');
      if (!colon) {
        msg_send(sock, MSG_ERROR, "Invalid format. Use: /w <nick> <message>");
        break;
      }
      size_t nick_len = (size_t)(colon - msg.payload);
      if (nick_len == 0 || nick_len >= MAX_NICK) {
        msg_send(sock, MSG_ERROR, "Invalid nickname in private message");
        break;
      }
      strncpy(target_nick, msg.payload, nick_len);
      target_nick[nick_len] = '\0';
      strncpy(private_text, colon + 1, MAX_PAYLOAD - 1);

      int target_sock = client_find_sock(target_nick);
      if (target_sock == -1) {
        snprintf(buf, sizeof(buf), "User '%s' not found", target_nick);
        msg_send(sock, MSG_ERROR, buf);
        break;
      }

      snprintf(buf, sizeof(buf), "[PRIVATE][%s]: %s", sender_nick,
               private_text);
      printf("[PRIVATE] %s → %s: %s\n", sender_nick, target_nick, private_text);

      osi_log(7, "Application", "prepare MSG_PRIVATE response");
      osi_log(6, "Presentation", "serialize Message");
      osi_log(4, "Transport", "send()");
      msg_send(target_sock, MSG_PRIVATE, buf);
      snprintf(buf, sizeof(buf), "[PRIVATE → %s]: %s", target_nick,
               private_text);
      msg_send(sock, MSG_PRIVATE, buf);
      break;
    }

    case MSG_PING:
      osi_log(7, "Application", "handle MSG_PING → send MSG_PONG");
      osi_log(6, "Presentation", "serialize Message");
      osi_log(4, "Transport", "send()");
      msg_send(sock, MSG_PONG, "pong");
      break;

    case MSG_BYE:
      osi_log(7, "Application", "handle MSG_BYE → disconnect client");
      snprintf(buf, sizeof(buf), "User [%s] disconnected", sender_nick);
      printf("%s\n", buf);
      broadcast_server_info(buf);
      goto cleanup;

    default:
      osi_log(7, "Application", "unknown message type — ignored");
      break;
    }
  }

cleanup:
  close(sock);
  client_remove(sock);
  return NULL;
}

int main(void) {
  int server_sock = socket(AF_INET, SOCK_STREAM, 0);
  if (server_sock < 0) {
    perror("socket");
    return 1;
  }

  int opt = 1;
  setsockopt(server_sock, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

  struct sockaddr_in addr;
  memset(&addr, 0, sizeof(addr));
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = INADDR_ANY;
  addr.sin_port = htons(PORT);

  if (bind(server_sock, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
    perror("bind");
    return 1;
  }
  if (listen(server_sock, 10) < 0) {
    perror("listen");
    return 1;
  }

  printf("=== Chat server started on port %d ===\n\n", PORT);

  while (1) {
    struct sockaddr_in client_addr;
    socklen_t addr_len = sizeof(client_addr);

    int client_sock =
        accept(server_sock, (struct sockaddr *)&client_addr, &addr_len);
    if (client_sock < 0) {
      perror("accept");
      continue;
    }

    printf("Client connected from %s:%d (sock=%d)\n",
           inet_ntoa(client_addr.sin_addr), ntohs(client_addr.sin_port),
           client_sock);

    if (client_add(client_sock) < 0) {
      fprintf(stderr, "Max clients reached, rejecting\n");
      close(client_sock);
      continue;
    }

    pthread_t tid;
    int *sock_ptr = (int *)malloc(sizeof(int));
    *sock_ptr = client_sock;
    pthread_create(&tid, NULL, handle_client, sock_ptr);
    pthread_detach(tid);
  }

  close(server_sock);
  return 0;
}

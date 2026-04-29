#include "protocol.h"

#include <atomic>
#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <string>
#include <thread>

struct SharedState {
  std::atomic<socket_t> sock{(socket_t)-1};
  std::atomic<bool> running{true};
  std::string nick;
};

static void trim_newline(std::string &text) {
  while (!text.empty() &&
         (text.back() == '\n' || text.back() == '\r' || text.back() == ' '))
    text.pop_back();
}

static bool connect_to_server(const std::string &host, uint16_t port,
                              socket_t &sock) {
  sock = socket(AF_INET, SOCK_STREAM, 0);
  if (sock == (socket_t)-1)
    return false;

  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(port);
#if defined(_WIN32)
  addr.sin_addr.s_addr = inet_addr(host.c_str());
  if (addr.sin_addr.s_addr == INADDR_NONE) {
    socket_close(sock);
    sock = (socket_t)-1;
    return false;
  }
#else
  if (inet_pton(AF_INET, host.c_str(), &addr.sin_addr) != 1) {
    socket_close(sock);
    sock = (socket_t)-1;
    return false;
  }
#endif

  if (connect(sock, (sockaddr *)&addr, sizeof(addr)) < 0) {
    socket_close(sock);
    sock = (socket_t)-1;
    return false;
  }
  return true;
}

static void send_message(socket_t sock, MessageType type,
                         const std::string &sender,
                         const std::string &receiver,
                         const std::string &payload, uint32_t msg_id = 0) {
  MessageEx out;
  clear_message(&out);
  out.type = type;
  out.msg_id = msg_id;
  out.timestamp = (int64_t)time(nullptr);
  std::strncpy(out.sender, sender.c_str(), MAX_NAME - 1);
  std::strncpy(out.receiver, receiver.c_str(), MAX_NAME - 1);
  out.length = (uint32_t)std::min<size_t>(payload.size(), MAX_PAYLOAD - 1);
  std::memcpy(out.payload, payload.data(), out.length);
  out.payload[out.length] = '\0';
  log_tcpip_send("serialize MessageEx", message_type_name(type),
                 sizeof(MessageEx) - MAX_PAYLOAD + out.length, "127.0.0.1");
  (void)msg_send_ex(sock, &out);
}

static void recv_loop(SharedState *state) {
  while (state->running.load()) {
    socket_t sock = state->sock.load();
    if (sock == (socket_t)-1) {
      std::this_thread::sleep_for(std::chrono::milliseconds(50));
      continue;
    }

    MessageEx msg;
    clear_message(&msg);
    int n = msg_recv_ex(sock, &msg);
    if (n <= 0) {
      if (state->running.load())
        std::printf("\n[SERVER]: connection closed\n");
      state->sock.store((socket_t)-1);
      break;
    }

    log_tcpip_recv("deserialize MessageEx", message_type_name(msg.type),
                   (size_t)n, "127.0.0.1");

    switch (msg.type) {
    case MSG_TEXT:
    case MSG_PRIVATE:
    case MSG_HISTORY_DATA:
      std::printf("%s\n", msg.payload);
      break;
    case MSG_SERVER_INFO:
      std::printf("[SERVER]: %s\n", msg.payload);
      break;
    case MSG_PONG:
      std::printf("[SERVER]: %s\n", msg.payload);
      break;
    case MSG_ERROR:
      std::printf("[ERROR]: %s\n", msg.payload);
      break;
    case MSG_BYE:
      std::printf("[SERVER]: %s\n", msg.payload);
      state->running.store(false);
      break;
    default:
      break;
    }
    std::fflush(stdout);
  }
}

int main(int argc, char **argv) {
  std::string host = "127.0.0.1";
  uint16_t port = PORT;
  std::string nick = "user";

  if (argc >= 2)
    host = argv[1];
  if (argc >= 3)
    port = (uint16_t)std::atoi(argv[2]);
  if (argc >= 4)
    nick = argv[3];

  if (socket_init() != 0) {
    std::fprintf(stderr, "socket_init failed\n");
    return 1;
  }

  socket_t sock = (socket_t)-1;
  if (!connect_to_server(host, port, sock)) {
    std::perror("connect");
    socket_cleanup();
    return 1;
  }

  SharedState state;
  state.sock.store(sock);
  state.nick = nick;

  MessageEx msg;
  if (msg_recv_ex(sock, &msg) <= 0 || msg.type != MSG_HELLO) {
    std::fprintf(stderr, "Expected MSG_HELLO from server\n");
    socket_close(sock);
    socket_cleanup();
    return 1;
  }
  log_tcpip_recv("deserialize MessageEx", message_type_name(msg.type),
                 sizeof(MessageEx) - MAX_PAYLOAD + msg.length, "127.0.0.1");
  std::printf("[SERVER]: %s\n", msg.payload);

  MessageEx welcome;
  clear_message(&welcome);
  welcome.type = MSG_WELCOME;
  welcome.length = 2;
  std::memcpy(welcome.payload, "ok", 2);
  log_tcpip_send("serialize MessageEx", message_type_name(MSG_WELCOME),
                 sizeof(MessageEx) - MAX_PAYLOAD + welcome.length,
                 "127.0.0.1");
  (void)msg_send_ex(sock, &welcome);

  send_message(sock, MSG_AUTH, nick, "", nick);

  if (msg_recv_ex(sock, &msg) <= 0) {
    std::fprintf(stderr, "Server closed connection during auth\n");
    socket_close(sock);
    socket_cleanup();
    return 1;
  }
  log_tcpip_recv("deserialize MessageEx", message_type_name(msg.type),
                 sizeof(MessageEx) - MAX_PAYLOAD + msg.length, "127.0.0.1");
  if (msg.type == MSG_ERROR) {
    std::printf("[ERROR]: %s\n", msg.payload);
    socket_close(sock);
    socket_cleanup();
    return 1;
  }
  if (msg.type == MSG_SERVER_INFO)
    std::printf("[SERVER]: %s\n", msg.payload);

  std::printf("\nWelcome, %s\n", nick.c_str());
  std::printf("Commands:\n");
  std::printf("  /help\n");
  std::printf("  /list\n");
  std::printf("  /history\n");
  std::printf("  /history N\n");
  std::printf("  /quit\n");
  std::printf("  /w <nick> <message>\n");
  std::printf("  /ping\n\n");

  std::thread recv_thread(recv_loop, &state);
  recv_thread.detach();

  while (state.running.load()) {
    std::string line;
    std::cout << "> ";
    std::cout.flush();
    if (!std::getline(std::cin, line))
      break;
    trim_newline(line);
    if (line.empty())
      continue;

    socket_t current_sock = state.sock.load();
    if (current_sock == (socket_t)-1)
      break;

    if (line == "/quit") {
      send_message(current_sock, MSG_BYE, nick, "", "bye");
      state.running.store(false);
      break;
    }

    if (line == "/ping") {
      send_message(current_sock, MSG_PING, nick, "", "ping");
      continue;
    }

    if (line == "/help") {
      send_message(current_sock, MSG_HELP, nick, "", "help");
      std::printf("Available commands:\n");
      std::printf("/help\n/list\n/history\n/history N\n/quit\n/w <nick> <message>\n/ping\n");
      std::printf("Tip: packets never sleep\n");
      continue;
    }

    if (line == "/list") {
      send_message(current_sock, MSG_LIST, nick, "", "list");
      continue;
    }

    if (line.rfind("/history", 0) == 0) {
      std::string count;
      if (line.size() > 8) {
        count = line.substr(8);
        trim_newline(count);
        while (!count.empty() && count.front() == ' ')
          count.erase(count.begin());
      }
      send_message(current_sock, MSG_HISTORY, nick, "", count);
      continue;
    }

    if (line.rfind("/w ", 0) == 0) {
      std::string rest = line.substr(3);
      size_t pos = rest.find(' ');
      if (pos == std::string::npos) {
        std::printf("[!] Usage: /w <nick> <message>\n");
        continue;
      }
      std::string target = rest.substr(0, pos);
      std::string text = rest.substr(pos + 1);
      if (target.empty() || text.empty()) {
        std::printf("[!] Empty private message\n");
        continue;
      }
      send_message(current_sock, MSG_PRIVATE, nick, target, text);
      continue;
    }

    send_message(current_sock, MSG_TEXT, nick, "", line);
  }

  socket_close(sock);
  socket_cleanup();
  std::printf("Disconnected from server\n");
  return 0;
}

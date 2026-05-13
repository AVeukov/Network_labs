#include "protocol.h"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

using Clock = std::chrono::steady_clock;

struct PendingState {
  MessageEx msg{};
  Clock::time_point sent_at{};
  bool acked = false;
  bool ponged = false;
  double rtt_ms = -1.0;
  MessageType type = MSG_TEXT;
};

struct PingSample {
  bool success = false;
  double rtt_ms = 0.0;
};

struct SharedState {
  std::atomic<socket_t> sock{(socket_t)-1};
  std::atomic<bool> running{true};
  std::string nick;
};

static std::mutex g_pending_mtx;
static std::condition_variable g_pending_cv;
static std::unordered_map<uint32_t, PendingState> g_pending;
static std::atomic<uint32_t> g_next_msg_id{1};
static std::vector<PingSample> g_ping_samples;
static std::atomic<bool> g_auth_phase{true};
static std::atomic<bool> g_auth_failed{false};

static uint32_t next_msg_id() { return g_next_msg_id.fetch_add(1); }

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

static MessageEx make_message(MessageType type, uint32_t msg_id,
                              const std::string &sender,
                              const std::string &receiver,
                              const std::string &payload) {
  MessageEx out;
  clear_message(&out);
  out.type = type;
  out.msg_id = msg_id;
  out.timestamp = (int64_t)time(nullptr);
  std::strncpy(out.sender, sender.c_str(), MAX_NAME - 1);
  std::strncpy(out.receiver, receiver.c_str(), MAX_NAME - 1);
  out.length = (uint32_t)std::min<size_t>(payload.size(), MAX_PAYLOAD - 1);
  if (out.length > 0)
    std::memcpy(out.payload, payload.data(), out.length);
  out.payload[out.length] = '\0';
  return out;
}

static void send_wire(socket_t sock, const MessageEx &msg, bool retry,
                      int attempt = 1, int max_attempts = 3) {
  if (retry) {
    if (attempt == 1) {
      std::printf("[Transport][RETRY] send %s (id=%u)\n",
                  message_type_name(msg.type), msg.msg_id);
    } else {
      std::printf("[Transport][RETRY] resend %d/%d (id=%u)\n", attempt - 1,
                  max_attempts - 1, msg.msg_id);
    }
  }
  log_tcpip_send("serialize MessageEx", message_type_name(msg.type),
                 sizeof(MessageEx) - MAX_PAYLOAD + msg.length, "127.0.0.1");
  (void)msg_send_ex(sock, &msg);
}

static bool wait_for_ack(uint32_t msg_id, int timeout_ms) {
  std::unique_lock<std::mutex> lock(g_pending_mtx);
  auto deadline = Clock::now() + std::chrono::milliseconds(timeout_ms);
  auto pred = [&] {
    auto it = g_pending.find(msg_id);
    return it != g_pending.end() && it->second.acked;
  };
  if (!g_pending_cv.wait_until(lock, deadline, pred)) {
    std::printf("[Transport][RETRY] wait ACK timeout\n");
    return false;
  }
  std::printf("[Transport][RETRY] ACK received (id=%u)\n", msg_id);
  return true;
}

static bool wait_for_pong(uint32_t msg_id, double &rtt_ms, int timeout_ms) {
  std::unique_lock<std::mutex> lock(g_pending_mtx);
  auto deadline = Clock::now() + std::chrono::milliseconds(timeout_ms);
  auto pred = [&] {
    auto it = g_pending.find(msg_id);
    return it != g_pending.end() && it->second.ponged;
  };
  if (!g_pending_cv.wait_until(lock, deadline, pred))
    return false;
  auto it = g_pending.find(msg_id);
  if (it == g_pending.end())
    return false;
  rtt_ms = it->second.rtt_ms;
  return true;
}

static bool send_reliable(socket_t sock, MessageType type,
                          const std::string &sender,
                          const std::string &receiver,
                          const std::string &payload, uint32_t &msg_id) {
  msg_id = next_msg_id();
  MessageEx msg = make_message(type, msg_id, sender, receiver, payload);

  {
    std::lock_guard<std::mutex> lock(g_pending_mtx);
    g_pending[msg_id] =
        PendingState{msg, Clock::now(), false, false, -1.0, type};
  }

  for (int attempt = 1; attempt <= 3; ++attempt) {
    {
      std::lock_guard<std::mutex> lock(g_pending_mtx);
      auto it = g_pending.find(msg_id);
      if (it != g_pending.end())
        it->second.sent_at = Clock::now();
    }

    send_wire(sock, msg, true, attempt, 3);
    if (wait_for_ack(msg_id, 2000)) {
      if (type != MSG_PING) {
        std::lock_guard<std::mutex> lock(g_pending_mtx);
        g_pending.erase(msg_id);
      }
      return true;
    }
  }

  {
    std::lock_guard<std::mutex> lock(g_pending_mtx);
    g_pending.erase(msg_id);
  }
  return false;
}

static void record_ping_sample(bool success, double rtt_ms) {
  std::lock_guard<std::mutex> lock(g_pending_mtx);
  g_ping_samples.push_back(PingSample{success, rtt_ms});
}

static void write_netdiag_file(const std::string &nick) {
  std::vector<PingSample> samples;
  {
    std::lock_guard<std::mutex> lock(g_pending_mtx);
    samples = g_ping_samples;
  }

  double sum_rtt = 0.0;
  int success_count = 0;
  double jitter_sum = 0.0;
  double prev_rtt = 0.0;
  bool prev_ok = false;
  int total = (int)samples.size();
  int failures = 0;

  for (const auto &s : samples) {
    if (!s.success) {
      ++failures;
      continue;
    }
    sum_rtt += s.rtt_ms;
    ++success_count;
    if (prev_ok)
      jitter_sum += std::abs(s.rtt_ms - prev_rtt);
    prev_rtt = s.rtt_ms;
    prev_ok = true;
  }

  double rtt_avg = success_count > 0 ? sum_rtt / success_count : 0.0;
  double jitter_avg = success_count > 1 ? jitter_sum / (success_count - 1) : 0.0;
  double loss = total > 0 ? (double)failures * 100.0 / total : 0.0;

  std::ostringstream path;
  path << "net_diag_" << nick << ".json";
  std::ofstream out(path.str(), std::ios::trunc);
  out << "{\n";
  out << "  \"nickname\": \"" << nick << "\",\n";
  out << "  \"rtt_avg_ms\": " << std::fixed << std::setprecision(3) << rtt_avg
      << ",\n";
  out << "  \"jitter_avg_ms\": " << jitter_avg << ",\n";
  out << "  \"loss_percent\": " << loss << ",\n";
  out << "  \"samples\": [\n";
  for (size_t i = 0; i < samples.size(); ++i) {
    out << "    {\"ok\": " << (samples[i].success ? "true" : "false")
        << ", \"rtt_ms\": " << samples[i].rtt_ms << "}";
    if (i + 1 < samples.size())
      out << ",";
    out << "\n";
  }
  out << "  ]\n";
  out << "}\n";
}

static void print_netdiag(const std::string &nick) {
  std::vector<PingSample> samples;
  {
    std::lock_guard<std::mutex> lock(g_pending_mtx);
    samples = g_ping_samples;
  }

  double sum_rtt = 0.0;
  int success_count = 0;
  double jitter_sum = 0.0;
  double prev_rtt = 0.0;
  bool prev_ok = false;
  int total = (int)samples.size();
  int failures = 0;

  for (const auto &s : samples) {
    if (!s.success) {
      ++failures;
      continue;
    }
    sum_rtt += s.rtt_ms;
    ++success_count;
    if (prev_ok)
      jitter_sum += std::abs(s.rtt_ms - prev_rtt);
    prev_rtt = s.rtt_ms;
    prev_ok = true;
  }

  double rtt_avg = success_count > 0 ? sum_rtt / success_count : 0.0;
  double jitter_avg = success_count > 1 ? jitter_sum / (success_count - 1) : 0.0;
  double loss = total > 0 ? (double)failures * 100.0 / total : 0.0;

  std::printf("RTT avg : %.1f ms\n", rtt_avg);
  std::printf("Jitter  : %.1f ms\n", jitter_avg);
  std::printf("Loss    : %.1f%%\n", loss);
  write_netdiag_file(nick);
}

static void handle_incoming(const MessageEx &msg, const SharedState &state) {
  switch (msg.type) {
  case MSG_ACK: {
    std::lock_guard<std::mutex> lock(g_pending_mtx);
    auto it = g_pending.find(msg.msg_id);
    if (it != g_pending.end()) {
      it->second.acked = true;
      g_pending_cv.notify_all();
    }
    break;
  }
  case MSG_PONG: {
    auto now = Clock::now();
    std::lock_guard<std::mutex> lock(g_pending_mtx);
    auto it = g_pending.find(msg.msg_id);
    if (it != g_pending.end()) {
      it->second.ponged = true;
      it->second.rtt_ms =
          std::chrono::duration<double, std::milli>(now - it->second.sent_at)
              .count();
      g_pending_cv.notify_all();
    }
    break;
  }
  case MSG_TEXT:
  case MSG_PRIVATE:
  case MSG_HISTORY_DATA:
    std::printf("%s\n", msg.payload);
    break;
  case MSG_SERVER_INFO:
    std::printf("[SERVER]: %s\n", msg.payload);
    break;
  case MSG_ERROR:
    std::printf("[ERROR]: %s\n", msg.payload);
    if (g_auth_phase.load()) {
      g_auth_failed.store(true);
    }
    break;
  case MSG_BYE:
    std::printf("[SERVER]: %s\n", msg.payload);
    state.running.store(false);
    break;
  default:
    break;
  }
  std::fflush(stdout);
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
      state->running.store(false);
      g_pending_cv.notify_all();
      break;
    }

    log_tcpip_recv("deserialize MessageEx", message_type_name(msg.type),
                   (size_t)n, "127.0.0.1");
    handle_incoming(msg, *state);
  }
}

static void print_help() {
  std::printf("Available commands:\n");
  std::printf("/help\n/list\n/history\n/history N\n/quit\n/w <nick> <message>\n");
  std::printf("/ping\n/netdiag\n");
  std::printf("Tip: packets never sleep\n");
}

static void run_ping(socket_t sock, const std::string &nick, int count) {
  if (count <= 0)
    count = 10;

  for (int i = 1; i <= count; ++i) {
    uint32_t msg_id = 0;
    if (!send_reliable(sock, MSG_PING, nick, "", "ping", msg_id)) {
      std::printf("PING %d -> timeout\n", i);
      record_ping_sample(false, 0.0);
      continue;
    }

    double rtt = 0.0;
    if (wait_for_pong(msg_id, rtt, 2000)) {
      std::printf("PING %d -> RTT=%.1fms\n", i, rtt);
      record_ping_sample(true, rtt);
      {
        std::lock_guard<std::mutex> lock(g_pending_mtx);
        g_pending.erase(msg_id);
      }
      continue;
    }

    std::printf("PING %d -> timeout\n", i);
    record_ping_sample(false, 0.0);
    {
      std::lock_guard<std::mutex> lock(g_pending_mtx);
      g_pending.erase(msg_id);
    }
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

  MessageEx welcome = make_message(MSG_WELCOME, 0, nick, "", "ok");
  log_tcpip_send("serialize MessageEx", message_type_name(MSG_WELCOME),
                 sizeof(MessageEx) - MAX_PAYLOAD + welcome.length,
                 "127.0.0.1");
  (void)msg_send_ex(sock, &welcome);

  std::thread recv_thread(recv_loop, &state);
  recv_thread.detach();

  uint32_t auth_id = 0;
  if (!send_reliable(sock, MSG_AUTH, nick, "", nick, auth_id)) {
    std::fprintf(stderr, "Authentication failed: ACK timeout\n");
    socket_close(sock);
    socket_cleanup();
    return 1;
  }

  if (g_auth_failed.load()) {
    socket_close(sock);
    socket_cleanup();
    return 1;
  }
  g_auth_phase.store(false);

  std::printf("\nWelcome, %s\n", nick.c_str());
  print_help();

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
      uint32_t id = 0;
      send_reliable(current_sock, MSG_BYE, nick, "", "bye", id);
      state.running.store(false);
      break;
    }

    if (line == "/help") {
      uint32_t id = 0;
      send_reliable(current_sock, MSG_HELP, nick, "", "help", id);
      print_help();
      continue;
    }

    if (line == "/list") {
      uint32_t id = 0;
      send_reliable(current_sock, MSG_LIST, nick, "", "list", id);
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
      uint32_t id = 0;
      send_reliable(current_sock, MSG_HISTORY, nick, "", count, id);
      continue;
    }

    if (line == "/ping" || line.rfind("/ping ", 0) == 0) {
      int count = 10;
      if (line.size() > 6) {
        count = std::atoi(line.substr(6).c_str());
        if (count <= 0)
          count = 10;
      }
      run_ping(current_sock, nick, count);
      continue;
    }

    if (line == "/netdiag") {
      print_netdiag(nick);
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
      uint32_t id = 0;
      send_reliable(current_sock, MSG_PRIVATE, nick, target, text, id);
      continue;
    }

    uint32_t id = 0;
    send_reliable(current_sock, MSG_TEXT, nick, "", line, id);
  }

  socket_close(sock);
  socket_cleanup();
  std::printf("Disconnected from server\n");
  return 0;
}

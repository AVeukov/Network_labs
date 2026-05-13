#include "protocol.h"

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <deque>
#include <fstream>
#include <mutex>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_set>
#include <unordered_map>
#include <vector>

struct ClientSession {
  socket_t sock = (socket_t)-1;
  std::string nick;
  bool authenticated = false;
  sockaddr_in addr{};
};

struct HistoryRecord {
  uint32_t msg_id = 0;
  int64_t timestamp = 0;
  std::string sender;
  std::string receiver;
  uint8_t type = 0;
  std::string text;
  bool delivered = false;
  bool is_offline = false;
};

struct ServerConfig {
  int delay_ms = 0;
  double drop_prob = 0.0;
  double corrupt_prob = 0.0;
};

static std::mutex g_clients_mtx;
static std::unordered_map<socket_t, ClientSession> g_clients;
static std::unordered_map<std::string, socket_t> g_nick_to_sock;
static ServerConfig g_cfg;

static std::mutex g_history_mtx;
static std::vector<HistoryRecord> g_history;
static std::unordered_map<std::string, std::deque<uint32_t>> g_offline;
static uint32_t g_next_msg_id = 1;

static const char *kHistoryFile = "history.json";

static std::string json_escape(const std::string &text) {
  std::string out;
  out.reserve(text.size() + 8);
  for (char ch : text) {
    switch (ch) {
    case '\\':
      out += "\\\\";
      break;
    case '"':
      out += "\\\"";
      break;
    case '\n':
      out += "\\n";
      break;
    case '\r':
      out += "\\r";
      break;
    case '\t':
      out += "\\t";
      break;
    default:
      out += ch;
      break;
    }
  }
  return out;
}

static std::string json_unescape(const std::string &text) {
  std::string out;
  out.reserve(text.size());
  for (size_t i = 0; i < text.size(); ++i) {
    if (text[i] == '\\' && i + 1 < text.size()) {
      char next = text[++i];
      switch (next) {
      case '\\':
        out += '\\';
        break;
      case '"':
        out += '"';
        break;
      case 'n':
        out += '\n';
        break;
      case 'r':
        out += '\r';
        break;
      case 't':
        out += '\t';
        break;
      default:
        out += next;
        break;
      }
    } else {
      out += text[i];
    }
  }
  return out;
}

static bool json_extract(const std::string &object, const std::string &key,
                         std::string &value) {
  std::string needle = "\"" + key + "\"";
  size_t pos = object.find(needle);
  if (pos == std::string::npos)
    return false;
  pos = object.find(':', pos + needle.size());
  if (pos == std::string::npos)
    return false;
  ++pos;
  while (pos < object.size() &&
         (object[pos] == ' ' || object[pos] == '\t' || object[pos] == '\n' ||
          object[pos] == '\r'))
    ++pos;
  if (pos >= object.size())
    return false;

  if (object[pos] == '"') {
    ++pos;
    std::string raw;
    bool escaped = false;
    for (; pos < object.size(); ++pos) {
      char ch = object[pos];
      if (escaped) {
        raw += '\\';
        raw += ch;
        escaped = false;
        continue;
      }
      if (ch == '\\') {
        escaped = true;
        continue;
      }
      if (ch == '"')
        break;
      raw += ch;
    }
    value = json_unescape(raw);
    return true;
  }

  size_t end = pos;
  while (end < object.size() && object[end] != ',' && object[end] != '}')
    ++end;
  value = object.substr(pos, end - pos);
  return true;
}

static bool parse_history_object(const std::string &object, HistoryRecord &rec) {
  std::string value;
  if (!json_extract(object, "msg_id", value))
    return false;
  rec.msg_id = (uint32_t)std::strtoul(value.c_str(), nullptr, 10);
  if (!json_extract(object, "timestamp", value))
    return false;
  rec.timestamp = (int64_t)std::strtoll(value.c_str(), nullptr, 10);
  if (!json_extract(object, "sender", value))
    return false;
  rec.sender = value;
  if (!json_extract(object, "receiver", value))
    return false;
  rec.receiver = value;
  if (!json_extract(object, "type", value))
    return false;
  rec.type = (uint8_t)std::strtoul(value.c_str(), nullptr, 10);
  if (!json_extract(object, "text", value))
    return false;
  rec.text = value;
  if (!json_extract(object, "delivered", value))
    return false;
  rec.delivered = (value.find("true") != std::string::npos || value == "1");
  if (!json_extract(object, "is_offline", value))
    return false;
  rec.is_offline = (value.find("true") != std::string::npos || value == "1");
  return true;
}

static void save_history_locked() {
  std::ofstream out(kHistoryFile, std::ios::trunc);
  out << "[\n";
  for (size_t i = 0; i < g_history.size(); ++i) {
    const HistoryRecord &rec = g_history[i];
    out << "  {";
    out << "\"msg_id\":" << rec.msg_id << ",";
    out << "\"timestamp\":" << (long long)rec.timestamp << ",";
    out << "\"sender\":\"" << json_escape(rec.sender) << "\",";
    out << "\"receiver\":\"" << json_escape(rec.receiver) << "\",";
    out << "\"type\":" << (unsigned)rec.type << ",";
    out << "\"text\":\"" << json_escape(rec.text) << "\",";
    out << "\"delivered\":" << (rec.delivered ? "true" : "false") << ",";
    out << "\"is_offline\":" << (rec.is_offline ? "true" : "false");
    out << "}";
    if (i + 1 < g_history.size())
      out << ",";
    out << "\n";
  }
  out << "]\n";
}

static void load_history_locked() {
  g_history.clear();
  g_next_msg_id = 1;

  std::ifstream in(kHistoryFile);
  if (!in.is_open())
    return;

  std::string data((std::istreambuf_iterator<char>(in)),
                   std::istreambuf_iterator<char>());

  size_t pos = 0;
  bool in_string = false;
  bool escaped = false;
  int depth = 0;
  size_t object_start = std::string::npos;
  while (pos < data.size()) {
    char ch = data[pos];
    if (in_string) {
      if (escaped) {
        escaped = false;
      } else if (ch == '\\') {
        escaped = true;
      } else if (ch == '"') {
        in_string = false;
      }
    } else {
      if (ch == '"') {
        in_string = true;
      } else if (ch == '{') {
        if (depth == 0)
          object_start = pos;
        ++depth;
      } else if (ch == '}') {
        --depth;
        if (depth == 0 && object_start != std::string::npos) {
          std::string object = data.substr(object_start, pos - object_start + 1);
          HistoryRecord rec;
          if (parse_history_object(object, rec)) {
            g_next_msg_id = std::max(g_next_msg_id, rec.msg_id + 1);
            g_history.push_back(rec);
          }
          object_start = std::string::npos;
        }
      }
    }
    ++pos;
  }
}

static HistoryRecord make_record(uint32_t msg_id, const std::string &sender,
                                 const std::string &receiver, uint8_t type,
                                 const std::string &text, bool delivered,
                                 bool is_offline) {
  HistoryRecord rec;
  rec.msg_id = msg_id;
  rec.timestamp = (int64_t)time(nullptr);
  rec.sender = sender;
  rec.receiver = receiver;
  rec.type = type;
  rec.text = text;
  rec.delivered = delivered;
  rec.is_offline = is_offline;
  return rec;
}

static HistoryRecord append_history_locked(const std::string &sender,
                                           const std::string &receiver,
                                           uint8_t type,
                                           const std::string &text,
                                           bool delivered,
                                           bool is_offline) {
  std::lock_guard<std::mutex> lock(g_history_mtx);
  HistoryRecord rec =
      make_record(g_next_msg_id++, sender, receiver, type, text, delivered,
                  is_offline);
  g_history.push_back(rec);
  save_history_locked();
  return rec;
}

static std::string format_record(const HistoryRecord &rec) {
  char timebuf[32];
  time_t ts = (time_t)rec.timestamp;
  format_time(ts, timebuf, sizeof(timebuf));

  std::ostringstream oss;
  oss << "[" << timebuf << "][id=" << rec.msg_id << "]";
  if (rec.type == MSG_PRIVATE)
    oss << "[" << (rec.is_offline ? "OFFLINE" : "PRIVATE") << "]";
  if (!rec.receiver.empty())
    oss << "[" << rec.sender << " -> " << rec.receiver << "]";
  else
    oss << "[" << rec.sender << "]";
  oss << ": " << rec.text;
  return oss.str();
}

static void mark_delivered(uint32_t msg_id) {
  std::lock_guard<std::mutex> lock(g_history_mtx);
  for (auto &rec : g_history) {
    if (rec.msg_id == msg_id) {
      rec.delivered = true;
      break;
    }
  }
  save_history_locked();
}

static socket_t find_sock_by_nick_locked(const std::string &nick) {
  auto it = g_nick_to_sock.find(nick);
  if (it == g_nick_to_sock.end())
    return (socket_t)-1;
  return it->second;
}

static void send_message(socket_t sock, MessageType type, uint32_t msg_id,
                         const std::string &sender,
                         const std::string &receiver,
                         const std::string &payload, int64_t timestamp) {
  MessageEx out;
  clear_message(&out);
  out.type = type;
  out.msg_id = msg_id;
  out.timestamp = timestamp;
  std::strncpy(out.sender, sender.c_str(), MAX_NAME - 1);
  std::strncpy(out.receiver, receiver.c_str(), MAX_NAME - 1);
  out.length = (uint32_t)std::min<size_t>(payload.size(), MAX_PAYLOAD - 1);
  std::memcpy(out.payload, payload.data(), out.length);
  out.payload[out.length] = '\0';
  log_tcpip_send("prepare MessageEx", message_type_name(type),
                 sizeof(MessageEx) - MAX_PAYLOAD + out.length, "127.0.0.1");
  (void)msg_send_ex(sock, &out);
}

static void send_ack(socket_t sock, uint32_t msg_id) {
  std::printf("[Transport][ACK] send MSG_ACK (id=%u)\n", msg_id);
  send_message(sock, MSG_ACK, msg_id, "SERVER", "", "", (int64_t)time(nullptr));
}

static bool apply_transport_simulation(MessageEx &msg, std::mt19937 &rng) {
  if (g_cfg.delay_ms > 0) {
    std::printf("[Transport][SIM] DELAY applied: %d ms\n", g_cfg.delay_ms);
    std::this_thread::sleep_for(std::chrono::milliseconds(g_cfg.delay_ms));
  }

  if (g_cfg.drop_prob > 0.0) {
    std::uniform_real_distribution<double> drop_dist(0.0, 1.0);
    if (drop_dist(rng) < g_cfg.drop_prob) {
      std::printf("[Transport][SIM] DROP (id=%u, rate=%.3f)\n", msg.msg_id,
                  g_cfg.drop_prob);
      return false;
    }
  }

  if (g_cfg.corrupt_prob > 0.0 && msg.length > 0) {
    std::uniform_real_distribution<double> corrupt_dist(0.0, 1.0);
    if (corrupt_dist(rng) < g_cfg.corrupt_prob) {
      std::uniform_int_distribution<size_t> pos_dist(0, msg.length - 1);
      size_t pos = pos_dist(rng);
      msg.payload[pos] ^= 0x1;
      std::printf("[Transport][SIM] CORRUPT payload (id=%u)\n", msg.msg_id);
    }
  }

  return true;
}

static void broadcast_server_info(const std::string &text) {
  std::vector<socket_t> recipients;
  {
    std::lock_guard<std::mutex> lock(g_clients_mtx);
    for (const auto &kv : g_clients) {
      if (kv.second.authenticated)
        recipients.push_back(kv.first);
    }
  }

  for (socket_t sock : recipients) {
    send_message(sock, MSG_SERVER_INFO, 0, "SERVER", "", text,
                 (int64_t)time(nullptr));
  }
}

static void broadcast_text(socket_t sender_sock, const std::string &text) {
  std::vector<socket_t> recipients;
  {
    std::lock_guard<std::mutex> lock(g_clients_mtx);
    for (const auto &kv : g_clients) {
      if (kv.first != sender_sock && kv.second.authenticated)
        recipients.push_back(kv.first);
    }
  }

  for (socket_t sock : recipients) {
    send_message(sock, MSG_TEXT, 0, "SERVER", "", text, (int64_t)time(nullptr));
  }
}

static void send_history_to_client(socket_t sock, int count) {
  std::vector<HistoryRecord> selected;
  {
    std::lock_guard<std::mutex> lock(g_history_mtx);
    if (count <= 0 || count > (int)g_history.size())
      count = (int)g_history.size();
    int start = (int)g_history.size() - count;
    if (start < 0)
      start = 0;
    selected.assign(g_history.begin() + start, g_history.end());
  }

  for (const auto &rec : selected) {
    send_message(sock, MSG_HISTORY_DATA, rec.msg_id, "SERVER", "",
                 format_record(rec), rec.timestamp);
  }
}

static void deliver_offline_messages(const std::string &nick) {
  std::deque<uint32_t> pending;
  {
    std::lock_guard<std::mutex> lock(g_history_mtx);
    auto it = g_offline.find(nick);
    if (it != g_offline.end()) {
      pending = it->second;
      g_offline.erase(it);
      save_history_locked();
    }
  }

  for (uint32_t msg_id : pending) {
    HistoryRecord rec;
    bool found = false;
    {
      std::lock_guard<std::mutex> lock(g_history_mtx);
      for (const auto &item : g_history) {
        if (item.msg_id == msg_id) {
          rec = item;
          found = true;
          break;
        }
      }
    }
    if (!found)
      continue;

    socket_t target_sock = (socket_t)-1;
    {
      std::lock_guard<std::mutex> lock(g_clients_mtx);
      target_sock = find_sock_by_nick_locked(nick);
    }
    if (target_sock == (socket_t)-1)
      break;

    send_message(target_sock, MSG_PRIVATE, rec.msg_id, rec.sender, rec.receiver,
                 format_record(rec), rec.timestamp);
    mark_delivered(rec.msg_id);
  }
}

static void remove_client(socket_t sock) {
  std::lock_guard<std::mutex> lock(g_clients_mtx);
  auto it = g_clients.find(sock);
  if (it == g_clients.end())
    return;
  if (it->second.authenticated)
    g_nick_to_sock.erase(it->second.nick);
  g_clients.erase(it);
}

static void handle_client(socket_t sock) {
  sockaddr_in peer{};
  socklen_t peer_len = sizeof(peer);
  getpeername(sock, (sockaddr *)&peer, &peer_len);

  char peer_ip[INET_ADDRSTRLEN] = {0};
#if defined(_WIN32)
  std::snprintf(peer_ip, sizeof(peer_ip), "%s", inet_ntoa(peer.sin_addr));
#else
  inet_ntop(AF_INET, &peer.sin_addr, peer_ip, sizeof(peer_ip));
#endif
  printf("Client connected from %s:%d\n", peer_ip, ntohs(peer.sin_port));

  send_message(sock, MSG_HELLO, 0, "SERVER", "",
               "Welcome to the TCP/IP chat server", (int64_t)time(nullptr));

  MessageEx msg;
  clear_message(&msg);
  if (msg_recv_ex(sock, &msg) <= 0 || msg.type != MSG_WELCOME) {
    socket_close(sock);
    return;
  }

  if (msg_recv_ex(sock, &msg) <= 0 || msg.type != MSG_AUTH || msg.length == 0) {
    send_message(sock, MSG_ERROR, 0, "SERVER", "",
                 "Authentication required", (int64_t)time(nullptr));
    if (msg.msg_id != 0)
      send_ack(sock, msg.msg_id);
    socket_close(sock);
    return;
  }

  std::string nick = msg.sender[0] ? msg.sender : std::string(msg.payload);
  if (nick.empty()) {
    send_message(sock, MSG_ERROR, 0, "SERVER", "",
                 "Nickname cannot be empty", (int64_t)time(nullptr));
    if (msg.msg_id != 0)
      send_ack(sock, msg.msg_id);
    socket_close(sock);
    return;
  }

  {
    std::lock_guard<std::mutex> lock(g_clients_mtx);
    if (!g_nick_to_sock.count(nick)) {
      ClientSession session;
      session.sock = sock;
      session.nick = nick;
      session.authenticated = true;
      session.addr = peer;
      g_clients[sock] = session;
      g_nick_to_sock[nick] = sock;
    } else {
      send_message(sock, MSG_ERROR, 0, "SERVER", "",
                   "Nickname already taken", (int64_t)time(nullptr));
      if (msg.msg_id != 0)
        send_ack(sock, msg.msg_id);
      socket_close(sock);
      return;
    }
  }

  send_message(sock, MSG_SERVER_INFO, 0, "SERVER", "",
               "Authentication successful", (int64_t)time(nullptr));

  std::string join_msg = "User [" + nick + "] connected";
  printf("%s\n", join_msg.c_str());
  broadcast_server_info(join_msg);
  deliver_offline_messages(nick);
  if (msg.msg_id != 0)
    send_ack(sock, msg.msg_id);

  std::unordered_set<uint32_t> seen_ids;
  std::mt19937 rng{std::random_device{}()};

  while (true) {
    if (msg_recv_ex(sock, &msg) <= 0) {
      std::string leave = "User [" + nick + "] disconnected";
      printf("%s\n", leave.c_str());
      broadcast_server_info(leave);
      break;
    }

    if (msg.type == MSG_ACK)
      continue;

    if (msg.msg_id != 0 && seen_ids.find(msg.msg_id) != seen_ids.end()) {
      std::printf("[Application][DEDUP] duplicate ignored (id=%u)\n",
                  msg.msg_id);
      send_ack(sock, msg.msg_id);
      continue;
    }

    if (!apply_transport_simulation(msg, rng))
      continue;

    if (msg.msg_id != 0)
      seen_ids.insert(msg.msg_id);

    std::printf("[Application][ACK] process %s (id=%u)\n",
                message_type_name(msg.type), msg.msg_id);

    if (msg.type == MSG_TEXT) {
      HistoryRecord rec = append_history_locked(nick, "", MSG_TEXT, msg.payload,
                                                true, false);
      std::string line = format_record(rec);
      printf("%s\n", line.c_str());
      broadcast_text(sock, line);
      send_ack(sock, msg.msg_id);
    } else if (msg.type == MSG_PRIVATE) {
      std::string receiver(msg.receiver);
      std::string text(msg.payload);
      if (receiver.empty()) {
        send_message(sock, MSG_ERROR, 0, "SERVER", "",
                     "Private message requires receiver",
                     (int64_t)time(nullptr));
        send_ack(sock, msg.msg_id);
        continue;
      }

      socket_t target_sock = (socket_t)-1;
      {
        std::lock_guard<std::mutex> lock(g_clients_mtx);
        target_sock = find_sock_by_nick_locked(receiver);
      }

      if (target_sock != (socket_t)-1) {
        HistoryRecord rec = append_history_locked(nick, receiver, MSG_PRIVATE,
                                                  text, true, false);
        std::string line = format_record(rec);
        printf("%s\n", line.c_str());
        send_message(target_sock, MSG_PRIVATE, rec.msg_id, nick, receiver, line,
                     rec.timestamp);
        send_message(sock, MSG_PRIVATE, rec.msg_id, nick, receiver, line,
                     rec.timestamp);
      } else {
        HistoryRecord rec = append_history_locked(nick, receiver, MSG_PRIVATE,
                                                  text, false, true);
        {
          std::lock_guard<std::mutex> lock(g_history_mtx);
          g_offline[receiver].push_back(rec.msg_id);
          save_history_locked();
        }
        std::string line = format_record(rec);
        printf("%s\n", line.c_str());
        send_message(sock, MSG_SERVER_INFO, rec.msg_id, "SERVER", "",
                     "Message stored for offline delivery",
                     rec.timestamp);
      }
      send_ack(sock, msg.msg_id);
    } else if (msg.type == MSG_PING) {
      std::printf("[Transport][PING] recv MSG_PING (id=%u)\n", msg.msg_id);
      send_ack(sock, msg.msg_id);
      std::printf("[Transport][PING] send MSG_PONG (id=%u)\n", msg.msg_id);
      send_message(sock, MSG_PONG, 0, "SERVER", "", "PONG",
                   (int64_t)time(nullptr));
    } else if (msg.type == MSG_BYE) {
      send_message(sock, MSG_BYE, 0, "SERVER", "", "bye",
                   (int64_t)time(nullptr));
      std::string leave = "User [" + nick + "] disconnected";
      printf("%s\n", leave.c_str());
      broadcast_server_info(leave);
      send_ack(sock, msg.msg_id);
      break;
    } else if (msg.type == MSG_LIST) {
      std::ostringstream oss;
      oss << "Online users\n";
      {
        std::lock_guard<std::mutex> lock(g_clients_mtx);
        for (const auto &kv : g_clients) {
          if (kv.second.authenticated)
            oss << kv.second.nick << "\n";
        }
      }
      send_message(sock, MSG_SERVER_INFO, 0, "SERVER", "", oss.str(),
                   (int64_t)time(nullptr));
      send_ack(sock, msg.msg_id);
    } else if (msg.type == MSG_HISTORY) {
      int count = 10;
      if (msg.length > 0) {
        count = std::atoi(msg.payload);
        if (count <= 0)
          count = 10;
      }
      send_history_to_client(sock, count);
      send_ack(sock, msg.msg_id);
    } else if (msg.type == MSG_HELP) {
      send_message(sock, MSG_SERVER_INFO, 0, "SERVER", "",
                   "Available commands: /help /list /history /history N /quit "
                   "/w <nick> <message> /ping",
                   (int64_t)time(nullptr));
      send_ack(sock, msg.msg_id);
    } else {
      send_message(sock, MSG_ERROR, 0, "SERVER", "", "Unknown message type",
                   (int64_t)time(nullptr));
      send_ack(sock, msg.msg_id);
    }
  }

  socket_close(sock);
  remove_client(sock);
}

int main(int argc, char **argv) {
  if (socket_init() != 0) {
    std::fprintf(stderr, "socket_init failed\n");
    return 1;
  }

  // Parse simple simulation flags from the command line.
  // Supported:
  //   --delay=100
  //   --drop=0.2
  //   --corrupt=0.1
  // Port remains the default 8080 for this lab.
  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    if (arg.rfind("--delay=", 0) == 0) {
      g_cfg.delay_ms = std::atoi(arg.substr(8).c_str());
    } else if (arg.rfind("--drop=", 0) == 0) {
      g_cfg.drop_prob = std::atof(arg.substr(7).c_str());
    } else if (arg.rfind("--corrupt=", 0) == 0) {
      g_cfg.corrupt_prob = std::atof(arg.substr(10).c_str());
    }
  }

  {
    std::lock_guard<std::mutex> lock(g_history_mtx);
    load_history_locked();
  }

  socket_t server_sock = socket(AF_INET, SOCK_STREAM, 0);
  if (server_sock == (socket_t)-1) {
    std::perror("socket");
    socket_cleanup();
    return 1;
  }

  int opt = 1;
  setsockopt(server_sock, SOL_SOCKET, SO_REUSEADDR, (const char *)&opt,
             sizeof(opt));

  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = INADDR_ANY;
  addr.sin_port = htons(PORT);

  if (bind(server_sock, (sockaddr *)&addr, sizeof(addr)) < 0) {
    std::perror("bind");
    socket_close(server_sock);
    socket_cleanup();
    return 1;
  }

  if (listen(server_sock, 16) < 0) {
    std::perror("listen");
    socket_close(server_sock);
    socket_cleanup();
    return 1;
  }

  std::printf("=== TCP/IP chat server started on port %d ===\n", PORT);
  std::printf("[Application] history file: %s\n", kHistoryFile);

  while (true) {
    sockaddr_in client_addr{};
    socklen_t addr_len = sizeof(client_addr);
    socket_t client_sock =
        accept(server_sock, (sockaddr *)&client_addr, &addr_len);
    if (client_sock == (socket_t)-1) {
      std::perror("accept");
      continue;
    }

    {
      std::lock_guard<std::mutex> lock(g_clients_mtx);
      g_clients[client_sock] = ClientSession{};
    }

    std::thread(handle_client, client_sock).detach();
  }

  socket_close(server_sock);
  socket_cleanup();
  return 0;
}

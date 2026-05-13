CXX := g++
CXXFLAGS := -std=c++17 -Wall -Wextra -O2
LDFLAGS := -pthread

ifeq ($(OS),Windows_NT)
LDFLAGS += -lws2_32
endif

TARGETS := server client

.PHONY: all clean

all: $(TARGETS)

server: server.cpp protocol.h
	$(CXX) $(CXXFLAGS) -o $@ server.cpp $(LDFLAGS)

client: client.cpp protocol.h
	$(CXX) $(CXXFLAGS) -o $@ client.cpp $(LDFLAGS)

clean:
	rm -f $(TARGETS) history.json net_diag_*.json

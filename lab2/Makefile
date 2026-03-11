CXX = g++
CXXFLAGS = -Wall -Wextra -std=c++17

all: server client

server: server.cpp
	$(CXX) $(CXXFLAGS) server.cpp -o server

client: client.cpp
	$(CXX) $(CXXFLAGS) client.cpp -o client

clean:
	rm -f server client

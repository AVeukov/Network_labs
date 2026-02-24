CXX = g++
CXXFLAGS = -Wall -Wextra -std=c++17

SERVER = server
CLIENT = client

SERVER_SRC = server.cpp
CLIENT_SRC = client.cpp

all: $(SERVER) $(CLIENT)

$(SERVER): $(SERVER_SRC)
	$(CXX) $(CXXFLAGS) $< -o $@

$(CLIENT): $(CLIENT_SRC)
	$(CXX) $(CXXFLAGS) $< -o $@

clean:
	rm -f $(SERVER) $(CLIENT)

rebuild: clean all
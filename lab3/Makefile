CXX = g++
CXXFLAGS = -Wall -Wextra
LDLIBS = -pthread

all: tcp_server_pool tcp_client_reconnect

tcp_server_pool: tcp_server_pool.cpp protocol.hpp
	$(CXX) $(CXXFLAGS) -o $@ tcp_server_pool.cpp $(LDLIBS)

tcp_client_reconnect: tcp_client_reconnect.cpp protocol.hpp
	$(CXX) $(CXXFLAGS) -o $@ tcp_client_reconnect.cpp $(LDLIBS)

clean:
	rm -f tcp_server_pool tcp_client_reconnect

.PHONY: all clean


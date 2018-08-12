# -fsanitize=undefined
CXXFLAGS += -std=c++11 -O3 -Wall -Wextra -I/usr/include/rados -I/usr/include/jsoncpp
#-Wa,-adhln -g
LDFLAGS += -pthread -lrados -ljsoncpp -lstdc++

#CC=clang-6.0


main: main.o
	$(CC) $^ -o $@ $(LDFLAGS)

.cpp.o:
	$(CC) $(CPPFLAGS) $(CXXFLAGS) -c $< -o $@

indent: main.cpp
	clang-format-6.0 -i main.cpp

builddep:
	sudo apt install -y --no-install-recommends libjsoncpp-dev

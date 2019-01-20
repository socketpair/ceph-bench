# -fsanitize=undefined
CXXFLAGS += -std=c++11 -O3 -Wall -Wextra -I/usr/include/rados -I/usr/include/jsoncpp
#-Wa,-adhln -g
LDFLAGS += -pthread -lrados -ljsoncpp -lstdc++ -lm

#CC=clang-6.0


main: main.o mysignals.o radosutil.o
	$(CC) $^ -o $@ $(LDFLAGS)

.cpp.o:
	$(CC) $(CPPFLAGS) $(CXXFLAGS) -c $< -o $@

indent: *.cpp *.h
	clang-format-6.0 -i $^

builddep:
	sudo apt install -y --no-install-recommends libjsoncpp-dev
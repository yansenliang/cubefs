CC = gcc
CFLAGS = -Wall -g  -std=gnu11 -Wno-unused-function -Wno-deprecated-declarations

LDFLAGS = -Wl,-rpath=.
LIB = -L. -L/usr/lib64 -L/usr/local/lib64 -L/usr/lib -L/usr/local/lib

INCLUDE = -I/usr/include -I/usr/local/include -I.
C_FILES=$(wildcard ./*.c )
C_OBJS=$(patsubst ./%.c,%.o,$(C_FILES))

CPPFLAGS = -Wall -g -fPIC -std=c++11 -Wno-unused-function -Wno-deprecated-declarations
CPP_FILES=$(wildcard *.cpp)
CPP_OBJS=$(patsubst %.cpp,%.o,$(CPP_FILES))


#SUBDIRS=test examples

LINK_SO = -lrdmacm -libverbs
MACRO=-DCBRDMA_WITH_DEBUG

.PRECIOUS: %.o

all: libcbrdma.so
	mkdir -p ../../../../build/bin/
	cp libcbrdma.so ../../../../build/bin/

libcbrdma.so: $(C_OBJS) $(STATS_OBJS)
	$(CC) -shared $(CFLAGS) $(MACRO) $^ -o $@ $(INCLUDE) $(LIB) $(LINK_SO)

%.o:./%.c
	$(CC) -fPIC $(CFLAGS) $(MACRO) -c $^ -o $@ $(INCLUDE)

.PHONY: clean

clean:
	@echo Making clean
	rm -rf *.so *.gcno *.gcda *.o

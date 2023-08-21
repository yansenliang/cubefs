.PHONY: clean

CXXFLAGS  := -Wall -g
LDLIBS  := ${LDLIBS} -lrdmacm -libverbs -lpthread -lglog -lgflags -lgtest -lcbrdma -I../src -L. -Wl,-rpath=.

APPS    := pingpong throughput

all: ${APPS}


clean:
	\rm -f ${APPS}

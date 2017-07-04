CC=g++
CFLAGS=-std=c++11 -c  -Wall -fgnu-tm -lleveldb
INCLUDES=-I../  -I/home/ming/workspace/kvseparation/src
SOURCES=$(wildcard *.cc)
OBJECTS=$(SOURCES:.cc=.o)

all: $(SOURCES) $(OBJECTS)

.cc.o:
	$(CC) $(CFLAGS) $(INCLUDES) $< -o $@

clean:
	$(RM) $(OBJECTS)


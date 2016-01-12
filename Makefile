CXX = g++
CXXFLAG = -O2 -std=c++11 -Wall
LFLAG = -l

LIBDIR0 = /usr/local/lib/
LIBDIRS = -L${LIBDIR0}

INC0 = ${HOME}/banana/include/
INC1 = /usr/local/include/
INC2 = /usr/include/
INC3 = /home/htao/Simple-Web-Server/
INC4 = /usr/local/include/tds++
INCDIRS = -I${INC0} -I${INC1} -I${INC2} -I${INC3} -I${INC4}

LIBBOOPROGRAMOPTIONS = -lboost_program_options
LIBBOOFILESYSTEM = -lboost_filesystem
LIBBOOSYSTEM = -lboost_system 
LIBBOOTHREAD = -lboost_thread 
LIBBOOCOROUTINE = -lboost_coroutine
LIBBOOCONTEXT = -lboost_context
LIBJSONCPP = -ljsoncpp
LIBLMDB = -llmdb
LDFLAGS   = $(LIBBOOPROGRAMOPTIONS) $(LIBBOOFILESYSTEM) \
	$(LIBBOOSYSTEM) $(LIBBOOTHREAD) \
	$(LIBBOOCOROUTINE) $(LIBBOOCONTEXT) \
	$(LIBJSONCPP) $(LIBLMDB)

DEBUGF = -g -D DEBUG
DEBUG = no

BIN=banana

SRC=$(wildcard *.cpp)
OBJ=$(SRC:%.cpp=%.o)

.SUFFIXES: .exe .CXX
.CXX.exe: $(CXX) $(CXXFLAG) $@ $< $@

.PHONY: clean

banana: $(SRC) 
	$(CXX) $(CXXFLAG) $(INCDIRS) -o $(BIN) $^ $(LIBDIRS) $(LDFLAGS)

#%.o: %.c 
#	$(CXX) $@ -c $<

clean: 
	rm -f *.o
	rm $(BIN)
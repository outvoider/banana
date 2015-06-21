CXX = g++
CXXFLAG = -g -O3 -std=c++11 -Wall
LFLAG = -l
IFLAG = -I
LIBFLAG = -L

#LIBDIR = /usr/local/lib/
#INCLUDEDIR = ./include/

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
LIBTDSPP = -l:libtds++.so
LDFLAGS   = $(LIBBOOPROGRAMOPTIONS) $(LIBBOOFILESYSTEM) \
	$(LIBBOOSYSTEM) $(LIBBOOTHREAD) \
	$(LIBBOOCOROUTINE) $(LIBBOOCONTEXT) \
	$(LIBJSONCPP) $(LIBLMDB) $(LIBTDSPP)

DEBUGF = -g -D DEBUG
DEBUG = no

BIN=banana
ODIR=build

SRC=$(wildcard *.cpp)
OBJ=$(SRC:%.cpp=%.o)

.SUFFIXES: .exe .CXX
.CXX.exe: $(CXX) $(CXXFLAG) $@ $< $@

hellomake: 
	$(OBJ)
	$(CXX) -o $@ $^ $(CFLAGS) $(LIBS)

main.exe: main.cpp
	$(CXX) $(IFLAG) $(INCLUDEDIR) $(CXXFLAG) boost.exe main.cpp $(LIBFLAG)$(LIBDIR) $(LFLAG)libboost_regex

.PHONY: clean

banana: $(SRC) 
	$(CXX) $(CXXFLAG) $(INCDIRS) -o $(BIN) $^ $(LIBDIRS) $(LDFLAGS)

#%.o: %.c 
#	$(CXX) $@ -c $<

clean: 
	rm -f *.o
	rm $(BIN)
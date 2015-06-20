CXX = g++
CXXFLAG = -std=c++11 -pedantic -Wall
LFLAG = -l
IFLAG = -I
LIBFLAG = -L

LIBDIR = /usr/local/lib/
INCLUDEDIR = ./include/

INC0 = ./include/
INC1 = /usr/local/include/
INC2 = /usr/include/
INC3 = /home/htao/Simple-Web-Server/
INCDIRS = -I${INC0} -I${INC1} -I${INC2} -I${INC3}

DEBUGF = -g -D DEBUG
DEBUG = no

BIN=build
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
	echo ${INCDIRS}
	$(CXX) $(CXXFLAG) $(INCDIRS) -o $(BIN) $^

#%.o: %.c 
#	$(CXX) $@ -c $<

clean: 
	rm -f *.o
	rm $(BIN)
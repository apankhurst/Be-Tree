CC = g++

CXXFLAGS=-Wall -std=c++11 -g -O3 

INC = ../include
SRC = ../src

HEADERS = $(wildcard $(INC)/*.hpp)
SOURCES = $(wildcard $(SRC)/*.cpp)
OBJECTS = $(SOURCES:.cpp=.o)


new_test: new_test.cpp $(HEADERS) $(OBJECTS)
	$(CC) $(CXXFLAGS) -I$(INC) -o new_test new_test.cpp $(OBJECTS)

.cpp.o: 
	$(CC) $(CXXFLAGS) -I$(INC) -c $< -o $@

clean:
	$(RM) *.o $(OBJECTS) 

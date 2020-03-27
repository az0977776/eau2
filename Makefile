all: clean build test

build:
	docker build -t cs4500:0.1 .
	docker run -it -v `pwd`:/test cs4500:0.1 bash -c "cd test/tests && cmake . && make"

server:
	g++ -pthread -O3 -Wall -pedantic -std=c++11 src/kvstore/server.cpp -o server && ./server

client:
	g++ -pthread -O3 -Wall -pedantic -std=c++11 src/kvstore/client.cpp -o client && ./client

kvstore:
	g++ -pthread -O3 -Wall -pedantic -std=c++11 src/kvstore/kvstore.cpp -o kvstore && ./kvstore

test: build 
	docker run -it -v `pwd`:/test cs4500:0.1 bash -c "cd test/tests && ./test_suite"

valgrind: 
	docker run -it -v `pwd`:/test cs4500:0.1 bash -c "cd test/tests && g++ -pthread -O3 -Wall -pedantic -std=c++11 milestone_2.cpp -o milestone2"
	docker run -it -v `pwd`:/test cs4500:0.1 bash -c "cd test/tests && valgrind --leak-check=yes --track-origins=yes ./milestone2"

milestone2: 
	docker run -it -v `pwd`:/test cs4500:0.1 bash -c "cd test/tests && g++ -pthread -O3 -Wall -pedantic -std=c++11 milestone_2.cpp -o milestone2"
	docker run -it -v `pwd`:/test cs4500:0.1 bash -c "cd test/tests && ./milestone2"

clean:
	-rm -rf tests/CMakeCache.txt
	-rm tests/test_suite
	-rm tests/milestone2
	-rm server
	-rm client

.PHONY: server client kvstore

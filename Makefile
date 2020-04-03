all: clean build test

test_all: test valgrind milestone2 

build:
	docker build -t cs4500:0.1 .
	docker run -it -v `pwd`:/test cs4500:0.1 bash -c "cd test/tests/unit_tests && cmake . && make"

server:
	g++ -pthread -O3 -Wall -pedantic -std=c++11 tests/server.cpp -o server
	./server &

client:
	g++ -pthread -O3 -Wall -pedantic -std=c++11 tests/client.cpp -o client && ./client

kvstore:
	g++ -pthread -O3 -Wall -pedantic -std=c++11 tests/kvstore.cpp -o kvstore && ./kvstore

test: build 
	docker run -it -v `pwd`:/test cs4500:0.1 bash -c "cd test/tests/unit_tests && ./test_suite"

valgrind: 
	docker run -it -v `pwd`:/test cs4500:0.1 bash -c "cd test/tests && g++ -pthread -O3 -Wall -pedantic -std=c++11 valgrind.cpp -o valgrind"
	docker run -it -v `pwd`:/test cs4500:0.1 bash -c "cd test/tests && valgrind --leak-check=yes --track-origins=yes ./valgrind"

milestone2: 
	docker run -it -v `pwd`:/test cs4500:0.1 bash -c "cd test/tests && g++ -pthread -O3 -Wall -pedantic -std=c++11 milestone_2.cpp -o milestone2"
	docker run -it -v `pwd`:/test cs4500:0.1 bash -c "cd test/tests && ./milestone2"

milestone3: server
	g++ -pthread -O3 -Wall -pedantic -std=c++11 tests/milestone_3.cpp -o milestone3
	./milestone3 &
	./milestone3 &
	./milestone3

milestone4: server
	g++ -pthread -O3 -Wall -pedantic -std=c++11 tests/milestone_4.cpp -o milestone4
	./milestone4 data/word_count.txt &
	./milestone4 data/word_count.txt &
	./milestone4 data/word_count.txt 

word_count: server
	g++ -pthread -O3 -Wall -pedantic -std=c++11 tests/milestone_4.cpp -o milestone4
	./milestone4 $(filename)&
	./milestone4 $(filename)&
	./milestone4 $(filename)

clean:
	-rm -rf tests/CMakeCache.txt
	-rm tests/unit_tests/test_suite
	-rm tests/milestone2
	-rm server
	-rm client
	-rm milestone3
	-rm milestone4
	-rm kvstore
	-rm tests/valgrind

.PHONY: server client kvstore milestone2 milestone3 milestone4 word_count

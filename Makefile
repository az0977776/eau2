all: clean build test

test_all: test milestone2 valgrind

build:
	docker build -t cs4500:0.1 .
	docker run -it -v `pwd`:/test cs4500:0.1 bash -c "cd test/tests/unit_tests && cmake . && make"

test: build 
	cp milestones/default_config.txt tests/unit_tests/config.txt
	docker run -it -v `pwd`:/test cs4500:0.1 bash -c "cd test/tests/unit_tests && ./test_suite"

valgrind: 
	docker run -it -v `pwd`:/test cs4500:0.1 bash -c "cd test && g++ -pthread -O3 -Wall -pedantic -std=c++11 tests/valgrind.cpp -o valgrind"
	docker run -it -v `pwd`:/test cs4500:0.1 bash -c "cd test && valgrind --leak-check=yes --track-origins=yes ./valgrind"

server:
	g++ -pthread -O3 -Wall -pedantic -std=c++11 src/server.cpp -o server
	./server &

milestone2_docker:
	docker run -it -v `pwd`:/test cs4500:0.1 bash -c "cd test && g++ -pthread -O3 -Wall -pedantic -std=c++11 milestones/milestone_2.cpp -o milestone2"
	docker run -it -v `pwd`:/test cs4500:0.1 bash -c "cd test && ./milestone2"	

milestone2: 
	g++ -pthread -O3 -Wall -pedantic -std=c++11 milestones/milestone_2.cpp -o milestone2 && ./milestone2

milestone3: server
	g++ -pthread -O3 -Wall -pedantic -std=c++11 milestones/milestone_3.cpp -o milestone3
	./milestone3 &
	./milestone3 &
	./milestone3

milestone4: server
	g++ -pthread -O3 -Wall -pedantic -std=c++11 milestones/milestone_4.cpp -o milestone4
	./milestone4 data/word_count.txt &
	./milestone4 data/word_count.txt &
	./milestone4 data/word_count.txt 

milestone5: server
	g++ -pthread -O3 -Wall -pedantic -std=c++11 milestones/milestone_5.cpp -o milestone5
	./milestone5 &
	./milestone5 &
	./milestone5 &
	./milestone5	

word_count: server
	g++ -pthread -O3 -Wall -pedantic -std=c++11 milestones/milestone_4.cpp -o milestone4
	./milestone4 $(filename)&
	./milestone4 $(filename)&
	./milestone4 $(filename)

clean:
	-rm -rf tests/CMakeCache.txt
	-rm tests/unit_tests/test_suite
	-rm milestone2
	-rm server
	-rm milestone3
	-rm milestone4
	-rm milestone5
	-rm valgrind
	-rm tests/unit_tests/config.txt tests/config.txt config.txt

.PHONY: server client kvstore milestone2 milestone3 milestone4 word_count milestone5 valgrind

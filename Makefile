all: clean build run

build:
	g++ -pthread -O3 -Wall -pedantic -std=c++11 bench.cpp -o bench

run: build
	git clone https://github.com/barthch/CS4500-a5p1-data.git || (cd CS4500-a5p1-data ; git pull)
	unzip -o CS4500-a5p1-data/data.zip
	./bench

clean:
	-rm bench
	-rm -rf bench.dSYM
	-rm -rf data.zip data.csv CS4500-a5p1-data

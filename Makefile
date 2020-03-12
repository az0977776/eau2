all: clean build test

build:
	docker build -t cs4500:0.1 .
	docker run -it -v `pwd`:/test cs4500:0.1 bash -c "cd test/tests && cmake . && make"

test: build
	docker run -it -v `pwd`:/test cs4500:0.1 bash -c "cd test/tests && ./test_dataframe"
	docker run -it -v `pwd`:/test cs4500:0.1 bash -c "cd test/tests && ./test_sorer"

valgrind: build
	docker run -it -v `pwd`:/test cs4500:0.1 bash -c "cd test/tests && valgrind --leak-check=yes ./test_dataframe"
	docker run -it -v `pwd`:/test cs4500:0.1 bash -c "cd test/tests && valgrind --leak-check=yes ./test_sorer"

clean:
	-rm -rf tests/CMakeCache.txt
	-rm tests/test_dataframe
	-rm tests/test_sorer

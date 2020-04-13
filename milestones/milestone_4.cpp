#include <unistd.h>
#include <stdio.h>

#include "../src/application/milestone_4.h"
#include "../src/kvstore/keyvaluestore.h"

int main(int argc, char **argv) {
    if (argc != 2) {
        printf("Usage: ./milestone4 <filename>\n");
        exit(1);
    }
    KVStore kvs;
    WordCount wc(argv[1], kvs);

    wc.run_();

    // this is to keep the program running
    // server should last for 20 seconds and should send a teardown to shut down the clients
    sleep(30);

    return 0;
}
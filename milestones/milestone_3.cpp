#include <stdlib.h>

#include "../src/application/milestone_3.h"
#include "../src/kvstore/keyvaluestore.h"

int main() {
    KVStore kvs;
    Demo demo(kvs);

    demo.run_();

    // this is to keep the program running
    sleep(20);

    return 0;
}

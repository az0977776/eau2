#include "../src/application/milestone_5.h"
#include "../src/kvstore/keyvaluestore.h"


int main() {
    KVStore kvs;
    Linus lin(kvs);

    lin.run_();
    /**
     * run on total files with 4 nodes. (Each process memory ~1.6 GB)
     * NODE 0:     after stage 6:
     * NODE 0:         tagged projects: 42827532 / 125486231
     * NODE 0:         tagged users: 8521984 / 32411734
     */

    sleep(70);

    return 0;
}
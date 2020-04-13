#include <stdlib.h>

#include "../src/application/milestone_2.h"

int main() {
    KVStore kv(false);  // do not run the client
    Trivial t(0, &kv);
    t.run_();

    printf("Milestone2: OK\n");
    return 0;
}



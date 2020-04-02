#include <stdlib.h>

#include "../src/kvstore/keyvaluestore.h"
#include "../src/dataframe/dataframe.h"

static const size_t SZ = 100;

void run_double(KVStore* kv) {
    double* vals = new double[SZ];
    double sum = 0;
    for (size_t i = 0; i < SZ; ++i) sum += vals[i] = i;
    Key* key = new Key(0, "double");
    DataFrame* df = DataFrame::fromArray(key, kv, SZ, vals);
    assert(df->get_double(0,1) == 1);
    Value* v = kv->get(*key);
    DataFrame* df2 = DataFrame::deserialize(v->get(), kv);
    for (size_t i = 0; i < SZ; ++i) sum -= df2->get_double(0,i);
    assert(sum==0);

    delete df; 
    delete df2;
    delete[] vals;
    delete v;
    delete key;
}

void run_bool(KVStore* kv) {
    bool* vals = new bool[SZ];
    for (size_t i = 0; i < SZ; ++i) {
        vals[i] = i % 2;
    }
    
    Key* key = new Key(0, "bool");
    DataFrame* df = DataFrame::fromArray(key, kv, SZ, vals);
    assert(df->get_bool(0,1) == 1);
    assert(df->get_bool(0, 32) == 0);

    Value* v = kv->get(*key);
    DataFrame* df2 = DataFrame::deserialize(v->get(), kv);
    for (size_t i = 0; i < SZ; ++i) {
        assert(df2->get_bool(0,i) == i % 2);
    }

    delete df; 
    delete df2;
    delete[] vals;
    delete v;
    delete key;
}

int main() {
    KVStore kv(false);  // do not run the client
    run_bool(&kv);

    printf("Valgrind: OK\n");
    return 0;
}



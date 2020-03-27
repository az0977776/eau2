#include <stdlib.h>

#include "../src/kvstore/keyvaluestore.h"
#include "../src/dataframe/dataframe.h"

class Trivial {
    public:
        KVStore *kv_;

        Trivial(size_t idx, KVStore* kv) {
            kv_ = kv;
        }

        void run_() {
            size_t SZ = 10000;
            double* vals = new double[SZ];
            double sum = 0;
            for (size_t i = 0; i < SZ; ++i) sum += vals[i] = i;
            Key* key = new Key(0, "triv");
            DataFrame* df = DataFrame::fromArray(key, kv_, SZ, vals);
            assert(df->get_double(0,1) == 1);
            Value* v = kv_->get(*key);
            DataFrame* df2 = DataFrame::deserialize(v->get(), kv_);
            for (size_t i = 0; i < SZ; ++i) sum -= df2->get_double(0,i);
            assert(sum==0);

            delete df; 
            delete df2;
            delete[] vals;
            delete v;
            delete key;
        }
};

int main() {
    KVStore kv(false);  // do not run the client
    Trivial t(0, &kv);
    t.run_();

    printf("Milestone2: OK\n");
    return 0;
}



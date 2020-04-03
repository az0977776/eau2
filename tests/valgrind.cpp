#include <stdlib.h>

#include "../src/kvstore/keyvaluestore.h"
#include "../src/dataframe/dataframe.h"
#include "../src/util/map.h"

static const size_t SZ = 10000;

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

void run_int(KVStore* kv) {
    int* vals = new int[SZ];
    int sum = 0;
    for (size_t i = 0; i < SZ; ++i) sum += vals[i] = i;
    Key* key = new Key(0, "int_col");
    DataFrame* df = DataFrame::fromArray(key, kv, SZ, vals);
    assert(df->get_int(0,1) == 1);
    Value* v = kv->get(*key);
    DataFrame* df2 = DataFrame::deserialize(v->get(), kv);
    for (size_t i = 0; i < SZ; ++i) sum -= df2->get_int(0,i);
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

void run_string(KVStore* kv) {
    String s0("a single string");
    String s1("a really long string that is stored in some list of char is not very hard ... continue");
    String s2("");
    String** vals = new String*[SZ];
    for (size_t i = 0; i < SZ; ++i) {
        switch (i % 3) {
            case 0:
                vals[i] = &s0;
                break;
            case 1:
                vals[i] = &s1;
                break;
            default:
                vals[i] = &s2;
                break;
        }
    }

    String* test;
    
    // NOTE: fromArray will copy the values of the strings
    Key* key = new Key(0, "string_col");
    DataFrame* df = DataFrame::fromArray(key, kv, SZ, vals);
    test = df->get_string(0,1);
    assert(test->equals(&s1));
    delete test;

    test = df->get_string(0, 32);
    assert(test->equals(&s2));
    delete test;

    Value* v = kv->get(*key);
    DataFrame* df2 = DataFrame::deserialize(v->get(), kv);
    for (size_t i = 0; i < SZ; ++i) {
        test = df2->get_string(0, i);
        switch (i % 3) {
            case 0:
                assert(test->equals(&s0));
                break;
            case 1:
                assert(test->equals(&s1));
                break;
            default:
                assert(test->equals(&s2));
                break;
        }
        delete test;
    }

    delete df; 
    delete df2;
    delete[] vals;
    delete v;
    delete key;
}

void map_clear_and_delete() {
    Map<String, String> m;
    String s0("foo");
    String s1("bar");
    String s2("baz");
    String s3("cat");
    String s4("dog");
    String s5("fish");

    m.add(s0.clone(), s1.clone());
    m.add(s2.clone(), s3.clone());
    m.add(s4.clone(), s5.clone());
    
    m.delete_and_clear_items();
}

int main() {
    KVStore kv(false);  // do not run the client
    run_bool(&kv);
    run_int(&kv);
    run_double(&kv);
    run_string(&kv);
    map_clear_and_delete();

    printf("Valgrind: OK\n");
    return 0;
}



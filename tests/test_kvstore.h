#include <gtest/gtest.h>

#include "../src/kvstore/keyvaluestore.h"
#include "../src/kvstore/keyvalue.h"

#include "test_macros.h"

void test_kvstore_put_get() {
    KVStore kvs;
    Key key1(0, "A Test");
    Key other_node(123, "A Test");
    Key key2(0, "Second key");

    Value v(15, "A VALUE TEST");
    Value v2(20, "different value");

    EXPECT_EQ(kvs.get(key1), nullptr);
    EXPECT_EQ(kvs.get(other_node), nullptr);
    EXPECT_EQ(kvs.get(key2), nullptr);

    kvs.put(key1, v);

    EXPECT_TRUE(v.equals(kvs.get(key1)));
    EXPECT_EQ(kvs.get(other_node), nullptr);
    EXPECT_EQ(kvs.get(key2), nullptr);

    kvs.put(key1, v2);

    EXPECT_TRUE(v2.equals(kvs.get(key1)));
    EXPECT_EQ(kvs.get(other_node), nullptr);
    EXPECT_EQ(kvs.get(key2), nullptr);

    kvs.put(key2, v);

    EXPECT_TRUE(v2.equals(kvs.get(key1)));
    EXPECT_EQ(kvs.get(other_node), nullptr);
    EXPECT_TRUE(v.equals(kvs.get(key2)));
}

TEST(testKVStore, testKVStorePutGet) {
    test_kvstore_put_get();
}


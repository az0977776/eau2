#include <gtest/gtest.h>

#include "../../src/kvstore/keyvaluestore.h"
#include "../../src/kvstore/keyvalue.h"

#include "test_macros.h"

void test_kvstore_put_get() {
    KVStore kvs(false);
    Key key1(0, "A Test");
    Key other_node(0, "other node key");
    Key key2(0, "Second key");

    char v_buf[13];
    memcpy(v_buf, "A VALUE TEST", 13);

    char v2_buf[16];
    memcpy(v_buf, "different value", 16);

    Value v(15, v_buf);
    Value v2(20, v2_buf);

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


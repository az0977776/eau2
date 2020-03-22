#include <gtest/gtest.h>

#include "../src/kvstore/keyvalue.h"

#include "test_macros.h"

// *************************** Key Tests ***********************************

void test_key_equals() {
    Key key(0, "one");
    Key other_node(123, "one");
    Key different_name(0, "different name");
    Key different_all(12345, "different all");
    Key same(0, "one");

    EXPECT_TRUE(key.equals(&key));
    EXPECT_TRUE(key.equals(&same));

    EXPECT_FALSE(key.equals(&other_node));
    EXPECT_FALSE(key.equals(&different_name));
    EXPECT_FALSE(key.equals(&different_all));
}

TEST(testKey, testKeyEquals) {
    test_key_equals();
}

void test_key_clone() {
    Key key(0, "one");
    Key* copy = key.clone();

    EXPECT_TRUE(key.equals(copy));

    delete copy;
}

TEST(testKey, testKeyClone) {
    test_key_clone();
}

void test_key_hash() {
    Key key(0, "one");
    Key other_node(123, "one");
    Key different_name(0, "different name");
    Key different_all(12345, "different all");
    Key same(0, "one");

    // equal key then equal hash
    EXPECT_TRUE(key.equals(&key));
    EXPECT_EQ(key.hash(), key.hash());

    EXPECT_TRUE(key.equals(&same));
    EXPECT_EQ(key.hash(), same.hash());

    // not equal , hash not equals
    EXPECT_FALSE(key.equals(&other_node));
    EXPECT_NE(key.hash(), other_node.hash());

    EXPECT_FALSE(key.equals(&different_name));
    EXPECT_NE(key.hash(), different_name.hash());

    EXPECT_FALSE(key.equals(&different_all));
    EXPECT_NE(key.hash(), different_all.hash());
}

TEST(testKey, testKeyHash) {
    test_key_hash();
}

void test_key_get_name() {
    String str("a string");
    Key key(0, str.c_str());

    EXPECT_TRUE(str.equals(key.get_name()));
}

TEST(testKey, testKeyGetName) {
    test_key_get_name();
}

void test_key_serialize() {
    Key key(0, "This is a key");

    char* buf = key.serialize();
    Key* other = Key::deserialize(buf);

    EXPECT_TRUE(key.equals(other));

    delete buf;
    delete other;
}

TEST(testKey, testKeySerialize) {
    test_key_serialize();
}

// This checks that the serial buf size is correct by checking that serialize does not write to it
void test_key_serial_buf_size() {
    char init = 'A';
    Key key(0, "This is a key");

    size_t buf_len = key.serial_buf_size();

    char* buf = new char[2 * buf_len];
    for (size_t i = 0; i < 2 * buf_len; i++) {
        buf[i] = init;  // set to a non zero value ...
    }

    key.serialize(buf); // serialize into the buffer
    Key* other = Key::deserialize(buf);

    EXPECT_TRUE(key.equals(other));  // check that it is equal

    // all char after the buf_len should be unchanged ... 
    for (size_t i = buf_len; i < 2 * buf_len; i++) {
        EXPECT_EQ(buf[i], init);
    }

    delete buf;
    delete other;
}

TEST(testKey, testKeySerialBufSize) {
    test_key_serial_buf_size();
}

// *************************** Value Tests ***********************************




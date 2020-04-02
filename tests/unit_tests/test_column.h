#include <gtest/gtest.h>

#include "../../src/util/string.h"  
#include "../../src/dataframe/column.h"
#include "../../src/kvstore/keyvaluestore.h"

#include "test_macros.h"

/**************************** Test Columns ***************************/
/******* Test IntColumn **********/

/**
 * Int column can push int on, and get them back. Also checks size and set.
 */
void test_int_column() {
    KVStore kvs(false);
    String s("column name here");
    IntColumn *ic = new IntColumn(&s, &kvs);
    ASSERT_EQ(ic->size(), 0);

    size_t num_elements = 50000;

    for (int i = 0 ; i < num_elements; i++) {
      ic->push_back(i);
    }

    ASSERT_EQ(ic->size(), num_elements);
    ASSERT_EQ(ic->get(4999), 4999);
    ASSERT_EQ(ic->get(num_elements - 1), num_elements - 1);

    delete ic;
}

TEST(testColumn, testIntCol) {
    test_int_column();
}

/**
 * Checks that a constructor with multiple inputs is possible.
 */
void test_int_constructor() {
    KVStore kvs(false);
    String s("column name here");
    IntColumn ic(&s, &kvs, 5, -1024, 0, 1042, 33, -1);

    ASSERT_EQ(ic.size(), 5);
    ASSERT_EQ(ic.get(0), -1024);
    ASSERT_EQ(ic.get(1), 0);

    // still add more data
    for (int i = 0 ; i < 5000; i++) {
      ic.push_back(i);
    }

    ASSERT_EQ(ic.size(), 5005);
    ASSERT_EQ(ic.get(0), -1024);
    ASSERT_EQ(ic.get(1), 0);
    ASSERT_EQ(ic.get(4999), 4994);
}

TEST(testColumn, testIntColumnConstructor) {
    test_int_constructor();
}

/**
 * The correct type is returned, 'I'
 */
void test_int_get_type() {
    KVStore kvs(false);
    String s("column name here");
    IntColumn ic(&s, &kvs, 5, -1024, 0, 1042, 33, -1);

    EXPECT_EQ(ic.get_type(), 'I');
}

TEST(testColumn, testIntColumnGetType) {
    test_int_get_type();
}
/**
 * The reset of IntColumn's tests are about unspecified behavior. 
 * This is specific to our implemention. 
 */
void test_int_column_as_bool() {
    KVStore kvs(false);
    String s("column name here");
    IntColumn *ic = new IntColumn(&s, &kvs);
    ic->as_bool();
    delete ic;
    exit(0);
}

TEST(testColumn, testIntColExitOnFailAsBool) { 
  CS4500_ASSERT_EXIT_255(test_int_column_as_bool);
}

void test_int_column_as_double() {
    KVStore kvs(false);
    String s("column name here");
    IntColumn *ic = new IntColumn(&s, &kvs);
    ic->as_double();
    delete ic;
    exit(0);
}

TEST(testColumn, testIntColExitOnFailAsDouble) { 
  CS4500_ASSERT_EXIT_255(test_int_column_as_double);
}

void test_int_column_as_string() {
    KVStore kvs(false);
    String s("column name here");
    IntColumn *ic = new IntColumn(&s, &kvs);
    ic->as_string();
    delete ic;
    exit(0);
}

TEST(testColumn, testIntColExitOnFailAsString) { 
  CS4500_ASSERT_EXIT_255(test_int_column_as_string);
}

/******* Test DoubleColumn **********/

/**
 * Doublecolumn can push double on, and get them back. Also checks size and set.
 */
void test_double_column() {
    KVStore kvs(false);
    String s("column name here");
    DoubleColumn *dc = new DoubleColumn(&s, &kvs);
    ASSERT_EQ(dc->size(), 0);

    for (int i = 0 ; i < 5000; i++) {
      dc->push_back(i / 8.0);
    }

    ASSERT_EQ(dc->size(), 5000);
    ASSERT_FLOAT_EQ(dc->get(4999), 4999 / 8.0);

    delete dc;
}

TEST(testColumn, testDoubleCol) {
    test_double_column();
}

/**
 * DoubleColumn returns the correct type.
 */
void test_double_column_type() {
    KVStore kvs(false);
    String s("column name here");
    String s2("column name 2");
    DoubleColumn *dc = new DoubleColumn(&s, &kvs);
    Column *c = new DoubleColumn(&s2, &kvs);

    EXPECT_EQ(dc->get_type(), 'D');
    EXPECT_EQ(c->get_type(), 'D');
    
    delete c;
    delete dc;
}

TEST(testColumn, testDoubleColType) {
    test_double_column_type();
}

/**
 * DoubleColumn can be created with a variable number of arguments
 */
void test_double_column_var_args() {
    KVStore kvs(false);
    String s("column name here");
    DoubleColumn *dc = new DoubleColumn(&s, &kvs, 6, -1234.5, 1.2, 0.0, 1.0, 0.00012345, 0.4553);

    EXPECT_EQ(dc->get_type(), 'D');
    EXPECT_EQ(dc->size(), 6);

    EXPECT_FLOAT_EQ(dc->get(0), -1234.5);
    EXPECT_FLOAT_EQ(dc->get(1), 1.2);
    EXPECT_FLOAT_EQ(dc->get(2), 0.0);
    EXPECT_FLOAT_EQ(dc->get(3), 1.0);
    EXPECT_FLOAT_EQ(dc->get(4), 0.00012345);
    EXPECT_FLOAT_EQ(dc->get(5), 0.4553);

    dc->push_back(-123.456);
    EXPECT_FLOAT_EQ(dc->get(6), -123.456);

    delete dc;
}

TEST(testColumn, testDoubleColVarArgsConstructor) {
    test_double_column_var_args();
}


/******* Test StringColumn **********/
/**
 * StringColumn can push double on, and get them back. Also checks size and set.
 */
void test_string_column() {
    KVStore kvs(false);
    String s("column name here");
    StringColumn *ic = new StringColumn(&s, &kvs);
    ASSERT_EQ(ic->size(), 0);

    String *a = new String("123");
    String *b = new String("foo");

    for (int i = 0 ; i < 5000; i++) {
      ic->push_back(a);
    }

    ASSERT_EQ(ic->size(), 5000);

    ASSERT_TRUE(ic->get(4999)->equals(a));

    // delete ic;
    delete a;
    delete b;
}

TEST(testColumn, testStringCol) {
    test_string_column();
}

/**
 * StringColumn returns the correct type.
 */
void test_string_column_type() {
    KVStore kvs(false);
    String s("column name here");
    String s2("foobar");
    StringColumn *sc = new StringColumn(&s, &kvs);
    Column *c = new StringColumn(&s2, &kvs);

    EXPECT_EQ(sc->get_type(), 'S');
    EXPECT_EQ(c->get_type(), 'S');
    
    delete c;
    delete sc;
}

TEST(testColumn, testStringColType) {
    test_string_column_type();
}

/**
 * StringColumn can be created with a variable number of arguments
 */
void test_string_column_var_args() {
    KVStore kvs(false);
    String col_name("column name here");
    String s1("1");
    String s2("2");
    String s3("3");
    String s4("4");
    StringColumn *sc = new StringColumn(&col_name, &kvs, 3, &s1, &s2, &s3);

    EXPECT_EQ(sc->get_type(), 'S');
    EXPECT_EQ(sc->size(), 3);

    EXPECT_TRUE(sc->get(0)->equals(&s1));
    EXPECT_TRUE(sc->get(1)->equals(&s2));
    EXPECT_TRUE(sc->get(2)->equals(&s3));

    sc->push_back(&s4);
    EXPECT_EQ(sc->size(), 4);
    EXPECT_TRUE(sc->get(3)->equals(&s4));

    delete sc;
}

TEST(testColumn, testStringColVarArgsConstructor) {
    test_string_column_var_args();
}

/******* Test BoolColumn **********/

/**
 * BoolColumn can push double on, and get them back. Also checks size and set.
 */
void test_bool_column() {
    KVStore kvs(false);
    String s("column name here");
    BoolColumn *ic = new BoolColumn(&s, &kvs);
    ASSERT_EQ(ic->size(), 0);

    for (int i = 0 ; i < 5000; i++) {
      ic->push_back(i % 2);
    }

    // our implementation has re allocates at 64, 1024 booleans
    // make sure to check these values
    ASSERT_EQ(ic->size(), 5000);
    EXPECT_TRUE(ic->get(4999));
    EXPECT_TRUE(ic->get(1));
    EXPECT_FALSE(ic->get(32));
    EXPECT_FALSE(ic->get(1024));
    EXPECT_FALSE(ic->get(64));
    EXPECT_FALSE(ic->get(1088));
    EXPECT_FALSE(ic->get(0));

    delete ic;
}

TEST(testColumn, testBoolCol) {
    test_bool_column();
}

/**
 * BoolColumn returns the correct type.
 */
void test_bool_column_type() {
    KVStore kvs(false);
    String s("column name here");
    String s2("column 2 name");
    BoolColumn *bc = new BoolColumn(&s, &kvs);
    Column *c = new BoolColumn(&s2, &kvs);

    EXPECT_EQ(bc->get_type(), BOOL);
    EXPECT_EQ(c->get_type(), BOOL);
    
    delete c;
    delete bc;
}

TEST(testColumn, testBoolColType) {
    test_bool_column_type();
}

/**
 * BoolColumn can be created with a variable number of arguments
 */
void test_bool_column_var_args() {
    KVStore kvs(false);
    String s("column name here");
    BoolColumn *bc = new BoolColumn(&s, &kvs, 6, true, true, true, false, true, false);

    EXPECT_EQ(bc->get_type(), BOOL);
    EXPECT_EQ(bc->size(), 6);
    EXPECT_TRUE(bc->get(0));
    EXPECT_TRUE(bc->get(1));
    EXPECT_TRUE(bc->get(2));
    EXPECT_FALSE(bc->get(3));
    EXPECT_TRUE(bc->get(4));
    EXPECT_FALSE(bc->get(5));

    bc->push_back(false);
    EXPECT_FALSE(bc->get(6));

    
    delete bc;
}

TEST(testColumn, testBoolColVarArgsConstructor) {
    test_bool_column_var_args();
}

/**
 * The rest of these tests are about our specific implmentation of the API's
 * unspecified behavior.
 */
void test_bool_column_as_int() {
    KVStore kvs(false);
    String s("column name here");
    BoolColumn *bc = new BoolColumn(&s, &kvs);
    bc->as_int();
    delete bc;
    exit(0);
}

TEST(testColumn, testBoolColExitOnFailAsBool) { 
  CS4500_ASSERT_EXIT_255(test_bool_column_as_int);
}

void test_bool_column_as_double() {
    KVStore kvs(false);
    String s("column name here");
    BoolColumn *bc = new BoolColumn(&s, &kvs);
    bc->as_double();
    delete bc;
    exit(0);
}

TEST(testColumn, testBoolColExitOnFailAsDouble) { 
  CS4500_ASSERT_EXIT_255(test_bool_column_as_double);
}

void test_bool_column_as_string() {
    KVStore kvs(false);
    String s("column name here");
    BoolColumn *bc = new BoolColumn(&s, &kvs);
    bc->as_string();
    delete bc;
    exit(0);
}

TEST(testColumn, testBoolColExitOnFailAsString) { 
  CS4500_ASSERT_EXIT_255(test_bool_column_as_string);
}

void test_bool_column_serialize() {
    KVStore kvs(false); 
    String s("foobar");
    BoolColumn *bc = new BoolColumn(&s, &kvs);

    for (int i = 0 ; i < 5000; i++) {
      bc->push_back(i % 2);
    }

    char* serialized_col = bc->serialize();     

    BoolColumn* bc2 = Column::deserialize(serialized_col, &kvs)->as_bool();

    EXPECT_EQ(bc2->size(), bc->size());
    EXPECT_TRUE(s.equals(bc2->key_buff_->get_base_str()));
    EXPECT_EQ(bc2->get(100), bc->get(100));
    EXPECT_EQ(bc2->get(101), bc->get(101));
    EXPECT_EQ(bc2->get(4999), bc->get(4999));

    delete bc;
    delete serialized_col;
    delete bc2;
}

TEST(testColumn, testBoolColumnSerialize) { 
  test_bool_column_serialize();
}

// This checks that the serial buf size is correct by checking that serialize does not write to it
void test_bool_column_serial_buf_size() {
    char init = 'A';
    KVStore kvs(false); 
    String s("foobar");
    BoolColumn bc(&s, &kvs);

    for (int i = 0 ; i < 5000; i++) {
      bc.push_back(i % 2);
    }

    size_t buf_len = bc.serial_buf_size();

    char* buf = new char[2 * buf_len];
    for (size_t i = 0; i < 2 * buf_len; i++) {
        buf[i] = init;  // set to a non zero value ...
    }

    bc.serialize(buf); // serialize into the buffer

    // check that it can still deserialize correctly
    BoolColumn* bc2 = Column::deserialize(buf, &kvs)->as_bool();

    EXPECT_EQ(bc2->size(), bc.size());
    EXPECT_TRUE(s.equals(bc2->key_buff_->get_base_str()));
    EXPECT_EQ(bc2->get(100), bc.get(100));
    EXPECT_EQ(bc2->get(101), bc.get(101));
    EXPECT_EQ(bc2->get(4999), bc.get(4999));

    // all char after the buf_len should be unchanged ... 
    for (size_t i = buf_len; i < 2 * buf_len; i++) {
        EXPECT_EQ(buf[i], init);
    }

    delete buf;
    delete bc2;
}

TEST(testColumn, testBoolColumnSerialBufSize) {
    test_bool_column_serial_buf_size();
}

void test_int_column_serialize() {
    KVStore kvs(false); 
    String s("foobar");
    IntColumn *bc = new IntColumn(&s, &kvs);

    for (int i = 0 ; i < 5000; i++) {
      bc->push_back(i);
    }

    char* serialized_col = bc->serialize();     

    IntColumn* bc2 = Column::deserialize(serialized_col, &kvs)->as_int();

    EXPECT_EQ(bc2->size(), bc->size());
    EXPECT_TRUE(s.equals(bc2->key_buff_->get_base_str()));
    EXPECT_EQ(bc2->get(4999), bc->get(4999));
    EXPECT_EQ(bc2->get(3000), bc->get(3000));

    delete bc;
    delete serialized_col;
    delete bc2;
}

TEST(testColumn, testIntColumnSerialize) { 
  test_int_column_serialize();
}

// This checks that the serial buf size is correct by checking that serialize does not write to it
void test_int_column_serial_buf_size() {
    char init = 'A';
    KVStore kvs(false); 
    String s("foobar");
    IntColumn ic(&s, &kvs);

    for (int i = 0 ; i < 5000; i++) {
      ic.push_back(i);
    }

    size_t buf_len = ic.serial_buf_size();

    char* buf = new char[2 * buf_len];
    for (size_t i = 0; i < 2 * buf_len; i++) {
        buf[i] = init;  // set to a non zero value ...
    }

    ic.serialize(buf); // serialize into the buffer

    // check that it can still deserialize correctly
    IntColumn* ic2 = Column::deserialize(buf, &kvs)->as_int();

    EXPECT_EQ(ic2->size(), ic.size());
    EXPECT_TRUE(s.equals(ic2->key_buff_->get_base_str()));
    EXPECT_EQ(ic2->get(4999), ic.get(4999));
    EXPECT_EQ(ic2->get(3000), ic.get(3000));

    // all char after the buf_len should be unchanged ... 
    for (size_t i = buf_len; i < 2 * buf_len; i++) {
        EXPECT_EQ(buf[i], init);
    }

    delete buf;
    delete ic2;
}

TEST(testColumn, testIntColumnSerialBufSize) {
    test_int_column_serial_buf_size();
}

void test_double_column_serialize() {
    KVStore kvs(false); 
    String s("foobar");
    DoubleColumn *bc = new DoubleColumn(&s, &kvs);

    for (int i = 0 ; i < 5000; i++) {
      bc->push_back(i / 8.0);
    }

    char* serialized_col = bc->serialize();     

    DoubleColumn* bc2 = Column::deserialize(serialized_col, &kvs)->as_double();

    EXPECT_EQ(bc2->size(), bc->size());
    EXPECT_TRUE(s.equals(bc2->key_buff_->get_base_str()));
    EXPECT_EQ(bc2->get(4999), bc->get(4999));
    EXPECT_EQ(bc2->get(3000), bc->get(3000));

    delete bc;
    delete serialized_col;
    delete bc2;
}

TEST(testColumn, testDoubleColumnSerialize) { 
  test_double_column_serialize();
}

// This checks that the serial buf size is correct by checking that serialize does not write to it
void test_double_column_serial_buf_size() {
    char init = 'A';
    KVStore kvs(false); 
    String s("foobar");
    DoubleColumn dc(&s, &kvs);

    for (int i = 0 ; i < 5000; i++) {
      dc.push_back(i / 8.0);
    }

    size_t buf_len = dc.serial_buf_size();

    char* buf = new char[2 * buf_len];
    for (size_t i = 0; i < 2 * buf_len; i++) {
        buf[i] = init;  // set to a non zero value ...
    }

    dc.serialize(buf); // serialize into the buffer

    // check that it can still deserialize correctly
    DoubleColumn* dc2 = Column::deserialize(buf, &kvs)->as_double();

    EXPECT_EQ(dc2->size(), dc.size());
    EXPECT_TRUE(s.equals(dc2->key_buff_->get_base_str()));
    EXPECT_EQ(dc2->get(4999), dc.get(4999));
    EXPECT_EQ(dc2->get(3000), dc.get(3000));

    // all char after the buf_len should be unchanged ... 
    for (size_t i = buf_len; i < 2 * buf_len; i++) {
        EXPECT_EQ(buf[i], init);
    }

    delete buf;
    delete dc2;
}

TEST(testColumn, testDoubleColumnSerialBufSize) {
    test_double_column_serial_buf_size();
}


void test_string_column_serialize() {
    KVStore kvs(false); 
    String s("foobar");
    String s0("abc");
    String s1("foo");
    StringColumn *bc = new StringColumn(&s, &kvs);

    for (int i = 0 ; i < 5000; i++) {
        if (i % 2 == 0) {
            bc->push_back(&s0);
        } else {
            bc->push_back(&s1);
        }
    }

    char* serialized_col = bc->serialize();     

    StringColumn* bc2 = Column::deserialize(serialized_col, &kvs)->as_string();

    EXPECT_EQ(bc2->size(), bc->size());
    EXPECT_TRUE(s.equals(bc2->key_buff_->get_base_str()));
    EXPECT_TRUE(bc2->get(4999)->equals(bc->get(4999)));
    EXPECT_TRUE(bc2->get(3000)->equals(bc->get(3000)));

    delete bc;
    delete serialized_col;
    delete bc2;
}

TEST(testColumn, testStringColumnSerialize) { 
  test_string_column_serialize();
}

// This checks that the serial buf size is correct by checking that serialize does not write to it
void test_string_column_serial_buf_size() {
    char init = 'A';
    KVStore kvs(false); 
    String s("foobar");
    String s0("abc");
    String s1("foo");
    StringColumn sc(&s, &kvs);

    for (int i = 0 ; i < 5000; i++) {
        if (i % 2 == 0) {
            sc.push_back(&s0);
        } else {
            sc.push_back(&s1);
        }
    }

    size_t buf_len = sc.serial_buf_size();

    char* buf = new char[2 * buf_len];
    for (size_t i = 0; i < 2 * buf_len; i++) {
        buf[i] = init;  // set to a non zero value ...
    }

    sc.serialize(buf); // serialize into the buffer

    // check that it can still deserialize correctly
    StringColumn* sc2 = Column::deserialize(buf, &kvs)->as_string();

    EXPECT_EQ(sc2->size(), sc.size());
    EXPECT_TRUE(s.equals(sc2->key_buff_->get_base_str()));
    EXPECT_TRUE(sc2->get(4999)->equals(sc.get(4999)));
    EXPECT_TRUE(sc2->get(3000)->equals(sc.get(3000)));

    // all char after the buf_len should be unchanged ... 
    for (size_t i = buf_len; i < 2 * buf_len; i++) {
        EXPECT_EQ(buf[i], init);
    }

    delete buf;
    delete sc2;
}

TEST(testColumn, testStringColumnSerialBufSize) {
    test_string_column_serial_buf_size();
}



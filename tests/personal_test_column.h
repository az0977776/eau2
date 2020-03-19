#pragma once

#include <gtest/gtest.h>

#include "../src/string.h"  
#include "../src/column.h"

#include "personal_test_macros.h"


/**************************** Test Columns ***************************/
/******* Test IntColumn **********/

/**
 * Int column can push int on, and get them back. Also checks size and set.
 */
void test_int_column() {
    IntColumn *ic = new IntColumn();
    ASSERT_EQ(ic->size(), 0);

    size_t num_elements = 50000;

    for (int i = 0 ; i < num_elements; i++) {
      ic->push_back(i);
    }

    ASSERT_EQ(ic->size(), num_elements);
    ASSERT_EQ(ic->get(4999), 4999);
    ASSERT_EQ(ic->get(num_elements - 1), num_elements - 1);

    ic->set(1555, 718237);
    ASSERT_EQ(ic->get(1555), 718237);

    delete ic;
}

TEST(testColumn, testIntCol) {
    test_int_column();
}

/**
 * Checks that a constructor with multiple inputs is possible.
 */
void test_int_constructor() {
    IntColumn ic(5, -1024, 0, 1042, 33, -1);

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
    IntColumn ic(5, -1024, 0, 1042, 33, -1);

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
    IntColumn *ic = new IntColumn();
    ic->as_bool();
    delete ic;
    exit(0);
}

TEST(testColumn, testIntColExitOnFailAsBool) { 
  CS4500_ASSERT_EXIT_255(test_int_column_as_bool);
}

void test_int_column_as_double() {
    IntColumn *ic = new IntColumn();
    ic->as_double();
    delete ic;
    exit(0);
}

TEST(testColumn, testIntColExitOnFailAsDouble) { 
  CS4500_ASSERT_EXIT_255(test_int_column_as_double);
}

void test_int_column_as_string() {
    IntColumn *ic = new IntColumn();
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
    DoubleColumn *dc = new DoubleColumn();
    ASSERT_EQ(dc->size(), 0);

    for (int i = 0 ; i < 5000; i++) {
      dc->push_back(i / 8.0);
    }

    ASSERT_EQ(dc->size(), 5000);
    ASSERT_FLOAT_EQ(dc->get(4999), 4999 / 8.0);

    dc->set(1555, 718237);
    ASSERT_FLOAT_EQ(dc->get(1555), 718237);

    delete dc;
}

TEST(testColumn, testDoubleCol) {
    test_double_column();
}

/**
 * DoubleColumn returns the correct type.
 */
void test_double_column_type() {
    DoubleColumn *dc = new DoubleColumn();
    Column *c = new DoubleColumn();

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
    DoubleColumn *dc = new DoubleColumn(6, -1234.5, 1.2, 0.0, 1.0, 0.00012345, 0.4553);

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
    StringColumn *ic = new StringColumn();
    ASSERT_EQ(ic->size(), 0);

    String *a = new String("123");
    String *b = new String("foo");

    for (int i = 0 ; i < 5000; i++) {
      ic->push_back(a);
    }

    ASSERT_EQ(ic->size(), 5000);
    ASSERT_TRUE(ic->get(4999)->equals(a));

    ic->set(1555, b);
    ASSERT_TRUE(ic->get(1555)->equals(b));

    delete ic;
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
    StringColumn *sc = new StringColumn();
    Column *c = new StringColumn();

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
    String s1("1");
    String s2("2");
    String s3("3");
    String s4("4");
    StringColumn *sc = new StringColumn(4, &s1, &s2, &s3, nullptr);

    EXPECT_EQ(sc->get_type(), 'S');
    EXPECT_EQ(sc->size(), 4);

    EXPECT_TRUE(sc->get(0)->equals(&s1));
    EXPECT_TRUE(sc->get(1)->equals(&s2));
    EXPECT_TRUE(sc->get(2)->equals(&s3));
    EXPECT_EQ(sc->get(3), nullptr);

    sc->push_back(&s4);
    EXPECT_EQ(sc->size(), 5);
    EXPECT_TRUE(sc->get(4)->equals(&s4));

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
    BoolColumn *ic = new BoolColumn();
    ASSERT_EQ(ic->size(), 0);

    for (int i = 0 ; i < 5000; i++) {
      ic->push_back(i % 2);
    }

    // our implementation has re allocates at 64, 1024 booleans
    // make sure to check these values
    ASSERT_EQ(ic->size(), 5000);
    EXPECT_TRUE(ic->get(4999));
    EXPECT_TRUE(ic->get(1));
    EXPECT_FALSE(ic->get(1024));
    EXPECT_FALSE(ic->get(64));
    EXPECT_FALSE(ic->get(1088));
    EXPECT_FALSE(ic->get(0));

    // check setting all permutations of booleans
    ic->set(1555, 0);
    EXPECT_FALSE(ic->get(1555));

    ic->set(1556, 0);
    EXPECT_FALSE(ic->get(1556));

    ic->set(1024, 1);
    EXPECT_TRUE(ic->get(1024));

    ic->set(1025, 1);
    EXPECT_TRUE(ic->get(1025));

    delete ic;
}

TEST(testColumn, testBoolCol) {
    test_bool_column();
}

/**
 * BoolColumn returns the correct type.
 */
void test_bool_column_type() {
    BoolColumn *bc = new BoolColumn();
    Column *c = new BoolColumn();

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
    BoolColumn *bc = new BoolColumn(6, true, true, true, false, true, false);

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
    BoolColumn *bc = new BoolColumn();
    bc->as_int();
    delete bc;
    exit(0);
}

TEST(testColumn, testBoolColExitOnFailAsBool) { 
  CS4500_ASSERT_EXIT_255(test_bool_column_as_int);
}

void test_bool_column_as_double() {
    BoolColumn *bc = new BoolColumn();
    bc->as_double();
    delete bc;
    exit(0);
}

TEST(testColumn, testBoolColExitOnFailAsDouble) { 
  CS4500_ASSERT_EXIT_255(test_bool_column_as_double);
}

void test_bool_column_as_string() {
    BoolColumn *bc = new BoolColumn();
    bc->as_string();
    delete bc;
    exit(0);
}

TEST(testColumn, testBoolColExitOnFailAsString) { 
  CS4500_ASSERT_EXIT_255(test_bool_column_as_string);
}
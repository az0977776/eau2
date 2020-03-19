#pragma once

#include <gtest/gtest.h>

#include "../src/string.h"  
#include "../src/schema.h"

#include "personal_test_macros.h"


/**************************** Test Schema ***************************/

/**
 * Is the schema created with the corrent column types from string.
 */
void test_schema_char_constructor() {
    Schema* s = new Schema("IIBSDD");

    ASSERT_EQ(s->width(), 6);
    ASSERT_EQ(s->length(), 0);

    EXPECT_EQ(s->col_type(0), 'I');
    EXPECT_EQ(s->col_type(1), 'I');
    EXPECT_EQ(s->col_type(2), 'B');
    EXPECT_EQ(s->col_type(3), 'S');
    EXPECT_EQ(s->col_type(4), 'D');
    EXPECT_EQ(s->col_type(5), 'D');

    delete s;
}

TEST(testSchema, testSchmaCharConstructor) {
    test_schema_char_constructor();
}

/**
 * Can an empty Schema be created.
 */
void test_schema_default_constructor() {
    Schema* s = new Schema();

    ASSERT_EQ(s->width(), 0);
    ASSERT_EQ(s->length(), 0);

    delete s;
}

TEST(testSchema, testSchmaDefalutConstructor) {
    test_schema_default_constructor();
}

/**
 * Does the schema deep copy the givnv constructor. All row names and column names
 * are copied. Types are also copied.
 */
void test_schema_copy_constructor() {
    Schema s;

    s.add_column('I');
    s.add_column('D');
    s.add_column('S');

    s.add_row();
    s.add_row();
    s.add_row();

    Schema copy(s);

    ASSERT_EQ(s.width(), copy.width());
    ASSERT_EQ(s.length(), 3);
    ASSERT_EQ(copy.length(), 0);

    // check types
    EXPECT_EQ(s.col_type(0), copy.col_type(0));
    EXPECT_EQ(s.col_type(1), copy.col_type(1));
    EXPECT_EQ(s.col_type(2), copy.col_type(2));
}

TEST(testSchema, testSchmaCopyConstructor) {
    test_schema_copy_constructor();
}

/**
 * Can a row be added with or without a name.
 */
void test_schema_add_row() {
    Schema s;

    ASSERT_EQ(s.length(), 0);

    s.add_row();
    s.add_row();
    s.add_row();

    ASSERT_EQ(s.width(), 0);
    ASSERT_EQ(s.length(), 3);
}

TEST(testSchema, testSchmaAddRow) {
    test_schema_add_row();
}

/**
 * Can a column be added with correct type. Also checks 
 * that column names can be nullptrs.
 */
void test_schema_add_col() {
    Schema s;

    ASSERT_EQ(s.length(), 0);
    ASSERT_EQ(s.width(), 0);

    s.add_column('I');
    s.add_column('D');
    s.add_column('S');

    ASSERT_EQ(s.width(), 3);
    ASSERT_EQ(s.length(), 0);
}

TEST(testSchema, testSchmaAddCol) {
    test_schema_add_col();
}

/**
 * A schema should be equal if all types, names are the same and in the same 
 * order.
 */
void test_schema_equals() {
    Schema s;
    Schema not_s("ISS");
    Schema same_s;

    s.add_row();
    s.add_row();
    s.add_row();

    s.add_column('I');
    s.add_column('D');
    s.add_column('S');

    same_s.add_row();
    same_s.add_row();
    same_s.add_row();

    same_s.add_column('I');
    same_s.add_column('D');
    same_s.add_column('S');

    ASSERT_EQ(s.width(), 3);
    ASSERT_EQ(s.length(), 3);

    ASSERT_EQ(same_s.width(), 3);
    ASSERT_EQ(same_s.length(), 3);

    EXPECT_TRUE(s.equals(&s));
    EXPECT_FALSE(s.equals(&not_s));
    EXPECT_TRUE(s.equals(&same_s));
}

TEST(testSchema, testSchmaEquals) {
    test_schema_equals();
}


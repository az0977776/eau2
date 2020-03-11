#pragma once

#include <gtest/gtest.h>

#include "string.h"  
#include "schema.h"

#include "personal_test_macros.h"


/**************************** Test Schema ***************************/

/**
 * Is the schema created with the corrent column types from string.
 */
void test_schema_char_constructor() {
    Schema* s = new Schema("IIBSFF");

    ASSERT_EQ(s->width(), 6);
    ASSERT_EQ(s->length(), 0);

    EXPECT_EQ(s->col_type(0), 'I');
    EXPECT_EQ(s->col_type(1), 'I');
    EXPECT_EQ(s->col_type(2), 'B');
    EXPECT_EQ(s->col_type(3), 'S');
    EXPECT_EQ(s->col_type(4), 'F');
    EXPECT_EQ(s->col_type(5), 'F');

    EXPECT_EQ(s->col_name(0), nullptr);
    EXPECT_EQ(s->col_name(1), nullptr);
    EXPECT_EQ(s->col_name(2), nullptr);
    EXPECT_EQ(s->col_name(3), nullptr);
    EXPECT_EQ(s->col_name(4), nullptr);
    EXPECT_EQ(s->col_name(5), nullptr);

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
    String row1("row 1 name");
    String row3("row 3 name");

    String col1("col 1 name");
    String col3("col 3 name");

    s.add_column('I', &col1);
    s.add_column('F', nullptr);
    s.add_column('S', &col3);

    s.add_row(&row1);
    s.add_row(nullptr);
    s.add_row(&row3);

    Schema copy(s);

    ASSERT_EQ(s.width(), copy.width());
    ASSERT_EQ(s.length(), copy.length());

    // check types
    EXPECT_EQ(s.col_type(0), copy.col_type(0));
    EXPECT_EQ(s.col_type(1), copy.col_type(1));
    EXPECT_EQ(s.col_type(2), copy.col_type(2));

    // check column names
    EXPECT_TRUE(s.col_name(0)->equals(copy.col_name(0)));
    EXPECT_EQ(s.col_name(1), copy.col_name(1));
    EXPECT_TRUE(s.col_name(2)->equals(copy.col_name(2)));

    // check row names
    EXPECT_TRUE(s.row_name(0)->equals(copy.row_name(0)));
    EXPECT_EQ(s.row_name(1), copy.row_name(1));
    EXPECT_TRUE(s.row_name(2)->equals(copy.row_name(2)));
}

TEST(testSchema, testSchmaCopyConstructor) {
    test_schema_copy_constructor();
}

/**
 * Can a row be added with or without a name.
 */
void test_schema_add_row() {
    Schema s;
    String row1("row 1 name");
    String row3("row 3 name");

    ASSERT_EQ(s.length(), 0);

    s.add_row(&row1);
    s.add_row(nullptr);
    s.add_row(&row3);

    ASSERT_EQ(s.width(), 0);
    ASSERT_EQ(s.length(), 3);

    EXPECT_TRUE(s.row_name(0)->equals(&row1));
    EXPECT_EQ(s.row_name(1), nullptr);
    EXPECT_TRUE(s.row_name(2)->equals(&row3));
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
    String col1("col 1 name");
    String col3("col 3 name");

    ASSERT_EQ(s.length(), 0);
    ASSERT_EQ(s.width(), 0);

    s.add_column('I', &col1);
    s.add_column('F', nullptr);
    s.add_column('S', &col3);

    ASSERT_EQ(s.width(), 3);
    ASSERT_EQ(s.length(), 0);

    EXPECT_TRUE(s.col_name(0)->equals(&col1));
    EXPECT_EQ(s.col_name(1), nullptr);
    EXPECT_TRUE(s.col_name(2)->equals(&col3));
}

TEST(testSchema, testSchmaAddCol) {
    test_schema_add_col();
}

/**
 * Can a column index be retrieved by the column name. If the name does 
 * not exist then it returns -1. If no name is passed in -1 is returned.
 */
void test_schema_col_idx() {
    Schema s;
    String col1("col 1 name");
    String col3("col 3 name");

    ASSERT_EQ(s.length(), 0);
    ASSERT_EQ(s.width(), 0);

    s.add_column('I', &col1);
    s.add_column('F', nullptr);
    s.add_column('S', &col3);

    ASSERT_EQ(s.width(), 3);
    ASSERT_EQ(s.length(), 0);

    EXPECT_EQ(s.col_idx(col1.c_str()), 0);
    EXPECT_EQ(s.col_idx(col3.c_str()), 2);
    EXPECT_EQ(s.col_idx("does not exist"), -1);
    EXPECT_EQ(s.col_idx(nullptr), -1);
}

TEST(testSchema, testSchmaColIdx) {
    test_schema_col_idx();
}

/**
 * Can a row index be retrieved by the column name. If the name does 
 * not exist then it returns -1. If no name is passed in -1 is returned.
 */
void test_schema_row_idx() {
    Schema s;
    String row1("row 1 name");
    String row3("row 3 name");

    ASSERT_EQ(s.length(), 0);
    ASSERT_EQ(s.width(), 0);

    s.add_row(&row1);
    s.add_row(nullptr);
    s.add_row(&row3);

    ASSERT_EQ(s.width(), 0);
    ASSERT_EQ(s.length(), 3);

    EXPECT_EQ(s.row_idx(row1.c_str()), 0);
    EXPECT_EQ(s.row_idx(row3.c_str()), 2);
    EXPECT_EQ(s.row_idx("does not exist"), -1);
    EXPECT_EQ(s.row_idx(nullptr), -1);
}

TEST(testSchema, testSchmaRowIdx) {
    test_schema_row_idx();
}

/**
 * A schema should be equal if all types, names are the same and in the same 
 * order.
 */
void test_schema_equals() {
    Schema s;
    Schema not_s("IFS");
    Schema same_s;

    String row1("row 1 name");
    String row3("row 3 name");
    String col1("col 1 name");
    String col3("col 3 name");

    s.add_row(&row1);
    s.add_row(nullptr);
    s.add_row(&row3);

    s.add_column('I', &col1);
    s.add_column('F', nullptr);
    s.add_column('S', &col3);

    String row1_other("row 1 name");
    String row3_other("row 3 name");
    String col1_other("col 1 name");
    String col3_other("col 3 name");

    same_s.add_row(&row1_other);
    same_s.add_row(nullptr);
    same_s.add_row(&row3_other);

    same_s.add_column('I', &col1_other);
    same_s.add_column('F', nullptr);
    same_s.add_column('S', &col3_other);

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


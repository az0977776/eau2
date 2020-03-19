#pragma once

#include <gtest/gtest.h>

#include "../src/string.h"  
#include "../src/row.h"
#include "../src/schema.h"

#include "personal_test_macros.h"


/**************************** Test Row ***************************/

/**
 * Can a row be created with the correct types. 
 * NOTE: editing the schema after row creating was unspecified. 
 *       It was choosen to exit on bad changes
 */
void test_row_constructor() {
    Schema schema("IDSBB");
    Row row(schema);
    
    ASSERT_EQ(row.width(), schema.width());
    EXPECT_EQ(row.col_type(0), 'I');
    EXPECT_EQ(row.col_type(1), 'D');
    EXPECT_EQ(row.col_type(2), 'S');
    EXPECT_EQ(row.col_type(3), 'B');
    EXPECT_EQ(row.col_type(4), 'B');

    // check that row's schema does not change
    schema.add_column('S');
    ASSERT_NE(row.width(), schema.width());
    ASSERT_EQ(row.width(), schema.width() - 1);
    
    EXPECT_NE(row.col_type(5), 'S'); // This will exit with code -1
    exit(0);
}

TEST(testRow, testRowConstructor) {
    CS4500_ASSERT_EXIT_255(test_row_constructor);
}

/**
 * A row can be set and accessed. The correct values are expected. 
 * This checks both the first set, and reseting the values.
 */
void test_row_set_and_get() {
    Schema schema("BIDS");
    Row row(schema);
    String str1("string 1");
    String str2("string 2");

    double f1 = 123.456;
    double f2 = 654.321;
    
    row.set(0, true);
    row.set(1, 1234);
    row.set(2, f1);
    row.set(3, &str1);

    ASSERT_EQ(row.width(), 4);

    EXPECT_TRUE(row.get_bool(0));
    EXPECT_EQ(row.get_int(1), 1234);
    EXPECT_FLOAT_EQ(row.get_double(2), f1);
    EXPECT_TRUE(row.get_string(3)->equals(&str1));

    // reset row values
    row.set(0, false);
    row.set(1, 4321);
    row.set(2, f2);
    row.set(3, &str2);

    EXPECT_FALSE(row.get_bool(0));
    EXPECT_EQ(row.get_int(1), 4321);
    EXPECT_FLOAT_EQ(row.get_double(2), f2);
    EXPECT_TRUE(row.get_string(3)->equals(&str2));
}

TEST(testRow, testRowSetAndGet) {
    test_row_set_and_get();
}

/**
 * Can the index of the row in the dataframe be set and accessed in a row.
 */
void test_row_set_row_idx() {
    Schema schema("BIDS");
    Row row(schema);

    row.set_idx(100);

    EXPECT_EQ(row.get_idx(), 100);
}

TEST(testRow, testRowGetRowIdx) {
    test_row_set_row_idx();
}

/*
 * Fielder::
 * A field vistor used to test row.visit(). Only works on row with 4 columns in BIFS order.
 * is initialized with expected values. Also checks that start and done are called.
 */
class TestRowVisitFielder : public Fielder {
    public:
        bool b_;
        int i_;
        double f_;
        String* s_;  // does not own
        size_t col_idx_;
        bool called_start_;
        bool called_done_;

        TestRowVisitFielder(bool b, int i, double f, String* s) {
            b_ = b;
            i_ = i;
            f_ = f;
            s_ = s;
            col_idx_ = 0;
            called_start_ = false;
            called_done_ = false;
        }

        virtual ~TestRowVisitFielder() {
          EXPECT_TRUE(called_start_);
          EXPECT_TRUE(called_done_);
        }

        virtual void start(size_t r) {
            col_idx_ = 0;
            called_start_ = true;
        }
        
        virtual void accept(bool b) {
            EXPECT_EQ(b, b_); // equal to the initial values
            col_idx_++;
        }

        virtual void accept(double f) {
            EXPECT_FLOAT_EQ(f, f_); // equal to the initial values
            col_idx_++;
        }

        virtual void accept(int i) {
            EXPECT_EQ(i, i_); // equal to the initial values
            col_idx_++;
        }

        virtual void accept(String* s) {
            if (s == nullptr) {  // make sure it is not null before calling equals
                EXPECT_EQ(s, s_); // equal to the initial values
            } else {
                EXPECT_TRUE(s->equals(s_));
            }
            col_idx_++;
        }
        
        virtual void done() {
            EXPECT_EQ(col_idx_, 4);  // should be called after 4 columns
            called_done_ = true;
        }
};

/**
 * Checks that every field in the row is visited. Checks that the correct value is
 * accepted, and that all Fielder methods are called.
 */
void test_row_visit() {
    Schema schema("BIDS");
    Row row(schema);

    String str1("string 1");
    String str2("string 1");
    double f1 = 123.456;
    
    row.set(0, true);
    row.set(1, 1234);
    row.set(2, f1);
    row.set(3, &str1);

    TestRowVisitFielder f(true, 1234, f1, &str2);
    row.visit(0, f);

    // check that string equals with nullptr is workable
    row.set(3, nullptr);
    TestRowVisitFielder f2(true, 1234, f1, nullptr);
    row.visit(0, f2);
}

TEST(submittedTest5, testRowVisit) {
    test_row_visit();
}


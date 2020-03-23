//lang:cpp
#pragma once

#include "../src/util/string.h"
#include "../src/dataframe/sorer.h"
#include "../src/util/helper.h"
#include "../src/dataframe/column.h"

/**
 * These functions are used for testing.
 */
int test_count = 0;

void OK(const char* c) { 
    printf("OK: %s %d tests Finished.\n", c, test_count); 
    printf("-------------------------------------------------------------\n");
    test_count = 0;
}

void FAILURE(const char* c) { 
    printf("FAIL: %s\n", c); 
    abort();
}

void test(bool t, const char* c) {
    if (!t) {
        FAILURE(c);
    } else {
        test_count++;
    }
}

void test(float actual, float expected, float range, const char* name) {
    if (actual > expected) {
        test(actual - expected < range, name);
    } else {
        test(expected - actual < range, name);
    }
}

void test(double actual, double expected, double range, const char* name) {
    if (actual > expected) {
        test(actual - expected < range, name);
    } else {
        test(expected - actual < range, name);
    }
}

void test(String* actual, const char* expected, const char* name) {
    test(strcmp(actual->c_str(), expected) == 0, name);
}

bool compare_array_chars(char** actual, const char** expected, size_t len) {
    for (size_t i = 0; i < len; i++) {
        if (actual[i] == nullptr && expected[i] == nullptr) {
            continue;
        } else if (actual[i] != nullptr && expected[i] == nullptr) {
            return false;
        } else if (actual[i] == nullptr && expected[i] != nullptr) {
            return false;
        } 
        if (strcmp(actual[i], expected[i]) != 0) {
            return false;
        }
    }
    return true;
}

bool parse_row_wrapper(const char* input, const char** expected) {
    KVStore kvs;
    SOR* reader = new SOR("../data/test.sor", &kvs);
    char buf[2048];

    size_t num_fields = -1;

    char** temp = reader->parse_row_(strcpy(buf, input), &num_fields);

    bool ret = compare_array_chars(temp, expected, num_fields);

    delete[] temp;
    delete reader;

    return ret;
}

bool parse_field_wrapper(const char* input, const char* output, int* len) {
    KVStore kvs;
    SOR* reader = new SOR("../data/test.sor", &kvs);
    char buf[2048];

    char* temp = reader->parse_field_(strcpy(buf, input), len);

    bool ret = temp == output || temp;

    delete reader;
    return ret;
}

void test_parse_row_() {
    const char* expected[5] = {"1", "2", "3", "4", "5"};
    test(parse_row_wrapper("<1> <2> <3> <4> <5>", expected), "parse_row_() - all ints");

    const char* expected2[5] = {nullptr, nullptr, nullptr, nullptr, nullptr};
    test(parse_row_wrapper("<> <> <> <> <>", expected2), "parse_row_() - all empty");

    const char* expected3[5] = {"HELLO", nullptr, nullptr, nullptr, nullptr};
    test(parse_row_wrapper("<HELLO   > <> <> <> ", expected3), "parse_row_() - one string one missing 3 empty");

    const char* expected4[5] = {"Hello World", "-1234", "0", "1.44322", nullptr};
    test(parse_row_wrapper("   <   \"Hello World\"   >    <-1234> <  0   >    <1.44322    >   <>", expected4), "parse_row_() - different types");

    const char* expected5[5] = {"Hello World", "-1234", "0", "<>", nullptr};
    test(parse_row_wrapper("   <   \"Hello World\"   ><-1234><  0   ><\"<>\"   >   <>", expected5), "parse_row_() - quoted brackets");

    OK("Parse row test.");
}

void test_parse_field_() {
    int len = -1000;

    test(parse_field_wrapper("hello  >  <  tes", "hello", &len), "parse_field_() - string");
    test(len == 5, "Length Test 1");

    test(parse_field_wrapper("0  >  <  tes", "0", &len), "parse_field_() - boolean 0");
    test(len == 1, "Length Test 2");

    test(parse_field_wrapper("12345  >  <  tes", "12345", &len), "parse_field_() - positive int");
    test(len == 5, "Length Test 3");

    test(parse_field_wrapper("-12345  >  <  tes", "-12345", &len), "parse_field_() - negative int");
    test(len == 6, "Length Test 4");

    test(parse_field_wrapper("    -12345  >  <  tes", "-12345", &len), "parse_field_() - negative int with space");
    test(len == 10, "Length Test 5");

    test(parse_field_wrapper("\"Hello World\"  >  <  tes", "Hello World", &len), "parse_field_() - quoted string");
    test(len == 12, "Length Test 6");

    test(parse_field_wrapper("\"<>\"  >  <  tes", "<>", &len), "parse_field_() - quoted less than greater than");
    test(len == 3, "Length Test 7");

    test(parse_field_wrapper("><", nullptr, &len), "parse_field_() - missing field");
    test(len == 0, "Length Test 8");

    OK("Parse field test.");
}

void test_infer_columns_() {
    KVStore kvs;
    SOR reader("../data/test.sor", &kvs);

    Schema* schema = reader.infer_columns_(0, 10000);

    test(schema->width() == 5, "Number of columns");
    test(schema->col_type(0) == BOOL, "type of column 0");
    test(schema->col_type(1) == INT, "type of column 1");
    test(schema->col_type(2) == DOUBLE, "type of column 2");
    test(schema->col_type(3) == STRING, "type of column 3");
    test(schema->col_type(4) == STRING, "type of column 4");

    delete schema;

    OK("infer columns test.");
}

void test_read() {
    KVStore kvs;
    SOR reader("../data/test.sor", &kvs);

    printf("before read\n");
    DataFrame* df = reader.read(0, 10000);
    Schema schema = df->get_schema();
    
    printf("before the col type check\n");

    test(df->ncols() == 5, "Number of columns");
    test(df->nrows() == 9, "Number of rows");
    test(schema.col_type(0) == BOOL, "type of column 0");
    test(schema.col_type(1) == INT, "type of column 1");
    test(schema.col_type(2) == DOUBLE, "type of column 2");
    test(schema.col_type(3) == STRING, "type of column 3");
    test(schema.col_type(4) == STRING, "type of column 4");
    printf("Before bool check\n");
    test(df->get_bool(0, 0) == false, "Value at column 0 row 0");
    test(df->get_bool(0, 1) == true, "Value at column 0 row 1");
    test(df->get_bool(0, 5) == false, "Value at column 0 row 5");
    test(df->get_bool(0, 6) == false, "Value at column 0 row 6");
    test(df->get_bool(0, 7) == true, "Value at column 0 row 7");
    printf("Before int check\n");

    test(df->get_int(1, 0) == 1, "Value at column 1 row 0");
    test(df->get_int(1, 1) == 133454, "Value at column 1 row 1");
    test(df->get_int(1, 5) == 0, "Value at column 1 row 5");
    test(df->get_int(1, 6) == 6, "Value at column 1 row 6");
    test(df->get_int(1, 7) == 7, "Value at column 1 row 7");
    printf("Before double check\n");

    test(df->get_double(2, 0) == 123, "Value at column 2 row 0");
    test(df->get_double(2, 1),  -123.938, 0.001, "Value at column 2 row 1");
    test(df->get_double(2, 5) == 0, "Value at column 2 row 5");
    test(df->get_double(2, 6) == -123, "Value at column 2 row 6");
    test(df->get_double(2, 7) == 444, "Value at column 2 row 7");
    printf("Before stirng check\n");

    test(df->get_string(3, 0), "-12444.21123", "Value at column 3 row 0");
    test(df->get_string(3, 1), "hello", "Value at column 3 row 1");
    test(df->get_string(3, 5), "", "Value at column 3 row 5");
    test(df->get_string(3, 6), "+12444.21123", "Value at column 3 row 6");
    test(df->get_string(4, 0), "hello world", "Value at column 4 row 0");

    delete df;

    OK("read test.");
}

void test_partial_file_read() {
    KVStore kvs;
    SOR reader("../data/test.sor", &kvs);

    DataFrame* df = reader.read(50, 10000);
    Schema schema = df->get_schema();

    test(df->ncols() == 5, "Number of columns");
    test(df->nrows() == 7, "Number of rows");
    test(schema.col_type(0) == BOOL, "type of column 0");
    test(schema.col_type(1) == INT, "type of column 1");
    test(schema.col_type(2) == INT, "type of column 2");
    test(schema.col_type(3) == DOUBLE, "type of column 3");
    test(schema.col_type(4) == STRING, "type of column 4");

    test(df->get_bool(0, 0) == false, "Value at column 0 row 0");
    test(df->get_int(1, 1) == 3, "Value at column 1 row 1");
    test(df->get_int(2, 1) == 123, "Value at column 2 row 1");
    test(df->get_double(3, 1), -12444.21123, 0.00001, "Value at column 3 row 1");
    test(df->get_string(4, 0), "quoted string", "Value at column 4 row 0");

    OK("start middle of file read test.");
    
    SOR reader2("../data/test.sor", &kvs);

    DataFrame* df2 = reader2.read(0, 85);
    Schema schema2 = df2->get_schema();

    test(df2->ncols() == 5, "Number of columns");
    test(df2->nrows() == 2, "Number of rows");
    
    test(schema2.col_type(0) == BOOL, "type of column 0");
    test(schema2.col_type(1) == INT, "type of column 1");
    test(schema2.col_type(2) == DOUBLE, "type of column 2");
    test(schema2.col_type(3) == STRING, "type of column 3");
    test(schema2.col_type(4) == STRING, "type of column 4");

    test(df2->get_bool(0, 0) == false, "Value at column 0 row 0");
    test(df2->get_int(1, 1) == 133454, "Value at column 1 row 1");
    test(df2->get_double(2, 1), -123.938, 0.001, "Value at column 2 row 1");
    test(df2->get_string(3, 1), "hello", "Value at column 3 row 1");
    test(df2->get_string(4, 0), "hello world", "Value at column 4 row 0");

    delete df;
    delete df2;

    OK("end middle of file read test.");
}

void run_sorer_tests() {
    test_parse_field_();
    test_parse_row_();
    test_infer_columns_();
    test_read();
    test_partial_file_read();

    printf("All sorer tests are good.\n");
    printf("===================================================================\n\n");
}
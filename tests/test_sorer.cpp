//lang:cpp
#include <string.h>

#include "sorer.h"
#include "helpers.h"
#include "column.h"

/**
 * These functions are used for testing.
 */
int test_count = 0;

void OK(const char* c) { 
    printf("OK: %s %d tests Finished.\n", c, test_count); 
    printf("-------------------------------------------------------------\n");
    test_count = 0;
}

void FAIL(const char* c) { 
    printf("FAIL: %s\n", c); 
    abort();
}

void test(bool t, const char* c) {
    if (!t) {
        FAIL(c);
    } else {
        test_count++;
    }
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
    SOR* reader = new SOR();
    char buf[2048];

    size_t num_fields = -1;

    char** temp = reader->parse_row_(strcpy(buf, input), &num_fields);

    bool ret = compare_array_chars(temp, expected, num_fields);

    
    delete temp;
    delete reader;
    return ret;
}

bool parse_field_wrapper(const char* input, const char* output, int* len) {
    SOR* reader = new SOR();
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
    FILE *f = fopen("data.sor", "r");
    affirm(f != NULL, "File is null pointer");

    SOR* reader = new SOR();

    reader->infer_columns_(f, 0, 10000);

    test(reader->len_ == 3, "Number of columns");
    test(reader->get_col_type(0) == type_bool, "type of column 0");
    test(reader->get_col_type(1) == type_int, "type of column 1");
    test(reader->get_col_type(2) == type_string, "type of column 2");
    test(reader->get_col_type(3) == type_unknown, "type of column 3");

    delete reader;

    OK("infer columns test.");
}

void test_read() {
    FILE *f = fopen("data.sor", "r");
    affirm(f != NULL, "File is null pointer");

    SOR* reader = new SOR();

    reader->read(f, 0, 10000);

    test(reader->len_ == 3, "Number of columns");
    test(reader->get_col_type(0) == type_bool, "type of column 0");
    test(reader->get_col_type(1) == type_int, "type of column 1");
    test(reader->get_col_type(2) == type_string, "type of column 2");
    test(reader->get_col_type(3) == type_unknown, "type of column 3");

    test(strcmp(reader->get_value(0, 0), "0") == 0, "Value at column 0 row 0");
    test(strcmp(reader->get_value(0, 1), "1") == 0, "Value at column 0 row 1");
    test(strcmp(reader->get_value(0, 2), "1") == 0, "Value at column 0 row 2");
    test(strcmp(reader->get_value(0, 3), "0") == 0, "Value at column 0 row 3");

    test(strcmp(reader->get_value(1, 0), "23") == 0, "Value at column 1 row 0");
    test(strcmp(reader->get_value(1, 1), "12") == 0, "Value at column 1 row 1");
    test(strcmp(reader->get_value(1, 2), "1") == 0, "Value at column 1 row 2");
    test(reader->get_value(1, 3) == nullptr, "Value at column 1 row 3");

    test(strcmp(reader->get_value(2, 0), "\"hi\"") == 0, "Value at column 2 row 0");
    test(strcmp(reader->get_value(2, 1), "\"Hello World\"") == 0, "Value at column 2 row 1");
    test(reader->get_value(2, 2) == nullptr, "Value at column 2 row 2");
    test(reader->get_value(2, 3) == nullptr, "Value at column 2 row 3");

    delete reader;

    OK("read test.");
}

void test_is_missing() {
    FILE *f = fopen("data.sor", "r");
    affirm(f != NULL, "File is null pointer");

    SOR* reader = new SOR();

    reader->read(f, 0, 10000);

    test(!reader->is_missing(0, 0), "column 0 row 0 is not missing a value");
    test(!reader->is_missing(0, 1), "column 0 row 1 is not missing a value");
    test(!reader->is_missing(0, 2), "column 0 row 2 is not missing a value");
    test(!reader->is_missing(0, 3), "column 0 row 3 is not missing a value");

    test(!reader->is_missing(1, 0), "column 1 row 0 is not missing a value");
    test(!reader->is_missing(1, 1), "column 1 row 1 is not missing a value");
    test(!reader->is_missing(1, 2), "column 1 row 2 is not missing a value");
    test(reader->is_missing(1, 3), "column 1 row 3 is missing a value");

    test(!reader->is_missing(2, 0), "column 2 row 0 is not missing a value");
    test(!reader->is_missing(2, 1), "column 2 row 1 is not missing a value");
    test(reader->is_missing(2, 2), "column 2 row 2 is missing a value");
    test(reader->is_missing(2, 3), "column 2 row 3 is missing a value");

    delete reader;

    OK("is_missing test.");
}

void test_partial_file_read() {
    FILE *f = fopen("data.sor", "r");
    affirm(f != NULL, "File is null pointer");

    SOR* reader = new SOR();

    reader->read(f, 5, 10000);

    test(reader->len_ == 3, "Number of columns");
    test(reader->get_col_type(0) == type_bool, "type of column 0");
    test(reader->get_col_type(1) == type_int, "type of column 1");
    test(reader->get_col_type(2) == type_string, "type of column 2");
    test(reader->get_col_type(3) == type_unknown, "type of column 3");

    test(strcmp(reader->get_value(0, 0), "1") == 0, "Value at column 0 row 0");
    test(strcmp(reader->get_value(1, 0), "12") == 0, "Value at column 1 row 0");

    delete reader;

    OK("start middle of file read test.");

    SOR* reader2 = new SOR();

    reader2->read(f, 0, 40);

    test(reader2->len_ == 3, "Number of columns");
    test(reader2->get_col_type(0) == type_bool, "type of column 0");
    test(reader2->get_col_type(1) == type_int, "type of column 1");
    test(reader2->get_col_type(2) == type_string, "type of column 2");
    test(reader2->get_col_type(3) == type_unknown, "type of column 3");

    test(strcmp(reader2->get_value(0, 0), "0") == 0, "Value at column 0 row 0");
    test(strcmp(reader2->get_value(1, 0), "23") == 0, "Value at column 1 row 0");
    test(strcmp(reader2->get_value(0, 1), "1") == 0, "Value at column 0 row 1");
    test(strcmp(reader2->get_value(1, 1), "12") == 0, "Value at column 1 row 1");
    test(reader2->get_value(0, 2) == nullptr, "Value at column 0 row 2");
    test(reader2->get_value(1, 2) == nullptr, "Value at column 1 row 2");

    delete reader2;

    OK("end middle of file read test.");
}

void test_can_add() {
    ColumnBool column_bool = ColumnBool();
    ColumnInt column_int = ColumnInt();
    ColumnFloat column_float = ColumnFloat();
    ColumnString column_string = ColumnString();
    char buf[2048];

    test(column_bool.can_add(strcpy(buf, "")), "Can add '' to boolean column");
    test(column_bool.can_add(nullptr), "Can add nullptr to boolean column");
    test(column_bool.can_add(strcpy(buf, "0")), "Can add 0 to boolean column");
    test(column_bool.can_add(strcpy(buf, "1")), "Can add 1 to boolean column");
    test(!column_bool.can_add(strcpy(buf, "111")), "Can not add 111 to boolean column");
    test(!column_bool.can_add(strcpy(buf, "1.3")), "Can not add 1.3 to boolean column");
    test(!column_bool.can_add(strcpy(buf, "hello")), "Can not add 111 to boolean column");

    test(column_int.can_add(strcpy(buf, "")), "Can add '' to integer column");
    test(column_int.can_add(nullptr), "Can add nullptr to integer column");
    test(column_int.can_add(strcpy(buf, "0")), "Can add 0 to integer column");
    test(column_int.can_add(strcpy(buf, "1")), "Can add 1 to integer column");
    test(column_int.can_add(strcpy(buf, "111")), "Can add 111 to integer column");
    test(column_int.can_add(strcpy(buf, "-111")), "Can add -111 to integer column");
    test(column_int.can_add(strcpy(buf, "+111")), "Can add +111 to integer column");
    test(!column_int.can_add(strcpy(buf, "1.3")), "Can not add 1.3 to integer column");
    test(!column_int.can_add(strcpy(buf, "hello")), "Can not add 111 to integer column");

    test(column_float.can_add(strcpy(buf, "")), "Can add '' to float column");
    test(column_float.can_add(nullptr), "Can add nullptr to float column");
    test(column_float.can_add(strcpy(buf, "0")), "Can add 0 to float column");
    test(column_float.can_add(strcpy(buf, "1")), "Can add 1 to float column");
    test(column_float.can_add(strcpy(buf, "111")), "Can add 111 to float column");
    test(column_float.can_add(strcpy(buf, "1.3")), "Can add 1.3 to float column");
    test(column_float.can_add(strcpy(buf, "-1.3")), "Can add -1.3 to float column");
    test(column_float.can_add(strcpy(buf, "+1.3")), "Can add +1.3 to float column");
    test(!column_float.can_add(strcpy(buf, "hello")), "Can not add 111 to float column");

    test(column_string.can_add(strcpy(buf, "")), "Can add '' to string column");
    test(column_string.can_add(nullptr), "Can add nullptr to string column");
    test(column_string.can_add(strcpy(buf, "0")), "Can add 0 to string column");
    test(column_string.can_add(strcpy(buf, "1")), "Can add 1 to string column");
    test(column_string.can_add(strcpy(buf, "111")), "Can add 111 to string column");
    test(column_string.can_add(strcpy(buf, "1.3")), "Can add 1.3 to string column");
    test(column_string.can_add(strcpy(buf, "hello")), "Can not add 111 to string column");

    OK("Can add to column test.");

}

void column_get_type() {
    ColumnBool column_bool = ColumnBool();
    ColumnInt column_int = ColumnInt();
    ColumnFloat column_float = ColumnFloat();
    ColumnString column_string = ColumnString();

    test(column_bool.get_type() == type_bool, "Boolean column has correct type");
    test(column_int.get_type() == type_int, "Integer column has correct type");
    test(column_float.get_type() == type_float, "Float column has correct type");
    test(column_string.get_type() == type_string, "String column has correct type");

    OK("Columns with correct type test.");
}


int main() {
    test_parse_field_();
    test_parse_row_();
    test_infer_columns_();
    test_read();
    test_is_missing();
    test_partial_file_read();
    test_can_add();
    column_get_type();

    printf("ALL tests are good.\n\n");
    return 0;
}
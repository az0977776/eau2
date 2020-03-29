#include <gtest/gtest.h>

#include "../../src/util/string.h"  
#include "../../src/dataframe/dataframe.h"
#include "../../src/dataframe/schema.h"
#include "../../src/dataframe/column.h"
#include "../../src/dataframe/row.h"

#include "test_macros.h"


/**************************** Test DataFrame ***************************/

/**
 * A function to creat a DataFrame with default values.
 */
DataFrame* build_data_frame(int size, String& str, Key& key, KVStore& kvs) {
    Schema schema("BIDS");

    DataFrame* df = new DataFrame(schema, key, &kvs);
    Row r(df->get_schema());

    double d = 0.1;

    for(int i = 0; i < size; i++) {
        d = (i * 1.0) / (1.0 * size);
        r.set(0, i % 2 == 0);
        r.set(1, i);
        r.set(2, d);
        r.set(3, &str);
        df->add_row(r);
    }

    return df;
}

/**
 * Does the DataFrame create correctly from a Schema? with correct types.
 * NOTE: also tests that changing the schema after adding does not work. This was left
 * as unspecified by the API.
 */
void test_dataframe_constructor_by_schema() {
    Key key(0, "Some_key");
    Key key2(0, "Other_key");
    KVStore kvs(false);
    Schema schema1("IDSBB");
    Schema schema2("I");

    DataFrame df1(schema1, key, &kvs);
    DataFrame df2(schema2, key2, &kvs);

    ASSERT_EQ(df1.ncols(), 5);
    ASSERT_EQ(df1.nrows(), 0);

    ASSERT_EQ(df2.ncols(), 1);
    EXPECT_EQ(df1.get_schema().col_type(0), 'I');
    EXPECT_EQ(df1.get_schema().col_type(1), 'D');
    EXPECT_EQ(df1.get_schema().col_type(2), 'S');
    EXPECT_EQ(df1.get_schema().col_type(3), 'B');
    EXPECT_EQ(df1.get_schema().col_type(4), 'B');

    // check that row's schema does not change
    schema1.add_column('S');
    ASSERT_EQ(df1.ncols(), 5);

    EXPECT_NE(df1.get_schema().col_type(5), 'S'); // This will exit with code -1
    exit(0);
}

TEST(testDataFrame, testDataFrameConstructorSchema) {
    CS4500_ASSERT_EXIT_255(test_dataframe_constructor_by_schema);
}

/**
 * Test that DataFrame copies only the column names. 
 * NOTE: Also tests behavior that was unspecified by the API.
 *      Choose to exit with code -1.
 */
void test_dataframe_copy_constructor() {
    Key key(0, "Some_key");
    KVStore kvs(false);
    Key key_copy(0, "key_copy");
    int size = 10;
    String str("apple");
    DataFrame* df = build_data_frame(size, str, key, kvs);

    ASSERT_EQ(df->ncols(), 4);
    ASSERT_EQ(df->nrows(), size);
    
    EXPECT_EQ(df->get_schema().col_type(0), 'B');
    EXPECT_EQ(df->get_schema().col_type(1), 'I');
    EXPECT_EQ(df->get_schema().col_type(2), 'D');
    EXPECT_EQ(df->get_schema().col_type(3), 'S');
    
    EXPECT_EQ(df->get_int(1, 4), 4);

    DataFrame df_copy(*df, key_copy);

    ASSERT_EQ(df_copy.ncols(), df->ncols());
    ASSERT_EQ(df_copy.nrows(), 0);
    
    EXPECT_EQ(df_copy.get_schema().col_type(0), df->get_schema().col_type(0));
    EXPECT_EQ(df_copy.get_schema().col_type(1), df->get_schema().col_type(1));
    EXPECT_EQ(df_copy.get_schema().col_type(2), df->get_schema().col_type(2));
    EXPECT_EQ(df_copy.get_schema().col_type(3), df->get_schema().col_type(3));

    EXPECT_NE(df_copy.get_int(1, 4), 4); // This should fail and exit with code -1

    delete df;
    exit(0);
}

TEST(testDataFrame, testDataFrameCopyConstructor) {
    CS4500_ASSERT_EXIT_255(test_dataframe_copy_constructor);
}

/**
 * Does the dataframe return the correct schema, even after adding columns.
 */
void test_dataframe_get_schema() {
    Key key(0, "Some_key");
    KVStore kvs(false);
    Schema schema("IBDS");
    String col_name("column name");

    schema.add_column('I');

    DataFrame df(schema, key, &kvs);

    EXPECT_TRUE(df.get_schema().equals(&schema));

    schema.add_column('B');

    EXPECT_FALSE(df.get_schema().equals(&schema)); // do not mutate the values
}

TEST(testDataFrame, testDataFrameGetSchema) {
    test_dataframe_get_schema();
}

/**
 * Can we add a column to a dataframe. 
 * NOTE: does not copy the givne columns
 */
void test_dataframe_add_column() {
    Key key(0, "Some_key");
    KVStore kvs(false);

    String col_name1("col_name_1");
    String col_name2("col_name_2");
    String col_name3("col_name_3");

    Schema schema("");
    DataFrame df(schema, key, &kvs);

    IntColumn ic1(&col_name1, &kvs, 5, 123, 456, 789, 1000, 2000);
    BoolColumn bc(&col_name2, &kvs, 5, true, true, false, true, false);
    IntColumn ic2(&col_name3, &kvs, 5, 5000, 3000, 1000, 59, -1);

    df.add_column(&ic1);
    df.add_column(&bc);
    df.add_column(&ic2);

    // Dataframe has rows and columns
    ASSERT_EQ(df.ncols(), 3);
    ASSERT_EQ(df.nrows(), 5);

    // added to rows/cols to schema
    ASSERT_EQ(df.get_schema().width(), 3);
    ASSERT_EQ(df.get_schema().length(), 5);
    
    // added with correct types
    EXPECT_EQ(df.get_schema().col_type(0), 'I');
    EXPECT_EQ(df.get_schema().col_type(1), 'B');
    EXPECT_EQ(df.get_schema().col_type(2), 'I');

    // added values
    EXPECT_EQ(df.get_int(0, 4), 2000);
    EXPECT_TRUE(df.get_bool(1, 3));
    EXPECT_EQ(df.get_int(2, 4), -1);
}

TEST(testDataFrame, testDataFrameAddColumn) {
    test_dataframe_add_column();
}

/**
 * Does DataFrame get the correct values.
 */
void test_dataframe_get_values() {
    Key key(0, "Some_key");
    KVStore kvs(false);
    int size = 10;
    String s("apple");
    DataFrame* df = build_data_frame(size, s, key, kvs);

    ASSERT_EQ(df->ncols(), 4);
    ASSERT_EQ(df->nrows(), size);
    
    EXPECT_EQ(df->get_schema().col_type(0), 'B');
    EXPECT_EQ(df->get_schema().col_type(1), 'I');
    EXPECT_EQ(df->get_schema().col_type(2), 'D');
    EXPECT_EQ(df->get_schema().col_type(3), 'S');

    EXPECT_FALSE(df->get_bool(0, 1));
    EXPECT_TRUE(df->get_bool(0, 8));
    
    EXPECT_EQ(df->get_int(1, 0), 0);
    EXPECT_EQ(df->get_int(1, 4), 4);
    
    EXPECT_FLOAT_EQ(df->get_double(2, 0), 0);
    EXPECT_FLOAT_EQ(df->get_double(2, 2), 2.0 / (1.0 * size));
    
    EXPECT_TRUE(df->get_string(3, 1)->equals(&s));

    delete df;
}

TEST(testDataFrame, testDataFrameGet) {
    test_dataframe_get_values();
}

/**
 * Does DataFrame set the correct values.
 */
void test_dataframe_set_values() {
    Key key(0, "Some_key");
    KVStore kvs(false);
    int size = 10;
    String s("apple");
    String s2("orange");
    DataFrame* df = build_data_frame(size, s, key, kvs);

    ASSERT_EQ(df->ncols(), 4);
    ASSERT_EQ(df->nrows(), size);
    
    ASSERT_EQ(df->get_schema().col_type(0), 'B');
    ASSERT_EQ(df->get_schema().col_type(1), 'I');
    ASSERT_EQ(df->get_schema().col_type(2), 'D');
    ASSERT_EQ(df->get_schema().col_type(3), 'S');

    EXPECT_FALSE(df->get_bool(0, 1));
    EXPECT_TRUE(df->get_bool(0, 8));
    df->set(0, 1, true);
    df->set(0, 8, false);
    EXPECT_TRUE(df->get_bool(0, 1));
    EXPECT_FALSE(df->get_bool(0, 8));
    
    EXPECT_EQ(df->get_int(1, 0), 0);
    EXPECT_EQ(df->get_int(1, 4), 4);
    df->set(1, 0, -1000);
    df->set(1, 4, -999);
    EXPECT_EQ(df->get_int(1, 0), -1000);
    EXPECT_EQ(df->get_int(1, 4), -999);
    
    EXPECT_FLOAT_EQ(df->get_double(2, 0), 0);
    EXPECT_FLOAT_EQ(df->get_double(2, 2), 2.0 / (1.0 * size));
    double f1 = 1.2345001;
    double f2 = 1920284.9;
    df->set(2, 0, f1);
    df->set(2, 2, f2);
    EXPECT_FLOAT_EQ(df->get_double(2, 0), f1);
    EXPECT_FLOAT_EQ(df->get_double(2, 2), f2);
    
    EXPECT_TRUE(df->get_string(3, 1)->equals(&s));
    df->set(3, size - 1, &s2);
    EXPECT_TRUE(df->get_string(3, size - 1)->equals(&s2));

    delete df;
}

TEST(testDataFrame, testDataFrameSet) {
    test_dataframe_set_values();
}

/**
 * DataFrame retuns the correct index based on name.
 */
void test_dataframe_get_col() {
    Key key(0, "Some_key");
    KVStore kvs(false);
    Schema schema("");
    DataFrame df(schema, key, &kvs);
    
    String col1_name("intcol");
    String col2_name("boolcol");
    String col3_name("intcol2");
    IntColumn ic1(&col1_name, &kvs, 5, 123, 456, 789, 1000, 2000);
    BoolColumn bc(&col2_name, &kvs, 5, true, true, false, true, false);
    IntColumn ic2(&col3_name, &kvs, 5, 5000, 3000, 1000, 59, -1);

    String ic1_name("first int");
    String bc_name("bool column");
    String ic2_name("second int");
    String does_not_exist("does not exist");

    df.add_column(&ic1);
    df.add_column(&bc);
    df.add_column(&ic2);

    // Dataframe has rows and columns
    ASSERT_EQ(df.ncols(), 3);
    ASSERT_EQ(df.nrows(), 5);

    // added to rows/cols to schema
    ASSERT_EQ(df.get_schema().width(), 3);
    ASSERT_EQ(df.get_schema().length(), 5);
    
    // added with correct types
    ASSERT_EQ(df.get_schema().col_type(0), 'I');
    ASSERT_EQ(df.get_schema().col_type(1), 'B');
    ASSERT_EQ(df.get_schema().col_type(2), 'I');
}

TEST(testDataFrame, testDataFrameGetCol) {
    test_dataframe_get_col();
}

/**
 * DataFrame puts the correct information into the given row.
 */
void test_dataframe_fill_row() {
    Key key(0, "Some_key");
    KVStore kvs(false);
    int size = 10;
    String s("apple");
    DataFrame* df = build_data_frame(size, s, key, kvs);
    Row row(df->get_schema());

    ASSERT_EQ(df->ncols(), 4);
    ASSERT_EQ(df->nrows(), size);
    
    ASSERT_EQ(df->get_schema().col_type(0), 'B');
    ASSERT_EQ(df->get_schema().col_type(1), 'I');
    ASSERT_EQ(df->get_schema().col_type(2), 'D');
    ASSERT_EQ(df->get_schema().col_type(3), 'S');

    df->fill_row(6, row);
    EXPECT_EQ(row.get_bool(0), df->get_bool(0, 6));
    EXPECT_EQ(row.get_int(1), df->get_int(1, 6));
    EXPECT_FLOAT_EQ(row.get_double(2), df->get_double(2, 6));
    EXPECT_TRUE(row.get_string(3)->equals(df->get_string(3, 6)));

    df->fill_row(9, row);
    EXPECT_EQ(row.get_bool(0), df->get_bool(0, 9));
    EXPECT_EQ(row.get_int(1), df->get_int(1, 9));
    EXPECT_FLOAT_EQ(row.get_double(2), df->get_double(2, 9));
    EXPECT_TRUE(row.get_string(3)->equals(df->get_string(3, 9)));

    delete df;
}

TEST(testDataFrame, testDataFillRow) {
    test_dataframe_fill_row();
}

// ********************* Submitted test 1 *******************************
// FilterOddRower is a subclass of Rower that is used for filtering out rows 
// with odd values as their first field
class FilterOddRower : public Rower {
    public:

        // checks if the row has a odd first field
        bool accept(Row& r) {
          return r.get_int(0) % 2 == 0;
        }

        Rower* clone() { 
          return new FilterOddRower();
        }
};

void test_filter() {
    Key key(0, "Some_key");
    KVStore kvs(false);
    // creates a schema and adds a column with a name
    Schema s("IBDS");
    String stri("cow");
    s.add_column('I');

    DataFrame df(s, key, &kvs);
    Row r(df.get_schema());
    String str("apple");

    // populate the DataFrame
    int size = 10;
    for(int i = 0; i < size; i++) {
      r.set(0, i);
      r.set(1, true);
      r.set(2, (size * 1.0 / (i + 1)));
      r.set(3, &str);
      r.set(4, i * 2);
      df.add_row(r);
    }

    FilterOddRower f;
    // uses the Rower in the DataFrame filter function
    DataFrame *df2 = df.filter(f);
    
    // every other row should have been deleted
    ASSERT_EQ(df2->nrows(), size / 2);
    
    // every row in the new DataFrame should be even
    for (int i = 0; i < df2->nrows(); i++) {
      EXPECT_EQ(df2->get_int(0, i) % 2, 0);
    }

    delete df2;
}

TEST(testDataFrame, testDataFrameFilter) {
  test_filter();
}

// ********************* Submitted test 2 *******************************
// Taxes is a Rower subclass that calculates and mutates the DataFrame to save the tax amount
class Taxes : public Rower {  
    public:
        DataFrame* df_;
        size_t salary=0, rate=1, isded=2, ded=3, taxes=4;  // these are for convenience

        Taxes(DataFrame* df) : df_(df) {}
        
        bool accept(Row& r) {
            // calculates the amoubt of tax for each row
            int tx = (int)r.get_int(salary) * r.get_double(rate);
            tx -= r.get_bool(isded) ? r.get_int(ded) : 0;
            df_->set(taxes, r.get_idx(), tx);
            return true;
        }
};

void test_map() {
    Key key(0, "Some_key");
    KVStore kvs(false);
    // Creating a data frame with the right structure 
    Schema scm("IDBII");       // the schema
    DataFrame df(scm, key, &kvs);         // the data frame  

    // populdates the DataFrame
    int size = 10;
    Row r(df.get_schema());
    for(int i = 0; i < size; i++) {
        r.set(0, i * 100);
        r.set(1, (i * 40.0 / size));
        r.set(2, false);
        r.set(3, i);
        r.set(4, 0);
        df.add_row(r);
    }   

    Taxes tx(&df);     // create our rower
    df.map(tx);   // apply it to every row

    ASSERT_EQ(df.nrows(), 10);

    // ensures that the tax values in the DataFrame were updated
    for (int i = 0; i < size; i++) {
        ASSERT_EQ(df.get_int(4, i), df.get_int(0, i) * df.get_double(1, i));
    }
}

TEST(testDataFrame, testDataFrameMap) {
  test_map();
}


// ********************* Submitted test 3 *******************************
// uses FilterOddRower from above

/**
 * DataFrame prints in the expected format.
 * NOTE: the captures all stdout durring the test.
 */
void test_dataframe_print() {
    Key key(0, "Some_key");
    KVStore kvs(false);
    char expected[256] =   "<1><0><0><\"apple with space\">\n" \
                            "<0><1><0.2><\"apple with space\">\n" \
                            "<1><2><0.4><\"apple with space\">\n" \
                            "<0><3><0.6><\"apple with space\">\n" \
                            "<1><4><0.8><\"apple with space\">\n";
    char buf[256] = {0}; // should be big enough
    freopen("/dev/null", "a", stdout);  // don't print to screen
    setbuf(stdout, buf); // redirect stdout to buffer

    int size = 5;
    String s("apple with space");
    DataFrame* df = build_data_frame(size, s, key, kvs);

    ASSERT_EQ(df->ncols(), 4);
    ASSERT_EQ(df->nrows(), size);
    
    EXPECT_EQ(df->get_schema().col_type(0), 'B');
    EXPECT_EQ(df->get_schema().col_type(1), 'I');
    EXPECT_EQ(df->get_schema().col_type(2), 'D');
    EXPECT_EQ(df->get_schema().col_type(3), 'S');

    
    df->print();
    freopen ("/dev/tty", "a", stdout);  // reset stdout
    EXPECT_STREQ(buf, expected);

    delete df;
}

TEST(testDataFrame, testDataFramePrint) {
    test_dataframe_print();
}

/**
 * Check DataFrame equality between values. Columns are compared with 
 * pointer equality.
 */
void test_dataframe_equals() {
    Key key0(0, "Some_key1");
    Key key1(0, "Some_key2");
    Key key2(0, "Some_key3");
    Key key3(0, "Some_key4");
    Key key4(0, "Some_key5");
    Key key5(0, "Some_key6");
    KVStore kvs(false);
    int size = 5;
    String s1("apple");
    String s2("orange");
    DataFrame* df = build_data_frame(size, s1, key1, kvs);
    DataFrame* df_shallow_copy = new DataFrame(*df, key2);
    DataFrame* not_df = build_data_frame(size, s2, key3, kvs);
    DataFrame* not_df2 = build_data_frame(size * 2, s1, key4, kvs);

    Schema schema("");

    DataFrame df1(schema, key0, &kvs);
    String col1_name("intcol");
    String col2_name("boolcol");
    String col3_name("intcol2");
    IntColumn ic1(&col1_name, &kvs, 5, 123, 456, 789, 1000, 2000);
    BoolColumn bc(&col2_name, &kvs, 5, true, true, false, true, false);
    IntColumn ic2(&col3_name, &kvs, 5, 5000, 3000, 1000, 59, -1);
    
    df1.add_column(&ic1);
    df1.add_column(&bc);
    df1.add_column(&ic2);

    DataFrame df2(schema, key5, &kvs);
    df2.add_column(&ic1);
    df2.add_column(&bc);
    df2.add_column(&ic2);

    EXPECT_TRUE(df->equals(df));
    EXPECT_TRUE(df1.equals(&df2));  // columns have pointer equality
    EXPECT_FALSE(df->equals(not_df));
    EXPECT_FALSE(df->equals(not_df2));
    EXPECT_FALSE(df->equals(df_shallow_copy));
    
    EXPECT_FALSE(df->equals(&s1));

    delete df;
    delete df_shallow_copy;
    delete not_df;
    delete not_df2;
}

TEST(testDataFrame, testDataFrameEquals) {
    test_dataframe_equals();
}

void test_dataframe_serialize() {
    Key key(0, "Some_key");
    KVStore kvs(false);
    int size = 10;
    String s("apple");
    DataFrame* df = build_data_frame(size, s, key, kvs);

    char* serialized_df = df->serialize();

    DataFrame* df2 = DataFrame::deserialize(serialized_df, &kvs);

    ASSERT_EQ(df2->ncols(), df->ncols());
    ASSERT_EQ(df2->nrows(), df->nrows());
    
    EXPECT_EQ(df2->get_schema().col_type(0), df->get_schema().col_type(0));
    EXPECT_EQ(df2->get_schema().col_type(1), df->get_schema().col_type(1));
    EXPECT_EQ(df2->get_schema().col_type(2), df->get_schema().col_type(2));
    EXPECT_EQ(df2->get_schema().col_type(3), df->get_schema().col_type(3));

    EXPECT_EQ(df2->get_bool(0, 1), df->get_bool(0, 1));
    EXPECT_EQ(df2->get_bool(0, 8), df->get_bool(0, 8));
    
    EXPECT_EQ(df2->get_int(1, 0), df->get_int(1, 0));
    EXPECT_EQ(df2->get_int(1, 4), df->get_int(1, 4));
    
    EXPECT_FLOAT_EQ(df2->get_double(2, 0), df->get_double(2, 0));
    EXPECT_FLOAT_EQ(df2->get_double(2, 2), df->get_double(2, 2));
    
    EXPECT_TRUE(df2->get_string(3, 1)->equals(&s));

    delete df;
    delete df2;
    delete serialized_df;
}

TEST(testDataFrame, testDataFrameSerialize) {
    test_dataframe_serialize();
}
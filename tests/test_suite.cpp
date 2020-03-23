#include <gtest/gtest.h>

#include "test_sorer.h"
#include "test_map.h"

#include "test_keyvalue.h"
#include "test_kvstore.h"
#include "test_column.h"
#include "test_schema.h"
#include "test_row.h"
#include "test_dataframe.h"


int main(int argc, char **argv) {
    //run_sorer_tests();

    //run_map_tests();

    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

// TESTS TODO:
/**
 * exit with error code -1 int, double, string column
 * column push worng type onto it
 * StringArray ... is this needed to be tested?
 * row get/set wrong index
 * dataframe add_column -- wrong size, nullptr, 
 * dataframe get_* -- wrong idx, wrong type
 * dataframe set -- wrong index, wrong type
 * dataframe add_row/fill_row with different schema
 * 
 */

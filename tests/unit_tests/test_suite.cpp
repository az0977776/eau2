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

    testing::InitGoogleTest(&argc, argv);
    int rv = RUN_ALL_TESTS();
    
    run_sorer_tests();
    run_map_tests();
    return rv;
}

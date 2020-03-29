#include <gtest/gtest.h>

#define CS4500_ASSERT_EXIT_ZERO(a)  ASSERT_EXIT(a(), ::testing::ExitedWithCode(0), ".*");
#define CS4500_ASSERT_EXIT_255(a)  ASSERT_EXIT(a(), ::testing::ExitedWithCode(255), ".*"); // exit with code -1 == 255



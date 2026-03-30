#include "dagforge/util/log.hpp"
#include "test_utils.hpp"

#include "gtest/gtest.h"

#include <cstdio>
#include <string>

TEST(LoggerTest, RepeatedStartStopCyclesDoNotCrash) {
  const auto path = dagforge::test::make_temp_path("dagforge_log_cycle_");
  ASSERT_FALSE(path.empty());

  for (int i = 0; i < 5; ++i) {
    ASSERT_TRUE(dagforge::log::set_output_file(path));
    dagforge::log::start();
    dagforge::log::info("cycle {}", i);
    dagforge::log::stop();
  }

  dagforge::log::set_output_stderr();
  std::remove(path.c_str());
}

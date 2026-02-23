#include "dagforge/core/error.hpp"
#include "dagforge/io/result.hpp"
#include <gtest/gtest.h>

using namespace dagforge;
using namespace dagforge::io;

TEST(IoBridgeTest, ToResultSuccess) {
  auto res = ok<std::size_t>(10).and_then(
      [](std::size_t bytes) -> Result<std::size_t> { return ok(bytes * 2); });

  ASSERT_TRUE(res.has_value());
  EXPECT_EQ(*res, 20);
}

TEST(IoBridgeTest, ToResultFailure) {
  bool executed = false;
  Result<std::size_t> initial = fail(make_error_code(IoError::TimedOut));
  auto res = initial.and_then([&](std::size_t bytes) -> Result<std::size_t> {
    executed = true;
    return ok(bytes * 2);
  });

  ASSERT_FALSE(res.has_value());
  EXPECT_FALSE(executed);
  EXPECT_EQ(res.error(), make_error_code(IoError::TimedOut));
}

TEST(IoBridgeTest, DiscardBytes) {
  auto res = ok();

  ASSERT_TRUE(res.has_value());
  static_assert(std::is_same_v<decltype(res), Result<void>>);
}

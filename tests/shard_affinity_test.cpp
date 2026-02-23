#include "dagforge/core/runtime.hpp"

#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <chrono>
#include <thread>

namespace dagforge::test {
namespace {

constexpr unsigned kShardCount = 16;
constexpr auto kTimeout = std::chrono::seconds(2);

} // namespace

TEST(ShardAffinityTest, CrossShardPostsStayOnTargetShard) {
  Runtime runtime(kShardCount);
  ASSERT_TRUE(runtime.start().has_value());

  std::array<std::atomic<unsigned>, kShardCount> hits{};
  std::array<std::atomic<bool>, kShardCount> mismatch{};

  for (auto &v : hits) {
    v.store(0, std::memory_order_relaxed);
  }
  for (auto &v : mismatch) {
    v.store(false, std::memory_order_relaxed);
  }

  for (unsigned source = 0; source < kShardCount; ++source) {
    runtime.post_to(source, [&runtime, &hits, &mismatch]() {
      for (unsigned target = 0; target < kShardCount; ++target) {
        runtime.post_to(target, [&runtime, &hits, &mismatch, target]() {
          if (runtime.current_shard() != target) {
            mismatch[target].store(true, std::memory_order_relaxed);
          }
          hits[target].fetch_add(1, std::memory_order_relaxed);
        });
      }
    });
  }

  const auto deadline = std::chrono::steady_clock::now() + kTimeout;
  while (std::chrono::steady_clock::now() < deadline) {
    unsigned done = 0;
    for (const auto &h : hits) {
      done += h.load(std::memory_order_relaxed);
    }
    if (done >= (kShardCount * kShardCount)) {
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }

  for (unsigned i = 0; i < kShardCount; ++i) {
    EXPECT_GE(hits[i].load(std::memory_order_relaxed), kShardCount);
    EXPECT_FALSE(mismatch[i].load(std::memory_order_relaxed));
  }

  runtime.stop();
}

} // namespace dagforge::test

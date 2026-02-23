#include "dagforge/core/shard.hpp"

#include <boost/asio/post.hpp>

#include <atomic>
#include <memory_resource>

#include "gtest/gtest.h"

using namespace dagforge;

TEST(ShardTest, BasicConstruction) {
  Shard shard(0);
  EXPECT_EQ(shard.id(), 0);
}

TEST(ShardTest, ShardId) {
  Shard shard1(0);
  Shard shard2(5);
  Shard shard3(10);

  EXPECT_EQ(shard1.id(), 0);
  EXPECT_EQ(shard2.id(), 5);
  EXPECT_EQ(shard3.id(), 10);
}

TEST(ShardTest, CtxNotStoppedInitially) {
  Shard shard(0);
  EXPECT_FALSE(shard.ctx().stopped());
}

TEST(ShardTest, MemoryResourceNotNull) {
  Shard shard(0);
  EXPECT_NE(shard.memory_resource(), nullptr);
}

TEST(ShardTest, MemoryResourceAllocFreeWorks) {
  Shard shard(0);
  auto *mr = shard.memory_resource();
  ASSERT_NE(mr, nullptr);
  void *p = mr->allocate(64, alignof(std::max_align_t));
  EXPECT_NE(p, nullptr);
  mr->deallocate(p, 64, alignof(std::max_align_t));
}

TEST(ShardTest, ContextExecutesPostedWork) {
  Shard shard(0);
  std::atomic<int> counter{0};
  boost::asio::post(shard.ctx(), [&] { counter.fetch_add(1); });
  shard.ctx().run_one();
  EXPECT_EQ(counter.load(), 1);
}

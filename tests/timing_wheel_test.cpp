#include "dagforge/io/timing_wheel.hpp"
#include "dagforge/core/runtime.hpp"

#include <atomic>
#include <chrono>
#include <memory>
#include <thread>

#include "gtest/gtest.h"

using namespace dagforge;
using namespace std::chrono_literals;

namespace {

constexpr auto kPollInterval = std::chrono::milliseconds(1);
constexpr auto kWaitTimeout = std::chrono::seconds(1);

auto race_timing_wheel_sleep(std::shared_ptr<std::atomic<bool>> completed)
    -> spawn_task {
  using namespace boost::asio::experimental::awaitable_operators;
  (void)co_await (async_sleep_on_timing_wheel(5s) ||
                  async_sleep(std::chrono::milliseconds(1)));
  completed->store(true, std::memory_order_release);
}

} // namespace

TEST(TimingWheelTest, FiresDelayedCallback) {
  io::IoContext io;
  io::TimingWheel wheel(io, 5ms, 64);
  std::atomic<bool> fired{false};

  wheel.start();
  [[maybe_unused]] const auto fired_handle = wheel.schedule_after(25ms, [&] {
    fired.store(true, std::memory_order_release);
    wheel.stop();
  });

  std::jthread runner([&] { (void)io.run(); });

  const auto deadline = std::chrono::steady_clock::now() + kWaitTimeout;
  while (!fired.load(std::memory_order_acquire) &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(kPollInterval);
  }

  EXPECT_TRUE(fired.load(std::memory_order_acquire));
  io.stop();
}

TEST(TimingWheelTest, CancelPreventsDelayedCallback) {
  io::IoContext io;
  io::TimingWheel wheel(io, 5ms, 64);
  std::atomic<bool> cancelled_fired{false};
  std::atomic<bool> stopper_fired{false};

  wheel.start();
  const auto cancelled = wheel.schedule_after(20ms, [&] {
    cancelled_fired.store(true, std::memory_order_release);
  });
  [[maybe_unused]] const auto stopper_handle = wheel.schedule_after(60ms, [&] {
    stopper_fired.store(true, std::memory_order_release);
    wheel.stop();
  });

  EXPECT_TRUE(wheel.cancel(cancelled));

  std::jthread runner([&] { (void)io.run(); });

  const auto deadline = std::chrono::steady_clock::now() + kWaitTimeout;
  while (!stopper_fired.load(std::memory_order_acquire) &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(kPollInterval);
  }

  EXPECT_TRUE(stopper_fired.load(std::memory_order_acquire));
  EXPECT_FALSE(cancelled_fired.load(std::memory_order_acquire));
  io.stop();
}

TEST(TimingWheelTest, CancelsPendingSleepWhenRaceLoses) {
  Runtime runtime(1);
  ASSERT_TRUE(runtime.start().has_value());

  auto completed = std::make_shared<std::atomic<bool>>(false);

  runtime.spawn_on(shard_id{0}, race_timing_wheel_sleep(completed));

  const auto deadline = std::chrono::steady_clock::now() + kWaitTimeout;
  while (!completed->load(std::memory_order_acquire) &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(kPollInterval);
  }
  ASSERT_TRUE(completed->load(std::memory_order_acquire));

  std::this_thread::sleep_for(20ms);
  EXPECT_EQ(runtime.timing_wheel_pending_count(0), 0U);

  runtime.stop();
}

TEST(TimingWheelTest, ResumesAfterBecomingIdle) {
  io::IoContext io;
  io::TimingWheel wheel(io, 5ms, 64);
  auto guard = boost::asio::make_work_guard(io);
  std::atomic<int> fired_count{0};

  wheel.start();
  [[maybe_unused]] const auto first_handle = wheel.schedule_after(15ms, [&] {
    fired_count.fetch_add(1, std::memory_order_acq_rel);
  });

  std::jthread runner([&] { (void)io.run(); });

  const auto deadline = std::chrono::steady_clock::now() + kWaitTimeout;
  while (fired_count.load(std::memory_order_acquire) < 1 &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(kPollInterval);
  }
  ASSERT_EQ(fired_count.load(std::memory_order_acquire), 1);

  std::this_thread::sleep_for(20ms);
  EXPECT_EQ(wheel.pending_count(), 0U);

  std::atomic<bool> second_armed{false};
  const auto resume_deadline = std::chrono::steady_clock::now() + kWaitTimeout;
  boost::asio::post(io, [&] {
    [[maybe_unused]] const auto second_handle = wheel.schedule_after(15ms, [&] {
      fired_count.fetch_add(1, std::memory_order_acq_rel);
      wheel.stop();
      guard.reset();
    });
    second_armed.store(true, std::memory_order_release);
  });

  while (!second_armed.load(std::memory_order_acquire) &&
         std::chrono::steady_clock::now() < resume_deadline) {
    std::this_thread::sleep_for(kPollInterval);
  }

  while (fired_count.load(std::memory_order_acquire) < 2 &&
         std::chrono::steady_clock::now() < resume_deadline) {
    std::this_thread::sleep_for(kPollInterval);
  }
  EXPECT_EQ(fired_count.load(std::memory_order_acquire), 2);

  io.stop();
}

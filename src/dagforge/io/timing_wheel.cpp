#include "dagforge/io/timing_wheel.hpp"
#include "dagforge/core/runtime.hpp"

#include <boost/asio/as_tuple.hpp>
#include <boost/asio/experimental/channel.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/system/system_error.hpp>

#include <algorithm>
#include <experimental/scope>
#include <utility>

namespace dagforge::io {

TimingWheel::TimingWheel(IoContext &io, std::chrono::nanoseconds tick,
                         std::size_t slot_count)
    : io_(io), timer_(io),
      tick_(std::max(tick, std::chrono::nanoseconds(std::chrono::milliseconds(1)))),
      buckets_(std::max<std::size_t>(slot_count, 1)) {}

auto TimingWheel::start() -> void {
  if (running_) {
    return;
  }
  running_ = true;
  if (!locations_.empty()) {
    arm_next_tick();
  }
}

auto TimingWheel::stop() -> void {
  running_ = false;
  armed_ = false;
  (void)timer_.cancel();
  for (auto &bucket : buckets_) {
    bucket.clear();
  }
  locations_.clear();
  cursor_ = 0;
}

auto TimingWheel::schedule_after(std::chrono::nanoseconds delay,
                                 Callback callback) -> Handle {
  if (!running_) {
    running_ = true;
  }

  Handle handle{next_handle_++};
  const auto ticks = tick_count_for_delay(delay, tick_);
  const auto offset = static_cast<std::size_t>((ticks - 1) % buckets_.size());
  const auto bucket_index = (cursor_ + offset) % buckets_.size();

  auto &bucket = buckets_[bucket_index];
  bucket.push_back(Entry{.handle = handle,
                         .rounds = (ticks - 1) / buckets_.size(),
                         .callback = std::move(callback)});
  auto it = std::prev(bucket.end());
  locations_.emplace(handle.value,
                     EntryLocation{.bucket_index = bucket_index, .iterator = it});

  if (!armed_) {
    arm_next_tick();
  }
  return handle;
}

auto TimingWheel::cancel(Handle handle) -> bool {
  auto found = locations_.find(handle.value);
  if (found == locations_.end()) {
    return false;
  }

  buckets_[found->second.bucket_index].erase(found->second.iterator);
  locations_.erase(found);
  return true;
}

auto TimingWheel::pending_count() const noexcept -> std::size_t {
  return locations_.size();
}

auto TimingWheel::tick_count_for_delay(std::chrono::nanoseconds delay,
                                       std::chrono::nanoseconds tick)
    -> std::uint64_t {
  if (delay <= std::chrono::nanoseconds::zero()) {
    return 1;
  }

  const auto delay_count = delay.count();
  const auto tick_count = tick.count();
  return static_cast<std::uint64_t>((delay_count + tick_count - 1) / tick_count);
}

auto TimingWheel::arm_next_tick() -> void {
  if (!running_ || armed_) {
    return;
  }

  armed_ = true;
  timer_.expires_after(tick_);
  timer_.async_wait([this](const boost::system::error_code &ec) {
    handle_tick(ec);
  });
}

auto TimingWheel::handle_tick(const boost::system::error_code &ec) -> void {
  armed_ = false;
  if (!running_ || ec == boost::asio::error::operation_aborted) {
    return;
  }

  auto &bucket = buckets_[cursor_];
  Bucket survivors;

  while (!bucket.empty()) {
    auto it = bucket.begin();
    if (it->rounds > 0) {
      --it->rounds;
      survivors.splice(survivors.end(), bucket, it);
      continue;
    }

    auto callback = std::move(it->callback);
    locations_.erase(it->handle.value);
    bucket.erase(it);
    if (callback) {
      callback();
    }
  }

  if (!survivors.empty()) {
    bucket.splice(bucket.end(), survivors);
    for (auto it = bucket.begin(); it != bucket.end(); ++it) {
      locations_[it->handle.value] =
          EntryLocation{.bucket_index = cursor_, .iterator = it};
    }
  }

  cursor_ = (cursor_ + 1) % buckets_.size();
  if (!locations_.empty()) {
    arm_next_tick();
  }
}

} // namespace dagforge::io

namespace dagforge {

auto async_sleep_on_timing_wheel(std::chrono::nanoseconds duration)
    -> spawn_task {
  if (duration <= std::chrono::nanoseconds::zero()) {
    co_await async_yield();
    co_return;
  }

  auto *runtime = detail::current_runtime;
  const auto shard = detail::current_shard_id;
  if (runtime == nullptr || shard == kInvalidShard) {
    co_await async_sleep(duration);
    co_return;
  }

  using WaitChannel =
      boost::asio::experimental::channel<void(boost::system::error_code)>;
  auto gate = std::make_shared<WaitChannel>(runtime->current_context(), 1);

  const auto handle = runtime->schedule_after_on(shard, duration, [gate]() mutable {
    (void)gate->try_send(boost::system::error_code{});
  });
  const auto cancel_on_exit =
      std::experimental::scope_exit([runtime, shard, handle] {
        runtime->cancel_after_on(shard, handle);
      });

  auto [ec] =
      co_await gate->async_receive(boost::asio::as_tuple(boost::asio::use_awaitable));
  if (ec && ec != boost::asio::error::operation_aborted) {
    throw boost::system::system_error(ec);
  }
}

} // namespace dagforge

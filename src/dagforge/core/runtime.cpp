#include "dagforge/core/runtime.hpp"

#include "dagforge/core/constants.hpp"
#include "dagforge/core/shard.hpp"
#include "dagforge/util/hash.hpp"
#include "dagforge/util/log.hpp"

#include <boost/asio/post.hpp>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <ranges>
#include <thread>
#include <vector>

namespace dagforge {

namespace {
[[nodiscard]] auto now_monotonic_ms() -> std::uint64_t {
  const auto now = std::chrono::steady_clock::now().time_since_epoch();
  return static_cast<std::uint64_t>(
      std::chrono::duration_cast<std::chrono::milliseconds>(now).count());
}

template <typename F>
concept ShardVisitor = std::invocable<F, unsigned>;

template <ShardVisitor Fn>
auto for_each_shard(unsigned num_shards, Fn &&fn) -> void {
  for (auto i : std::views::iota(0U, num_shards)) {
    fn(i);
  }
}
} // namespace

Runtime::Runtime(unsigned num_shards) {
  if (num_shards == 0) {
    num_shards = 4;
  }
  num_shards_ = num_shards;

  shards_.reserve(num_shards);
  work_guards_.resize(num_shards);
  inbound_queues_.resize(num_shards);
  drain_scheduled_.reserve(num_shards);
  pending_inbound_.reserve(num_shards);
  shard_last_tick_ms_.reserve(num_shards);
  for (unsigned i = 0; i < num_shards; ++i) {
    drain_scheduled_.emplace_back(std::make_unique<std::atomic<bool>>(false));
    pending_inbound_.emplace_back(
        std::make_unique<std::atomic<std::uint32_t>>(0));
    shard_last_tick_ms_.emplace_back(
        std::make_unique<std::atomic<std::uint64_t>>(now_monotonic_ms()));
  }

  for (unsigned source = 0; source < num_shards; ++source) {
    inbound_queues_[source].resize(num_shards);
    for (unsigned target = 0; target < num_shards; ++target) {
      if (source == target) {
        continue;
      }
      inbound_queues_[source][target] = std::make_unique<PairQueue>();
    }
  }

  for_each_shard(num_shards, [this](unsigned i) {
    shards_.emplace_back(std::make_unique<Shard>(i));
  });
}

Runtime::~Runtime() noexcept { stop(); }

auto Runtime::start() -> Result<void> {
  if (running_.exchange(true))
    return ok();

  stop_requested_.store(false);

  log::debug("Starting runtime with {} shards", shards_.size());

  threads_.reserve(num_shards_);
  for_each_shard(num_shards_, [this](unsigned i) {
    auto &ctx = shards_[i]->ctx();
    ctx.restart();
    work_guards_[i].emplace(boost::asio::make_work_guard(ctx));
    threads_.emplace_back([this, i] { run_shard(i); });
  });

  start_stall_detection();

  return ok();
}

auto Runtime::stop() noexcept -> void {
  if (!running_.exchange(false))
    return;
  stop_requested_.store(true);
  stop_stall_detection();

  for (auto i : std::views::iota(0U, num_shards_)) {
    if (work_guards_[i].has_value()) {
      work_guards_[i]->reset();
      work_guards_[i].reset();
    }
    shards_[i]->ctx().stop();
  }

  // std::jthread auto-joins on destruction, just clear the vector
  threads_.clear();
}

auto Runtime::is_running() const noexcept -> bool {
  return running_.load(std::memory_order_acquire);
}

auto Runtime::current_shard() const noexcept -> shard_id {
  return detail::current_shard_id;
}

auto Runtime::is_current_shard() const noexcept -> bool {
  return detail::current_runtime == this &&
         detail::current_shard_id != kInvalidShard;
}

auto Runtime::current_context() noexcept -> io::IoContext & {
  return shards_[current_shard()]->ctx();
}

auto Runtime::run_shard(shard_id id) -> void {
  detail::current_shard_id = id;
  detail::current_runtime = this;

  auto &ctx = shards_[id]->ctx();
  start_heartbeat_on_shard(id);
  // Drain any cross-shard work that arrived before this shard entered run().
  drain_inbound(id);
  ctx.run();

  detail::current_shard_id = kInvalidShard;
  detail::current_runtime = nullptr;
}

auto Runtime::enqueue_cross_shard(shard_id source, shard_id target,
                                  QueueItem item) -> bool {
  if (source == target || target >= num_shards_ || source >= num_shards_) {
    return false;
  }
  auto &q_ptr = inbound_queues_[source][target];
  if (!q_ptr) {
    return false;
  }
  if (!q_ptr->push(std::move(item))) {
    return false;
  }
  pending_inbound_[target]->fetch_add(1, std::memory_order_release);
  schedule_drain(target);
  return true;
}

auto Runtime::schedule_drain(shard_id target) -> void {
  if (target >= num_shards_) {
    return;
  }
  bool expected = false;
  if (!drain_scheduled_[target]->compare_exchange_strong(
          expected, true, std::memory_order_acq_rel,
          std::memory_order_acquire)) {
    return;
  }
  boost::asio::post(shards_[target]->ctx().get_executor(),
                    [this, target]() { drain_inbound(target); });
}

auto Runtime::drain_inbound(shard_id target) -> void {
  if (target >= num_shards_) {
    return;
  }

  for (;;) {
    for (unsigned source = 0; source < num_shards_; ++source) {
      if (source == target) {
        continue;
      }
      auto &q_ptr = inbound_queues_[source][target];
      if (!q_ptr) {
        continue;
      }
      QueueItem item;
      while (q_ptr->pop(item)) {
        if (item) {
          pending_inbound_[target]->fetch_sub(1, std::memory_order_acquire);
          item->run(*this, target);
        }
        item.reset();
      }
    }

    drain_scheduled_[target]->store(false, std::memory_order_release);
    if (!has_pending_for_target(target)) {
      break;
    }
    bool expected = false;
    if (!drain_scheduled_[target]->compare_exchange_strong(
            expected, true, std::memory_order_acq_rel,
            std::memory_order_acquire)) {
      break;
    }
  }
}

auto Runtime::has_pending_for_target(shard_id target) const -> bool {
  return pending_inbound_[target]->load(std::memory_order_acquire) > 0;
}

auto Runtime::pending_cross_shard_queue_length() const -> std::size_t {
  std::size_t total = 0;
  for (unsigned source = 0; source < num_shards_; ++source) {
    for (unsigned target = 0; target < num_shards_; ++target) {
      if (source == target) {
        continue;
      }
      const auto &q_ptr = inbound_queues_[source][target];
      if (q_ptr) {
        total += q_ptr->read_available();
      }
    }
  }
  return total;
}

auto Runtime::stall_age_ms(shard_id id) const -> std::uint64_t {
  if (id >= num_shards_) {
    return 0;
  }
  const auto last_tick =
      shard_last_tick_ms_[id]->load(std::memory_order_acquire);
  const auto now_ms = now_monotonic_ms();
  return now_ms >= last_tick ? (now_ms - last_tick) : 0;
}

auto Runtime::start_heartbeat_on_shard(shard_id id) -> void {
  auto heartbeat = [](Runtime *self, shard_id s_id) -> spawn_task {
    constexpr auto kHeartbeatInterval = std::chrono::milliseconds(50);
    while (self->running_.load(std::memory_order_acquire)) {
      self->shard_last_tick_ms_[s_id]->store(now_monotonic_ms(),
                                             std::memory_order_release);
      co_await async_sleep(kHeartbeatInterval);
    }
    co_return;
  };
  spawn_on(id, heartbeat(this, id));
}

auto Runtime::start_stall_detection() -> void {
  for (unsigned i = 0; i < num_shards_; ++i) {
    shard_last_tick_ms_[i]->store(now_monotonic_ms(),
                                  std::memory_order_release);
  }

  stall_watchdog_thread_ = std::jthread([this](std::stop_token st) {
    constexpr auto kWatchdogPollInterval = std::chrono::milliseconds(100);
    constexpr std::uint64_t kStallThresholdMs = 200;
    std::vector<bool> warned(num_shards_, false);
    while (!st.stop_requested() && running_.load(std::memory_order_acquire)) {
      std::this_thread::sleep_for(kWatchdogPollInterval);
      if (!running_.load(std::memory_order_acquire)) {
        break;
      }

      for (unsigned i = 0; i < num_shards_; ++i) {
        const auto age_ms = stall_age_ms(i);
        if (age_ms > kStallThresholdMs) {
          if (!warned[i]) {
            log::error("Shard {} stalled ({} ms without heartbeat). Possible "
                       "blocking call in coroutine.",
                       i, age_ms);
            warned[i] = true;
          }
        } else {
          warned[i] = false;
        }
      }
    }
  });
}

auto Runtime::stop_stall_detection() -> void {
  if (stall_watchdog_thread_.joinable()) {
    stall_watchdog_thread_.request_stop();
    stall_watchdog_thread_.join();
  }
}

} // namespace dagforge

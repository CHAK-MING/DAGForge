#pragma once

#include "dagforge/core/memory.hpp"
#include "dagforge/core/coroutine.hpp"
#include "dagforge/core/error.hpp"
#include "dagforge/core/shard.hpp"

#include <boost/asio/bind_allocator.hpp>
#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/post.hpp>
#include <boost/lockfree/spsc_queue.hpp>

#include <algorithm>
#include <atomic>
#include <cassert>
#include <chrono>
#include <cstdint>
#include <functional>
#include <limits>
#include <memory>
#include <span>
#include <thread>
#include <vector>

namespace dagforge {

inline constexpr shard_id kInvalidShard = std::numeric_limits<shard_id>::max();

class Runtime {
public:
  explicit Runtime(unsigned num_shards = 0, bool pin_shards_to_cores = false,
                   unsigned cpu_affinity_offset = 0);
  ~Runtime() noexcept;

  Runtime(const Runtime &) = delete;
  Runtime &operator=(const Runtime &) = delete;

  [[nodiscard]] auto start() -> Result<void>;
  auto stop() noexcept -> void;
  [[nodiscard]] auto is_running() const noexcept -> bool;

  /// Launch an awaitable coroutine on the specified shard.  This is the
  /// preferred way to spawn work — replaces `coro.take() + schedule_on()`.
  template <typename T> auto spawn_on(shard_id target, task<T> coro) -> void {
    assert(target < num_shards_);
    submit_to_shard(
        target,
        [&]() {
          return std::make_unique<CrossShardThunk>(
              [coro = std::move(coro)](Runtime &rt, shard_id queued_target) mutable {
                Allocator alloc{rt.shards_[queued_target]->memory_resource()};
                co_spawn(rt.shards_[queued_target]->ctx().get_executor(),
                         std::move(coro),
                         boost::asio::bind_allocator(alloc, detached));
              });
        },
        [&]() {
          Allocator alloc{shards_[target]->memory_resource()};
          co_spawn(shards_[target]->ctx().get_executor(), std::move(coro),
                   boost::asio::bind_allocator(alloc, detached));
        });
  }

  /// Launch an awaitable coroutine on the current shard's executor.
  template <typename T> auto spawn(task<T> coro) -> void {
    auto sid = current_shard();
    if (sid == kInvalidShard)
      sid = 0;
    spawn_on(sid, std::move(coro));
  }

  /// Launch on shard 0 (for external callers).
  template <typename T> auto spawn_external(task<T> coro) -> void {
    auto target = static_cast<shard_id>(
        external_rr_.fetch_add(1, std::memory_order_relaxed) %
        std::max(1U, num_shards_));
    spawn_on(target, std::move(coro));
  }

  [[nodiscard]] auto shard_count() const noexcept -> unsigned {
    return num_shards_;
  }
  [[nodiscard]] auto pending_cross_shard_queue_length() const -> std::size_t;
  [[nodiscard]] auto stall_age_ms(shard_id id) const -> std::uint64_t;
  [[nodiscard]] auto pinned_cpu_for_shard(shard_id id) const noexcept -> int;

  [[nodiscard]] auto current_shard() const noexcept -> shard_id;
  [[nodiscard]] auto is_current_shard() const noexcept -> bool;
  [[nodiscard]] auto current_context() noexcept -> io::IoContext &;
  [[nodiscard]] auto shard(shard_id id) noexcept -> Shard & {
    assert(id < num_shards_);
    return *shards_[id];
  }

  /// Get the executor for a specific shard — useful for co_spawn.
  [[nodiscard]] auto executor_for(shard_id id)
      -> boost::asio::io_context::executor_type {
    assert(id < num_shards_);
    return shards_[id]->ctx().get_executor();
  }

  /// Post a callable to a specific shard's executor (fire-and-forget).
  /// The callable must be copyable or moveable and takes no arguments.
  template <typename F> auto post_to(shard_id target, F &&fn) -> void {
    assert(target < num_shards_);
    submit_to_shard(
        target,
        [&]() {
          return std::make_unique<CrossShardThunk>(
              [fn = std::forward<F>(fn)](Runtime &, shard_id) mutable { fn(); });
        },
        [&]() {
          boost::asio::post(shards_[target]->ctx().get_executor(),
                            std::forward<F>(fn));
        });
  }

  /// Post a copy of a callable to every shard's executor (Seastar-style
  /// invoke_on_all / broadcast).  The callable is copy-constructed once per
  /// shard so it must be cheaply copyable (e.g. capture by shared_ptr).
  template <typename F> auto broadcast_to_all_shards(F fn) -> void {
    for (unsigned i = 0; i < num_shards_; ++i) {
      boost::asio::post(shards_[i]->ctx().get_executor(), fn);
    }
  }

private:
  template <typename ThunkFactory, typename DirectFn>
  auto submit_to_shard(shard_id target, ThunkFactory &&make_thunk,
                       DirectFn &&direct_submit) -> void {
    if (is_current_shard()) {
      auto source = current_shard();
      if (source != kInvalidShard && source != target) {
        auto thunk = make_thunk();
        if (enqueue_cross_shard(source, target, std::move(thunk))) {
          return;
        }
        // Enqueue failed (queue full): run directly on target shard.
        thunk->run(*this, target);
        return;
      }
    }
    direct_submit();
  }

  struct CrossShardThunk {
    explicit CrossShardThunk(
        std::move_only_function<void(Runtime &, shard_id)> fn_)
        : fn(std::move(fn_)) {}

    auto run(Runtime &rt, shard_id target) -> void { fn(rt, target); }

    std::move_only_function<void(Runtime &, shard_id)> fn;
  };

  using QueueItem = CrossShardThunk *;
  // Cross-shard dispatch uses one SPSC mailbox per source/target pair.
  // A strand only serializes handlers already scheduled onto one executor;
  // it does not replace the cross-thread handoff we need here.
  using PairQueue =
      boost::lockfree::spsc_queue<QueueItem, boost::lockfree::capacity<4096>>;

  [[nodiscard]] auto enqueue_cross_shard(shard_id source, shard_id target,
                                         std::unique_ptr<CrossShardThunk> item)
      -> bool;
  auto schedule_drain(shard_id target) -> void;
  auto drain_inbound(shard_id target) -> void;
  [[nodiscard]] auto has_pending_for_target(shard_id target) const -> bool;
  auto run_shard(shard_id id) -> void;
  auto bind_shard_thread_to_cpu(shard_id id) -> void;
  auto start_stall_detection() -> void;
  auto stop_stall_detection() -> void;
  auto start_heartbeat_on_shard(shard_id id) -> void;

  alignas(64) std::atomic<bool> running_{false};
  bool pin_shards_to_cores_{false};
  unsigned cpu_affinity_offset_{0};
  unsigned num_shards_;
  std::vector<std::unique_ptr<Shard>> shards_;
  std::vector<std::optional<
      boost::asio::executor_work_guard<boost::asio::io_context::executor_type>>>
      work_guards_;
  std::vector<std::jthread> threads_;
  std::jthread stall_watchdog_thread_;
  std::vector<std::vector<std::unique_ptr<PairQueue>>> inbound_queues_;
  alignas(64) std::vector<std::unique_ptr<std::atomic<bool>>> drain_scheduled_;
  alignas(64)
      std::vector<std::unique_ptr<std::atomic<std::uint32_t>>> pending_inbound_;
  alignas(64) std::vector<
      std::unique_ptr<std::atomic<std::uint64_t>>> shard_last_tick_ms_;
  std::vector<int> pinned_cpus_;
  alignas(64) std::atomic<bool> stop_requested_{false};
  alignas(64) std::atomic<std::uint64_t> external_rr_{0};
};

// Thread-local for internal use only
namespace detail {
inline thread_local shard_id current_shard_id = kInvalidShard;
inline thread_local Runtime *current_runtime = nullptr;
inline thread_local pmr::memory_resource *override_memory_resource = nullptr;
} // namespace detail

// ---------------------------------------------------------------------------
// Free-function coroutine helpers
// ---------------------------------------------------------------------------

[[nodiscard]] inline auto current_io_context() noexcept -> io::IoContext & {
  assert(detail::current_runtime != nullptr);
  assert(detail::current_shard_id != kInvalidShard);
  return detail::current_runtime->current_context();
}

[[nodiscard]] inline auto current_memory_resource() noexcept
    -> pmr::memory_resource * {
  if (detail::override_memory_resource != nullptr) {
    return detail::override_memory_resource;
  }
  assert(detail::current_runtime != nullptr);
  assert(detail::current_shard_id != kInvalidShard);
  return detail::current_runtime->shard(detail::current_shard_id)
      .memory_resource();
}

[[nodiscard]] inline auto current_memory_resource_or_default() noexcept
    -> pmr::memory_resource * {
  if (detail::override_memory_resource != nullptr) {
    return detail::override_memory_resource;
  }
  if (detail::current_runtime == nullptr ||
      detail::current_shard_id == kInvalidShard) {
    return pmr::get_default_resource();
  }
  return detail::current_runtime->shard(detail::current_shard_id)
      .memory_resource();
}

class ScopedMemoryResourceOverride {
public:
  explicit ScopedMemoryResourceOverride(pmr::memory_resource *resource)
      : previous_(detail::override_memory_resource) {
    detail::override_memory_resource = resource;
  }

  ScopedMemoryResourceOverride(const ScopedMemoryResourceOverride &) = delete;
  auto operator=(const ScopedMemoryResourceOverride &)
      -> ScopedMemoryResourceOverride & = delete;

  ~ScopedMemoryResourceOverride() {
    detail::override_memory_resource = previous_;
  }

private:
  pmr::memory_resource *previous_;
};

template <typename Rep, typename Period>
[[nodiscard]] inline auto
async_sleep(std::chrono::duration<Rep, Period> duration) {
  return io::async_sleep(
      current_io_context(),
      std::chrono::duration_cast<std::chrono::nanoseconds>(duration));
}

} // namespace dagforge

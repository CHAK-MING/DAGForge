#include "bench_utils.hpp"

#include "dagforge/app/http/websocket.hpp"
#include "dagforge/core/runtime.hpp"

#include <benchmark/benchmark.h>

#include <atomic>
#include <cstdint>
#include <latch>
#include <memory>
#include <string>
#include <vector>

namespace dagforge {
namespace {

struct BroadcastContext {
  std::atomic<std::latch *> completion{nullptr};
};

class FakeWebSocketConnection final : public http::IWebSocketConnection {
public:
  FakeWebSocketConnection(int fd, std::shared_ptr<BroadcastContext> ctx)
      : fd_(fd), ctx_(std::move(ctx)) {}

  auto send_text(std::string) -> spawn_task override {
    auto *latch = ctx_->completion.load(std::memory_order_acquire);
    if (latch != nullptr) {
      latch->count_down();
    }
    co_return;
  }

  auto send_binary(std::vector<std::byte>) -> spawn_task override { co_return; }

  auto send_close() -> spawn_task override {
    closed_.store(true, std::memory_order_release);
    co_return;
  }

  auto send_pong(std::vector<std::byte>) -> spawn_task override { co_return; }

  auto handle_frames(std::move_only_function<void(http::WebSocketOpCode,
                                                  std::span<const std::byte>)>)
      -> spawn_task override {
    co_return;
  }

  [[nodiscard]] auto is_closed() const -> bool override {
    return closed_.load(std::memory_order_acquire);
  }

  [[nodiscard]] auto fd() const -> int override { return fd_; }
  auto force_close() -> void override {
    closed_.store(true, std::memory_order_release);
  }

private:
  int fd_{-1};
  std::shared_ptr<BroadcastContext> ctx_;
  std::atomic<bool> closed_{false};
};

auto add_fake_connections(http::WebSocketHub *hub, shard_id sid, int count,
                          std::shared_ptr<BroadcastContext> ctx,
                          std::latch *done) -> spawn_task {
  for (int i = 0; i < count; ++i) {
    const int fake_fd = static_cast<int>(sid) * 100000 + i + 1;
    hub->add_connection(
        std::make_shared<FakeWebSocketConnection>(fake_fd, ctx));
  }
  done->count_down();
  co_return;
}

auto run_broadcast_on_shard(http::WebSocketHub *hub,
                            http::WebSocketHub::LogMessage msg,
                            std::latch *submitted) -> spawn_task {
  hub->broadcast_log(std::move(msg));
  submitted->count_down();
  co_return;
}

[[nodiscard]] auto shard_connection_count(unsigned shards, int total,
                                          shard_id sid) -> int {
  const auto base = total / static_cast<int>(shards);
  const auto rem = total % static_cast<int>(shards);
  return base + (static_cast<int>(sid) < rem ? 1 : 0);
}

void BM_WebSocketHubBroadcastLogFanout(benchmark::State &state) {
  const auto shards = static_cast<unsigned>(state.range(0));
  const auto total_connections = static_cast<int>(state.range(1));
  if (shards == 0 || total_connections <= 0) {
    state.SkipWithError("requires shards > 0 and total_connections > 0");
    return;
  }

  bench::RuntimeGuard guard(shards);
  auto &runtime = guard.runtime;
  http::WebSocketHub hub(runtime);
  constexpr shard_id kBroadcastShard = 0;

  auto context = std::make_shared<BroadcastContext>();
  const http::WebSocketHub::LogMessage log_msg{
      .timestamp = "1",
      .dag_run_id = "bench_run",
      .task_id = "bench_task",
      .stream = "stdout",
      .content = "payload",
  };

  std::latch setup_done{shards};
  for (shard_id sid = 0; sid < shards; ++sid) {
    const int this_shard_count =
        shard_connection_count(shards, total_connections, sid);
    runtime.spawn_on(sid, add_fake_connections(&hub, sid, this_shard_count,
                                               context, &setup_done));
  }
  setup_done.wait();

  if (hub.connection_count() != static_cast<std::size_t>(total_connections)) {
    state.SkipWithError("failed to install expected fake connections");
    return;
  }

  for (auto _ : state) {
    std::latch done{total_connections};
    std::latch submitted{1};
    context->completion.store(&done, std::memory_order_release);

    runtime.spawn_on(kBroadcastShard,
                     run_broadcast_on_shard(&hub, log_msg, &submitted));

    submitted.wait();
    done.wait();
    context->completion.store(nullptr, std::memory_order_release);
  }

  state.SetItemsProcessed(static_cast<int64_t>(total_connections) *
                          state.iterations());
}

BENCHMARK(BM_WebSocketHubBroadcastLogFanout)
    ->Args({4, 1000})
    ->Args({4, 10000})
    ->Unit(benchmark::kMillisecond);

} // namespace
} // namespace dagforge

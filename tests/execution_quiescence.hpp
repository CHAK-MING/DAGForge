#pragma once


#include <chrono>
#include <thread>

#include "dagforge/app/application.hpp"
#include "dagforge/app/services/execution_service.hpp"
#include "dagforge/app/services/persistence_service.hpp"
#include "dagforge/app/services/scheduler_service.hpp"
#include "dagforge/core/runtime.hpp"
#include "dagforge/core/shard.hpp"

namespace dagforge::bench {

[[nodiscard]] inline auto wait_for_execution_quiescence(
    Runtime &runtime, ExecutionService &execution, int timeout_ms) -> bool {
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);

  while (std::chrono::steady_clock::now() < deadline) {
    const auto remaining_ms = static_cast<int>(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            deadline - std::chrono::steady_clock::now())
            .count());
    if (remaining_ms <= 0) {
      break;
    }

    sync_wait_on_runtime(runtime,
                         execution.wait_for_completion_async(remaining_ms));

    if (!execution.has_active_runs() && execution.coro_count() == 0 &&
        runtime.pending_cross_shard_queue_length() == 0) {
      return true;
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }

  return !execution.has_active_runs() && execution.coro_count() == 0 &&
         runtime.pending_cross_shard_queue_length() == 0;
}

} // namespace dagforge::bench

#pragma once

#include "dagforge/executor/executor.hpp"

#include <memory>
#include <unordered_map>

namespace dagforge {

class CompositeExecutor : public IExecutor {
public:
  CompositeExecutor() = default;
  ~CompositeExecutor() override = default;

  CompositeExecutor(const CompositeExecutor &) = delete;
  auto operator=(const CompositeExecutor &) -> CompositeExecutor & = delete;
  CompositeExecutor(CompositeExecutor &&) = delete;
  auto operator=(CompositeExecutor &&) -> CompositeExecutor & = delete;

  auto register_executor(ExecutorType type, std::unique_ptr<IExecutor> executor)
      -> void;

  auto start(ExecutorRequest req, ExecutionSink sink) -> Result<void> override;

  auto cancel(const InstanceId &instance_id) -> void override;

private:
  // Immutable after init — no lock needed.
  std::unordered_map<ExecutorType, std::unique_ptr<IExecutor>> executors_;
};

[[nodiscard]] auto create_composite_executor(Runtime &rt)
    -> std::unique_ptr<IExecutor>;

} // namespace dagforge

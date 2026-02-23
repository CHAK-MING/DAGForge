#include "dagforge/executor/composite_executor.hpp"
#include "dagforge/executor/executor.hpp"
#include "dagforge/util/log.hpp"
#include "dagforge/util/util.hpp"

namespace dagforge {

auto CompositeExecutor::register_executor(ExecutorType type,
                                          std::unique_ptr<IExecutor> executor)
    -> void {
  executors_[type] = std::move(executor);
}

auto CompositeExecutor::start(ExecutorRequest req, ExecutionSink sink)
    -> Result<void> {
  auto type = std::visit(
      overloaded{
          [](const ShellExecutorConfig &) { return ExecutorType::Shell; },
          [](const DockerExecutorConfig &) { return ExecutorType::Docker; },
          [](const SensorExecutorConfig &) { return ExecutorType::Sensor; }},
      req.config);

  auto it = executors_.find(type);
  if (it == executors_.end()) {
    log::error("CompositeExecutor: no executor registered for type {}",
               to_string_view(type));
    return fail(Error::InvalidArgument);
  }
  IExecutor *target_executor = it->second.get();

  return target_executor->start(std::move(req), std::move(sink));
}

auto CompositeExecutor::cancel(const InstanceId &instance_id) -> void {
  // Lock-free path: cancel is idempotent in child executors, so we can
  // broadcast directly instead of maintaining a synchronized instance->executor
  // map.
  for (auto &[_, exec] : executors_) {
    exec->cancel(instance_id);
  }
}

auto create_composite_executor(Runtime &rt) -> std::unique_ptr<IExecutor> {
  auto composite = std::make_unique<CompositeExecutor>();
  auto &factory = ExecutorFactory::instance();
  for (auto type : factory.registered_types()) {
    if (auto executor = factory.create(type, rt)) {
      composite->register_executor(type, std::move(executor));
    }
  }
  return composite;
}

} // namespace dagforge

#include "dagforge/executor/executor.hpp"

#include "dagforge/util/log.hpp"


namespace dagforge {

namespace {

class NoopExecutor final : public IExecutor {
public:
  explicit NoopExecutor(Runtime & /*rt*/) {}

  auto start(ExecutorRequest req, ExecutionSink sink) -> Result<void> override {
    const auto *noop = req.config.as<NoopExecutorConfig>();
    if (!noop) {
      return fail(Error::InvalidArgument);
    }

    ExecutorResult result = make_executor_result(
        req.memory_resource ? req.memory_resource.get()
                            : current_memory_resource_or_default());
    result.exit_code = noop->exit_code;
    if (sink.on_state) {
      sink.on_state(req.instance_id, "started");
    }
    if (sink.on_complete) {
      sink.on_complete(req.instance_id, std::move(result));
    }
    return ok();
  }

  auto cancel(const InstanceId &) -> void override {}
};

} // namespace

auto create_noop_executor(Runtime &rt) -> std::unique_ptr<IExecutor> {
  return std::make_unique<NoopExecutor>(rt);
}

} // namespace dagforge

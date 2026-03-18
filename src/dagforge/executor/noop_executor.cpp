#include "dagforge/executor/executor.hpp"

#include "dagforge/util/log.hpp"

namespace dagforge {

namespace {

class NoopExecutor final : public IExecutor {
public:
  explicit NoopExecutor(Runtime &rt) : runtime_(rt) {}

  auto start(ExecutorRequest req, ExecutionSink sink) -> Result<void> override {
    const auto *noop = req.config.as<NoopExecutorConfig>();
    if (!noop) {
      return fail(Error::InvalidArgument);
    }

    auto complete = [instance_id = req.instance_id.clone(),
                     exit_code = noop->exit_code,
                     resource = req.memory_resource, sink = std::move(sink)]()
                        mutable -> spawn_task {
      ExecutorResult result = make_executor_result(
          resource ? resource.get() : current_memory_resource_or_default());
      result.exit_code = exit_code;
      if (sink.on_state) {
        sink.on_state(instance_id, "started");
      }
      if (sink.on_complete) {
        sink.on_complete(instance_id, std::move(result));
      }
      co_return;
    };

    const auto target = runtime_.is_current_shard() ? runtime_.current_shard()
                                                    : shard_id{0};
    runtime_.spawn_on(target, complete());
    return ok();
  }

  auto cancel(const InstanceId &) -> void override {}

private:
  Runtime &runtime_;
};

} // namespace

auto create_noop_executor(Runtime &rt) -> std::unique_ptr<IExecutor> {
  return std::make_unique<NoopExecutor>(rt);
}

} // namespace dagforge

#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/core/error.hpp"
#include "dagforge/core/runtime.hpp"
#include "dagforge/executor/executor_types.hpp"
#include "dagforge/util/id.hpp"
#endif
#include "dagforge/util/log.hpp"

#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <optional>
#include <string_view>
#include <utility>
#include <vector>

#include <boost/asio/async_result.hpp>

namespace dagforge {

struct TaskConfig;

using ExecutorHeartbeatCallback =
    std::move_only_function<void(const InstanceId &instance_id)>;

struct ExecutorResult {
  int exit_code{0};
  pmr::string stdout_output{current_memory_resource_or_default()};
  pmr::string stderr_output{current_memory_resource_or_default()};
  pmr::string error{current_memory_resource_or_default()};
  bool timed_out{false};
  bool stdout_streamed{false};
  bool stderr_streamed{false};
};

[[nodiscard]] inline auto make_executor_result(
    pmr::memory_resource *resource = current_memory_resource_or_default())
    -> ExecutorResult {
  ExecutorResult result;
  result.stdout_output = pmr::string(resource);
  result.stderr_output = pmr::string(resource);
  result.error = pmr::string(resource);
  return result;
}

struct ExecutorRequest {
  InstanceId instance_id;
  std::string command;
  std::string working_dir;
  std::chrono::seconds execution_timeout{std::chrono::seconds(3600)};
  ExecutorConfig config;
  std::shared_ptr<pmr::memory_resource> memory_resource;

  [[nodiscard]] auto resource() const noexcept -> pmr::memory_resource * {
    return memory_resource != nullptr ? memory_resource.get()
                                      : current_memory_resource_or_default();
  }
};

struct ExecutionSink {
  ExecutorHeartbeatCallback on_heartbeat;
  std::move_only_function<void(const InstanceId &instance_id,
                               std::string_view message)>
      on_state;
  std::move_only_function<void(const InstanceId &instance_id,
                               std::string_view data)>
      on_stdout;
  std::move_only_function<void(const InstanceId &instance_id,
                               std::string_view data)>
      on_stderr;
  std::move_only_function<void(const InstanceId &instance_id,
                               ExecutorResult result)>
      on_complete;
};

class Runtime;
class IExecutor;

[[nodiscard]] auto create_shell_executor(Runtime &rt)
    -> std::unique_ptr<IExecutor>;

[[nodiscard]] auto create_docker_executor(Runtime &rt)
    -> std::unique_ptr<IExecutor>;

[[nodiscard]] auto create_sensor_executor(Runtime &rt)
    -> std::unique_ptr<IExecutor>;

[[nodiscard]] auto create_noop_executor(Runtime &rt)
    -> std::unique_ptr<IExecutor>;

class IExecutor {
public:
  virtual ~IExecutor() = default;

  virtual auto start(ExecutorRequest req, ExecutionSink sink)
      -> Result<void> = 0;

  virtual auto cancel(const InstanceId &instance_id) -> void = 0;
};

class ExecutorRegistry {
public:
  using Creator =
      std::move_only_function<std::unique_ptr<IExecutor>(Runtime &) const>;
  using ConfigBuilder =
      std::move_only_function<Result<ExecutorConfig>(const TaskConfig &) const>;
  using ConfigSerializer =
      std::move_only_function<std::string(const ExecutorConfig &) const>;
  using ConfigParser = std::move_only_function<Result<ExecutorConfig>(
      std::string_view persisted_config) const>;
  using TaskValidator =
      std::move_only_function<void(const TaskConfig &,
                                   std::vector<std::string> &) const>;

  static auto instance() -> ExecutorRegistry &;

  auto register_type(ExecutorType type, Creator creator,
                     ConfigBuilder builder,
                     ConfigSerializer serializer = {},
                     ConfigParser parser = {}, TaskValidator validator = {})
      -> void;

  [[nodiscard]] auto create(ExecutorType type, Runtime &rt) const
      -> std::unique_ptr<IExecutor>;

  [[nodiscard]] auto build_config(const TaskConfig &task) const
      -> Result<ExecutorConfig>;
  [[nodiscard]] auto serialize_config(const ExecutorConfig &config) const
      -> std::string;
  [[nodiscard]] auto parse_persisted_config(ExecutorType type,
                                            std::string_view persisted_config)
      const -> Result<ExecutorConfig>;
  auto validate_task(const TaskConfig &task,
                     std::vector<std::string> &errors) const -> void;

  [[nodiscard]] auto registered_types() const -> std::vector<ExecutorType>;

private:
  struct Entry {
    Creator creator;
    ConfigBuilder builder;
    ConfigSerializer serializer;
    ConfigParser parser;
    TaskValidator validator;
  };

  std::flat_map<ExecutorType, Entry> entries_;
};

template <typename T, typename... Args>
auto log_result_error(const Result<T> &result,
                      std::format_string<Args...> fmt, Args &&...args)
    -> const Result<T> & {
  if (!result) {
    log::error("{}: {}", std::format(fmt, std::forward<Args>(args)...),
               result.error().message());
  }
  return result;
}

inline auto execute_async(Runtime & /*runtime*/, IExecutor &executor,
                          ExecutorRequest req,
                          std::shared_ptr<pmr::memory_resource>
                              memory_resource = {},
                          std::move_only_function<void(std::string_view)>
                              on_stdout = {},
                          std::move_only_function<void(std::string_view)>
                              on_stderr = {},
                          ExecutorHeartbeatCallback on_heartbeat = {})
    -> task<ExecutorResult> {
  req.memory_resource = std::move(memory_resource);

  return boost::asio::async_initiate<const boost::asio::use_awaitable_t<>,
                                     void(ExecutorResult)>(
      [&executor, req = std::move(req), on_stdout = std::move(on_stdout),
       on_stderr = std::move(on_stderr),
       on_heartbeat = std::move(on_heartbeat)](auto handler) mutable {
        ExecutionSink sink;
        // Capture handler by shared_ptr so we can call it on start failure too.
        auto shared_h = std::make_shared<decltype(handler)>(std::move(handler));
        auto *resource = req.resource();
        if (on_stdout) {
          sink.on_stdout =
              [cb = std::move(on_stdout)](const InstanceId &,
                                          std::string_view data) mutable {
                cb(data);
              };
        }
        if (on_stderr) {
          sink.on_stderr =
              [cb = std::move(on_stderr)](const InstanceId &,
                                          std::string_view data) mutable {
                cb(data);
              };
        }
        if (on_heartbeat) {
          sink.on_heartbeat =
              [cb = std::move(on_heartbeat)](const InstanceId &id) mutable {
                cb(id);
              };
        }
        sink.on_complete = [shared_h](const InstanceId &,
                                      ExecutorResult res) mutable {
          std::move(*shared_h)(std::move(res));
        };

        auto start_res = executor.start(std::move(req), std::move(sink));
        if (!start_res) {
          // start() failed before scheduling; on_complete will never fire.
          ExecutorResult err_result = make_executor_result(resource);
          err_result.exit_code = 1;
          err_result.error =
              pmr::string(start_res.error().message(), resource);
          std::move(*shared_h)(std::move(err_result));
        }
      },
      boost::asio::use_awaitable);
}

inline auto execute_async(Runtime &runtime, IExecutor &executor,
                          InstanceId instance_id, ExecutorConfig config,
                          std::shared_ptr<pmr::memory_resource>
                              memory_resource = {},
                          std::move_only_function<void(std::string_view)>
                              on_stdout = {},
                          std::move_only_function<void(std::string_view)>
                              on_stderr = {},
                          ExecutorHeartbeatCallback on_heartbeat = {},
                          std::string command = {},
                          std::string working_dir = {},
                          std::chrono::seconds execution_timeout =
                              std::chrono::seconds(3600))
    -> task<ExecutorResult> {
  return execute_async(runtime, executor,
                       ExecutorRequest{.instance_id = std::move(instance_id),
                                       .command = std::move(command),
                                       .working_dir = std::move(working_dir),
                                       .execution_timeout = execution_timeout,
                                       .config = std::move(config),
                                       .memory_resource = {}},
                       std::move(memory_resource), std::move(on_stdout),
                       std::move(on_stderr), std::move(on_heartbeat));
}

} // namespace dagforge

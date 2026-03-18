#pragma once

#include "dagforge/core/memory.hpp"
#include "dagforge/core/error.hpp"
#include "dagforge/core/runtime.hpp"
#include "dagforge/util/enum.hpp"
#include "dagforge/util/id.hpp"
#include "dagforge/util/log.hpp"
#include <atomic>
#include <chrono>
#include <cstdint>
#include <flat_map>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <boost/asio/async_result.hpp>
#include <boost/describe/enum.hpp>

namespace dagforge {

struct TaskConfig;

enum class ExecutorType : std::uint8_t {
  Shell,
  Docker,
  Sensor,
  Noop,
};
BOOST_DESCRIBE_ENUM(ExecutorType, Shell, Docker, Sensor, Noop)

enum class ImagePullPolicy : std::uint8_t {
  Never,
  IfNotPresent,
  Always,
};
BOOST_DESCRIBE_ENUM(ImagePullPolicy, Never, IfNotPresent, Always)
DAGFORGE_DEFINE_ENUM_SERDE(ExecutorType, ExecutorType::Shell)
DAGFORGE_DEFINE_ENUM_SERDE(ImagePullPolicy, ImagePullPolicy::Never)

struct ShellExecutorConfig {
  std::string command;
  std::string working_dir;
  std::chrono::seconds execution_timeout{std::chrono::seconds(3600)};
  std::flat_map<std::string, std::string> env;
};

struct DockerExecutorConfig {
  std::string image;
  std::string command;
  std::string working_dir;
  std::chrono::seconds execution_timeout{std::chrono::seconds(3600)};
  std::flat_map<std::string, std::string> env;
  std::string docker_socket{"/var/run/docker.sock"};
  ImagePullPolicy pull_policy{ImagePullPolicy::Never};
};

enum class SensorType : std::uint8_t {
  File,
  Http,
  Command,
};
BOOST_DESCRIBE_ENUM(SensorType, File, Http, Command)
DAGFORGE_DEFINE_ENUM_SERDE(SensorType, SensorType::File)

struct SensorExecutorConfig {
  SensorType type{SensorType::File};
  std::string target;
  std::chrono::seconds poke_interval{std::chrono::seconds(30)};
  std::chrono::seconds execution_timeout{std::chrono::seconds(3600)};
  bool soft_fail{false};
  int expected_status{200};
  std::string http_method{"GET"};
};

struct NoopExecutorConfig {
  int exit_code{0};
};

template <typename T> struct ExecutorConfigTraits;

template <> struct ExecutorConfigTraits<ShellExecutorConfig> {
  static constexpr ExecutorType type = ExecutorType::Shell;
};

template <> struct ExecutorConfigTraits<DockerExecutorConfig> {
  static constexpr ExecutorType type = ExecutorType::Docker;
};

template <> struct ExecutorConfigTraits<SensorExecutorConfig> {
  static constexpr ExecutorType type = ExecutorType::Sensor;
};

template <> struct ExecutorConfigTraits<NoopExecutorConfig> {
  static constexpr ExecutorType type = ExecutorType::Noop;
};

class ExecutorConfig {
public:
  ExecutorConfig() : ExecutorConfig(ShellExecutorConfig{}) {}

  template <typename T>
  ExecutorConfig(T value)
      : payload_(std::make_shared<Model<T>>(std::move(value))) {}

  template <typename T> auto operator=(T value) -> ExecutorConfig & {
    payload_ = std::make_shared<Model<T>>(std::move(value));
    return *this;
  }

  [[nodiscard]] auto type() const noexcept -> ExecutorType {
    return payload_ != nullptr ? payload_->type() : ExecutorType::Shell;
  }

  template <typename T> [[nodiscard]] auto as() noexcept -> T * {
    auto *model = dynamic_cast<Model<T> *>(payload_.get());
    return model != nullptr ? &model->value : nullptr;
  }

  template <typename T> [[nodiscard]] auto as() const noexcept -> const T * {
    auto *model = dynamic_cast<const Model<T> *>(payload_.get());
    return model != nullptr ? &model->value : nullptr;
  }

private:
  struct Concept {
    virtual ~Concept() = default;
    [[nodiscard]] virtual auto type() const noexcept -> ExecutorType = 0;
  };

  template <typename T> struct Model final : Concept {
    explicit Model(T value_) : value(std::move(value_)) {}
    [[nodiscard]] auto type() const noexcept -> ExecutorType override {
      return ExecutorConfigTraits<T>::type;
    }
    T value;
  };

  std::shared_ptr<Concept> payload_;
};

inline constexpr int kExitCodeTimeout = 124;
inline constexpr int kExitCodeSkip = 100;
inline constexpr int kExitCodeImmediateFail = 101;

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
  ExecutorConfig config;
  std::shared_ptr<pmr::memory_resource> memory_resource;

  [[nodiscard]] auto resource() const noexcept -> pmr::memory_resource * {
    return memory_resource != nullptr ? memory_resource.get()
                                      : current_memory_resource_or_default();
  }
};

struct ExecutionSink {
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
                          InstanceId instance_id, ExecutorConfig config,
                          std::shared_ptr<pmr::memory_resource>
                              memory_resource = {},
                          std::move_only_function<void(std::string_view)>
                              on_stdout = {},
                          std::move_only_function<void(std::string_view)>
                              on_stderr = {})
    -> task<ExecutorResult> {
  ExecutorRequest req{.instance_id = std::move(instance_id),
                      .config = std::move(config),
                      .memory_resource = std::move(memory_resource)};

  auto result =
      co_await boost::asio::async_initiate<const boost::asio::use_awaitable_t<>,
                                           void(ExecutorResult)>(
          [&executor, req = std::move(req), on_stdout = std::move(on_stdout),
           on_stderr = std::move(on_stderr)](auto handler) mutable {
            ExecutionSink sink;
            // Capture handler by shared_ptr so we can call it on start failure
            // too.
            auto shared_h =
                std::make_shared<decltype(handler)>(std::move(handler));
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
            sink.on_complete = [shared_h](const InstanceId &,
                                          ExecutorResult res) mutable {
              std::move (*shared_h)(std::move(res));
            };

            auto start_res = executor.start(std::move(req), std::move(sink));
            if (!start_res) {
              // start() failed before scheduling; on_complete will never fire.
              ExecutorResult err_result = make_executor_result(resource);
              err_result.exit_code = 1;
              err_result.error =
                  pmr::string(start_res.error().message(), resource);
              std::move (*shared_h)(std::move(err_result));
            }
          },
          boost::asio::use_awaitable);

  co_return result;
}

} // namespace dagforge

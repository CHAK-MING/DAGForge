#pragma once

#include "dagforge/core/error.hpp"
#include "dagforge/core/runtime.hpp"
#include "dagforge/util/enum.hpp"
#include "dagforge/util/id.hpp"
#include <atomic>
#include <chrono>
#include <cstdint>
#include <flat_map>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

#include <boost/asio/async_result.hpp>
#include <boost/describe/enum.hpp>

namespace dagforge {

enum class ExecutorType : std::uint8_t {
  Shell,
  Docker,
  Sensor,
};
BOOST_DESCRIBE_ENUM(ExecutorType, Shell, Docker, Sensor)

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

using ExecutorConfig = std::variant<ShellExecutorConfig, DockerExecutorConfig,
                                    SensorExecutorConfig>;

inline constexpr int kExitCodeTimeout = 124;
inline constexpr int kExitCodeSkip = 100;
inline constexpr int kExitCodeImmediateFail = 101;

struct ExecutorResult {
  int exit_code{0};
  std::pmr::string stdout_output{current_memory_resource_or_default()};
  std::pmr::string stderr_output{current_memory_resource_or_default()};
  std::pmr::string error{current_memory_resource_or_default()};
  bool timed_out{false};
};

struct ExecutorRequest {
  InstanceId instance_id;
  ExecutorConfig config;
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

class IExecutor {
public:
  virtual ~IExecutor() = default;

  virtual auto start(ExecutorRequest req, ExecutionSink sink)
      -> Result<void> = 0;

  virtual auto cancel(const InstanceId &instance_id) -> void = 0;
};

class ExecutorFactory {
public:
  using Creator =
      std::move_only_function<std::unique_ptr<IExecutor>(Runtime &) const>;

  static auto instance() -> ExecutorFactory & {
    static ExecutorFactory factory;
    return factory;
  }

  auto register_creator(ExecutorType type, Creator creator) -> void {
    creators_[type] = std::move(creator);
  }

  [[nodiscard]] auto create(ExecutorType type, Runtime &rt) const
      -> std::unique_ptr<IExecutor> {
    auto it = creators_.find(type);
    if (it != creators_.end()) {
      return it->second(rt);
    }
    return nullptr;
  }

  [[nodiscard]] auto registered_types() const -> std::vector<ExecutorType> {
    auto types = creators_ | std::views::keys | std::ranges::to<std::vector>();
    return types;
  }

private:
  ExecutorFactory() {
    register_creator(ExecutorType::Shell,
                     [](Runtime &rt) { return create_shell_executor(rt); });
    register_creator(ExecutorType::Docker,
                     [](Runtime &rt) { return create_docker_executor(rt); });
    register_creator(ExecutorType::Sensor,
                     [](Runtime &rt) { return create_sensor_executor(rt); });
  }
  std::flat_map<ExecutorType, Creator> creators_;
};

inline auto execute_async(Runtime & /*runtime*/, IExecutor &executor,
                          InstanceId instance_id, ExecutorConfig config,
                          shard_id /*resume_shard*/ = kInvalidShard)
    -> task<ExecutorResult> {
  ExecutorRequest req{.instance_id = std::move(instance_id),
                      .config = std::move(config)};

  auto result =
      co_await boost::asio::async_initiate<const boost::asio::use_awaitable_t<>,
                                           void(ExecutorResult)>(
          [&executor, req = std::move(req)](auto handler) mutable {
            ExecutionSink sink;
            // Capture handler by shared_ptr so we can call it on start failure
            // too.
            auto shared_h =
                std::make_shared<decltype(handler)>(std::move(handler));
            sink.on_complete = [shared_h](const InstanceId &,
                                          ExecutorResult res) mutable {
              std::move (*shared_h)(std::move(res));
            };

            auto start_res = executor.start(std::move(req), std::move(sink));
            if (!start_res) {
              // start() failed before scheduling; on_complete will never fire.
              ExecutorResult err_result;
              err_result.exit_code = 1;
              err_result.error = std::pmr::string(start_res.error().message());
              std::move (*shared_h)(std::move(err_result));
            }
          },
          boost::asio::use_awaitable);

  co_return result;
}

} // namespace dagforge

#include "dagforge/client/docker/docker_client.hpp"
#include "dagforge/core/coroutine.hpp"
#include "dagforge/core/runtime.hpp"
#include "dagforge/executor/executor.hpp"
#include "dagforge/executor/executor_utils.hpp"
#include "dagforge/util/log.hpp"

#include <experimental/scope>
#include <format>
#include <memory>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace dagforge {

struct DockerShardState {
  struct ContainerInfo {
    std::string container_id;
    std::string socket_path;
  };
  std::unordered_map<InstanceId, ContainerInfo> containers;
  std::unordered_set<InstanceId> cancelled_instances;
};

struct DockerExecutionContext {
  DockerShardState *state{};

  auto register_container(const InstanceId &id, std::string container_id,
                          std::string socket_path) -> void {
    state->containers[id] =
        DockerShardState::ContainerInfo{.container_id = std::move(container_id),
                                        .socket_path = std::move(socket_path)};
  }

  auto unregister_container(const InstanceId &id) -> void {
    state->containers.erase(id);
  }

  [[nodiscard]] auto get_container(const InstanceId &id) const
      -> Result<DockerShardState::ContainerInfo> {
    auto it = state->containers.find(id);
    if (it != state->containers.end()) {
      return ok(it->second);
    }
    return fail(Error::NotFound);
  }

  auto mark_cancelled(const InstanceId &id) -> void {
    state->cancelled_instances.insert(id);
  }

  [[nodiscard]] auto is_cancelled(const InstanceId &id) const -> bool {
    return state->cancelled_instances.contains(id);
  }
};

namespace {

using RealDockerClient = docker::DockerClient<http::HttpClient>;

auto generate_container_name(const InstanceId &id) -> std::string {
  std::string name = "dagforge_" + std::string(id.value());
  std::replace(name.begin(), name.end(), ':', '_');
  std::replace(name.begin(), name.end(), '-', '_');
  return name;
}

auto finish(ExecutionSink &sink, const InstanceId &id, ExecutorResult res)
    -> void {
  if (sink.on_complete) {
    sink.on_complete(id, std::move(res));
  }
}

auto execute_docker_task(Runtime &runtime, DockerExecutorConfig config,
                         InstanceId inst_id, ExecutionSink sink,
                         DockerExecutionContext ctx) -> spawn_task {
  ExecutorResult result;

  auto client_result = co_await RealDockerClient::connect(
      runtime.current_context(), config.docker_socket);

  if (!client_result) {
    result.error = "Failed to connect to Docker daemon";
    result.exit_code = -1;
    finish(sink, inst_id, std::move(result));
    co_return;
  }

  auto client = std::move(*client_result);
  auto container_name = generate_container_name(inst_id);

  docker::ContainerConfig container_config{
      .image = config.image,
      .command = config.command,
      .working_dir = config.working_dir,
      .env = config.env,
  };

  if (config.pull_policy == ImagePullPolicy::Always) {
    auto pull_result = co_await client->pull_image(config.image);
    if (!pull_result) {
      result.error = std::format("Failed to pull image: {}",
                                 docker::to_string_view(pull_result.error()));
      result.exit_code = -1;
      finish(sink, inst_id, std::move(result));
      co_return;
    }
  }

  auto create_result =
      co_await client->create_container(container_config, container_name);

  if (!create_result &&
      create_result.error() == docker::DockerError::ImageNotFound &&
      config.pull_policy == ImagePullPolicy::IfNotPresent) {
    log::info("DockerExecutor: image not found, pulling {}", config.image);
    auto pull_result = co_await client->pull_image(config.image);
    if (!pull_result) {
      result.error = std::format("Failed to pull image: {}",
                                 docker::to_string_view(pull_result.error()));
      result.exit_code = -1;
      finish(sink, inst_id, std::move(result));
      co_return;
    }
    create_result =
        co_await client->create_container(container_config, container_name);
  }

  if (!create_result.has_value()) {
    result.error = std::format("Failed to create container: {}",
                               static_cast<int>(create_result.error()));
    result.exit_code = -1;
    finish(sink, inst_id, std::move(result));
    co_return;
  }

  auto container_id = create_result->id;
  log::info("DockerExecutor: created container {} for instance {}",
            container_id, inst_id);

  ctx.register_container(inst_id, container_id, config.docker_socket);
  std::experimental::scope_exit unregister{
      [state = ctx.state, inst_id] { state->containers.erase(inst_id); }};

  auto start_result = co_await client->start_container(container_id);
  if (!start_result) {
    result.error = "Failed to start container";
    result.exit_code = -1;
    (void)co_await client->remove_container(container_id, true);
    finish(sink, inst_id, std::move(result));
    co_return;
  }

  auto wait_result = co_await client->wait_container(container_id);

  if (ctx.is_cancelled(inst_id)) {
    result.error = "Task was cancelled";
    result.exit_code = -1;
    finish(sink, inst_id, std::move(result));
    co_return;
  }

  if (!wait_result) {
    result.error = "Failed to wait for container";
    result.exit_code = -1;
    (void)co_await client->remove_container(container_id, true);
    finish(sink, inst_id, std::move(result));
    co_return;
  }

  result.exit_code = wait_result->status_code;
  if (!wait_result->error.empty()) {
    result.error = wait_result->error;
  }

  auto logs_result = co_await client->get_logs(container_id);
  if (logs_result) {
    result.stdout_output = std::move(logs_result->stdout_output);
    result.stderr_output = std::move(logs_result->stderr_output);
  }

  (void)co_await client->remove_container(container_id, true);
  log::info("DockerExecutor: finished instance {} exit_code={}", inst_id,
            result.exit_code);

  finish(sink, inst_id, std::move(result));
}

} // namespace

class DockerExecutorImpl final : public IExecutor {
public:
  explicit DockerExecutorImpl(Runtime &rt)
      : runtime_(&rt), shard_states_(rt.shard_count()) {}

  DockerExecutorImpl(DockerExecutorImpl &&) noexcept = default;
  DockerExecutorImpl &operator=(DockerExecutorImpl &&) noexcept = default;
  DockerExecutorImpl(const DockerExecutorImpl &) = delete;
  DockerExecutorImpl &operator=(const DockerExecutorImpl &) = delete;

  auto start(ExecutorRequest req, ExecutionSink sink) -> Result<void> override {

    const auto *docker_config = std::get_if<DockerExecutorConfig>(&req.config);
    if (!docker_config) {
      if (sink.on_complete) {
        sink.on_complete(
            req.instance_id,
            ExecutorResult{.exit_code = -1,
                           .stdout_output = "",
                           .stderr_output = "",
                           .error = "Invalid configuration for Docker executor",
                           .timed_out = false});
      }
      return fail(Error::InvalidArgument);
    }

    log::info("DockerExecutor start: instance_id={} image={} cmd='{}'",
              req.instance_id, docker_config->image,
              cmd_preview(docker_config->command));

    auto owner = owner_shard(req.instance_id);
    auto t = execute_docker_task(
        *runtime_, *docker_config, req.instance_id, std::move(sink),
        DockerExecutionContext{.state = &shard_states_[owner]});
    runtime_->spawn_on(owner, std::move(t));
    return ok();
  }

  auto cancel(const InstanceId &instance_id) -> void override {
    auto owner = owner_shard(instance_id);
    runtime_->post_to(owner, [this, owner, instance_id] {
      DockerExecutionContext ctx{.state = &shard_states_[owner]};
      ctx.mark_cancelled(instance_id);
      auto container_res = ctx.get_container(instance_id);
      if (!container_res) {
        if (container_res.error() == make_error_code(Error::NotFound)) {
          log::warn("DockerExecutor: no active container for instance {}",
                    instance_id);
        } else {
          log::error(
              "DockerExecutor: error retrieving container for instance {}: {}",
              instance_id, container_res.error().message());
        }
        return;
      }

      log::info("DockerExecutor: cancelling container {} for instance {}",
                container_res->container_id, instance_id);

      auto cancel_task = [](Runtime &runtime, std::string container_id,
                            std::string socket_path) -> spawn_task {
        auto client_result = co_await RealDockerClient::connect(
            runtime.current_context(), socket_path);
        if (client_result) {
          auto stop_result =
              co_await (*client_result)
                  ->stop_container(container_id, std::chrono::seconds{5});
          if (!stop_result) {
            log::error(
                "Failed to stop container {}: {}", container_id,
                std::string{docker::to_string_view(stop_result.error())});
          }

          auto remove_result =
              co_await (*client_result)->remove_container(container_id, true);
          if (!remove_result) {
            log::error(
                "Failed to remove container {}: {}", container_id,
                std::string{docker::to_string_view(remove_result.error())});
          }
        }
        co_return;
      };

      runtime_->spawn_on(owner,
                         cancel_task(*runtime_, container_res->container_id,
                                     container_res->socket_path));
    });
  }

private:
  [[nodiscard]] auto owner_shard(const InstanceId &instance_id) const noexcept
      -> shard_id {
    const auto shards = std::max(1U, runtime_->shard_count());
    return static_cast<shard_id>(std::hash<InstanceId>{}(instance_id) % shards);
  }

  Runtime *runtime_;
  std::vector<DockerShardState> shard_states_;
};

auto create_docker_executor(Runtime &rt) -> std::unique_ptr<IExecutor> {
  return std::make_unique<DockerExecutorImpl>(rt);
}

} // namespace dagforge

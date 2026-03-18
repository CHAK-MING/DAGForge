#include "dagforge/client/docker/docker_client.hpp"
#include "dagforge/core/coroutine.hpp"
#include "dagforge/core/runtime.hpp"
#include "dagforge/executor/executor.hpp"
#include "dagforge/executor/executor_state.hpp"
#include "dagforge/executor/executor_utils.hpp"
#include "dagforge/util/log.hpp"

#include <experimental/scope>
#include <format>
#include <memory>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace dagforge {

struct ContainerHandle {
  std::string container_id;
  std::string socket_path;
};

using DockerShardState = ExecutorShardState<ContainerHandle>;

struct DockerExecutionContext {
  DockerShardState *state{};

  auto register_container(const InstanceId &id, std::string container_id,
                          std::string socket_path) -> void {
    state->register_active(
        id, ContainerHandle{.container_id = std::move(container_id),
                            .socket_path = std::move(socket_path)});
  }

  auto unregister_container(const InstanceId &id) -> void {
    state->unregister_active(id);
  }

  [[nodiscard]] auto get_container(const InstanceId &id) const -> Result<ContainerHandle> {
    if (auto handle = state->find_active(id)) {
      return ok(std::move(*handle));
    }
    return fail(Error::NotFound);
  }

  auto mark_cancelled(const InstanceId &id) -> void { state->mark_cancelled(id); }

  [[nodiscard]] auto is_cancelled(const InstanceId &id) const -> bool {
    return state->is_cancelled(id);
  }
};

namespace {

using RealDockerClient = docker::DockerClient<http::HttpClient>;

[[nodiscard]] auto docker_error_message(const std::error_code &error)
    -> std::string {
  return error.message();
}

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
                         pmr::memory_resource *resource,
                         DockerExecutionContext ctx) -> spawn_task {
  ExecutorResult result = make_executor_result(resource);

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
                                 docker_error_message(pull_result.error()));
      result.exit_code = -1;
      finish(sink, inst_id, std::move(result));
      co_return;
    }
  }

  auto create_result =
      co_await client->create_container(container_config, container_name);

  if (!create_result &&
      create_result.error() == make_error_code(docker::DockerError::ImageNotFound) &&
      config.pull_policy == ImagePullPolicy::IfNotPresent) {
    log::info("DockerExecutor: image not found, pulling {}", config.image);
    auto pull_result = co_await client->pull_image(config.image);
    if (!pull_result) {
      result.error = std::format("Failed to pull image: {}",
                                 docker_error_message(pull_result.error()));
      result.exit_code = -1;
      finish(sink, inst_id, std::move(result));
      co_return;
    }
    create_result =
        co_await client->create_container(container_config, container_name);
  }

  if (!create_result.has_value()) {
    result.error = std::format(
        "Failed to create container: {}",
        docker_error_message(create_result.error()));
    result.exit_code = -1;
    finish(sink, inst_id, std::move(result));
    co_return;
  }

  auto container_id = create_result->id;
  log::info("DockerExecutor: created container {} for instance {}",
            container_id, inst_id);

  ctx.register_container(inst_id, container_id, config.docker_socket);
  std::experimental::scope_exit unregister{
      [state = ctx.state, inst_id] { state->unregister_active(inst_id); }};

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
    const auto *docker_config = req.config.as<DockerExecutorConfig>();
    if (!docker_config) {
      if (sink.on_complete) {
        ExecutorResult result = make_executor_result(req.resource());
        result.exit_code = -1;
        result.error =
            pmr::string("Invalid configuration for Docker executor",
                             req.resource());
        sink.on_complete(req.instance_id, std::move(result));
      }
      return fail(Error::InvalidArgument);
    }

    log::info("DockerExecutor start: instance_id={} image={} cmd='{}'",
              req.instance_id, docker_config->image,
              cmd_preview(docker_config->command));

    auto sid = runtime_->is_current_shard() ? runtime_->current_shard() : 0;
    auto t = execute_docker_task(
        *runtime_, *docker_config, req.instance_id, std::move(sink),
        req.resource(),
        DockerExecutionContext{.state = &shard_states_[sid]});
    runtime_->spawn(std::move(t));
    return ok();
  }

  auto cancel(const InstanceId &instance_id) -> void override {
    cancel_on_all_shards(*runtime_, shard_states_, instance_id,
                         [this, instance_id](DockerShardState &state,
                                             const InstanceId &id) {
                           DockerExecutionContext ctx{.state = &state};
                           ctx.mark_cancelled(id);
                           auto container_res = ctx.get_container(id);
                           if (!container_res) {
                             if (container_res.error() !=
                                 make_error_code(Error::NotFound)) {
                               log::error(
                                   "DockerExecutor: error retrieving container for instance {}: {}",
                                   instance_id, container_res.error().message());
                             }
                             return;
                           }

                           log::info(
                               "DockerExecutor: cancelling container {} for instance {}",
                               container_res->container_id, id);

                           auto cancel_task = [](Runtime &runtime,
                                                 std::string container_id,
                                                 std::string socket_path)
                               -> spawn_task {
                             auto client_result = co_await RealDockerClient::connect(
                                 runtime.current_context(), socket_path);
                             if (client_result) {
                               auto stop_result =
                                   co_await (*client_result)
                                       ->stop_container(container_id,
                                                        std::chrono::seconds{5});
                               if (!stop_result) {
                                 log::error("Failed to stop container {}: {}",
                                            container_id,
                                            docker_error_message(
                                                stop_result.error()));
                               }

                               auto remove_result = co_await (*client_result)
                                                        ->remove_container(
                                                            container_id, true);
                               if (!remove_result) {
                                 log::error(
                                     "Failed to remove container {}: {}",
                                     container_id,
                                     docker_error_message(
                                         remove_result.error()));
                               }
                             }
                             co_return;
                           };

                           auto sid = runtime_->is_current_shard()
                                          ? runtime_->current_shard()
                                          : 0;
                           runtime_->spawn_on(
                               sid, cancel_task(*runtime_,
                                                container_res->container_id,
                                                container_res->socket_path));
                         });
  }

private:
  Runtime *runtime_;
  std::vector<DockerShardState> shard_states_;
};

auto create_docker_executor(Runtime &rt) -> std::unique_ptr<IExecutor> {
  return std::make_unique<DockerExecutorImpl>(rt);
}

} // namespace dagforge

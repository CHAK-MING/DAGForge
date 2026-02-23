#include "dagforge/cli/commands.hpp"
#include "dagforge/cli/management_client.hpp"

#include "dagforge/config/config.hpp"
#include "dagforge/config/dag_definition.hpp"
#include "dagforge/executor/composite_executor.hpp"
#include "dagforge/util/json.hpp"
#include "dagforge/util/log.hpp"
#include "dagforge/util/time.hpp"

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/use_future.hpp>

#include <chrono>
#include <filesystem>
#include <format>
#include <print>

namespace dagforge::cli {
namespace {

auto to_executor_config(const TaskConfig &task) -> ExecutorConfig {
  switch (task.executor) {
  case ExecutorType::Shell: {
    ShellExecutorConfig exec;
    exec.command = task.command;
    exec.working_dir = task.working_dir;
    exec.execution_timeout = task.execution_timeout;
    return exec;
  }
  case ExecutorType::Docker: {
    DockerExecutorConfig exec;
    exec.command = task.command;
    exec.working_dir = task.working_dir;
    exec.execution_timeout = task.execution_timeout;
    if (const auto *docker_cfg =
            std::get_if<DockerTaskConfig>(&task.executor_config)) {
      exec.image = docker_cfg->image;
      exec.docker_socket = docker_cfg->socket;
      exec.pull_policy = docker_cfg->pull_policy;
    }
    return exec;
  }
  case ExecutorType::Sensor: {
    SensorExecutorConfig exec;
    exec.execution_timeout = task.execution_timeout;
    if (const auto *sensor_cfg =
            std::get_if<SensorTaskConfig>(&task.executor_config)) {
      exec.type = sensor_cfg->type;
      exec.target = sensor_cfg->target;
      exec.poke_interval = sensor_cfg->poke_interval;
      exec.soft_fail = sensor_cfg->soft_fail;
      exec.expected_status = sensor_cfg->expected_status;
      exec.http_method = sensor_cfg->http_method;
    }
    return exec;
  }
  }
  ShellExecutorConfig fallback;
  fallback.command = task.command;
  fallback.working_dir = task.working_dir;
  fallback.execution_timeout = task.execution_timeout;
  return fallback;
}

auto load_dag(const Config &config, const DAGId &dag_id) -> Result<DAGInfo> {
  if (!config.dag_source.directory.empty()) {
    const auto dag_file = std::filesystem::path(config.dag_source.directory) /
                          std::format("{}.toml", dag_id.str());
    if (std::filesystem::exists(dag_file)) {
      return DAGDefinitionLoader::load_from_file(dag_file.string());
    }
  }

  ManagementClient client(config.database);
  if (auto r = client.open(); !r) {
    return fail(r.error());
  }
  return client.get_dag(dag_id);
}

} // namespace

auto cmd_test_task(const TestTaskOptions &opts) -> int {
  log::set_output_stderr();
  auto config_res = ConfigLoader::load_from_file(opts.config_file);
  if (!config_res) {
    std::println(stderr, "Error: {}", config_res.error().message());
    return 1;
  }

  auto dag_res = load_dag(*config_res, DAGId{opts.dag_id});
  if (!dag_res) {
    std::println(stderr, "Error: DAG '{}' not found: {}", opts.dag_id,
                 dag_res.error().message());
    return 1;
  }

  auto *task = dag_res->find_task(TaskId{opts.task_id});
  if (!task) {
    std::println(stderr, "Error: task '{}' not found in DAG '{}'", opts.task_id,
                 opts.dag_id);
    return 1;
  }

  Runtime runtime(config_res->scheduler.shards);
  if (auto r = runtime.start(); !r) {
    std::println(stderr, "Error: failed to start runtime: {}",
                 r.error().message());
    return 1;
  }

  auto executor = create_composite_executor(runtime);
  const auto now = std::chrono::system_clock::now();
  const auto iid = std::format("test_{}_{}_{}", opts.dag_id, opts.task_id,
                               util::to_unix_millis(now));

  auto fut = boost::asio::co_spawn(
      runtime.shard(0).ctx(),
      execute_async(runtime, *executor, InstanceId{iid},
                    to_executor_config(*task), shard_id{0}),
      boost::asio::use_future);

  ExecutorResult result;
  try {
    result = fut.get();
  } catch (const std::exception &e) {
    runtime.stop();
    std::println(stderr, "Error: task test execution failed: {}", e.what());
    return 1;
  }
  runtime.stop();

  if (opts.json) {
    JsonValue out{
        {"dag_id", opts.dag_id},
        {"task_id", opts.task_id},
        {"instance_id", iid},
        {"exit_code", result.exit_code},
        {"timed_out", result.timed_out},
        {"status", result.exit_code == 0 ? "success" : "failed"},
        {"stdout", std::string(result.stdout_output)},
        {"stderr", std::string(result.stderr_output)},
        {"error", std::string(result.error)},
    };
    std::println("{}", dump_json(out));
    return result.exit_code == 0 ? 0 : 1;
  }

  std::println("Task test finished:");
  std::println("  DAG:        {}", opts.dag_id);
  std::println("  Task:       {}", opts.task_id);
  std::println("  Instance:   {}", iid);
  std::println("  Exit code:  {}", result.exit_code);
  std::println("  Timed out:  {}", result.timed_out ? "yes" : "no");
  if (!result.error.empty()) {
    std::println("  Error:      {}", std::string(result.error));
  }
  if (!result.stdout_output.empty()) {
    std::println("\nstdout:\n{}", std::string(result.stdout_output));
  }
  if (!result.stderr_output.empty()) {
    std::println("\nstderr:\n{}", std::string(result.stderr_output));
  }

  return result.exit_code == 0 ? 0 : 1;
}

} // namespace dagforge::cli

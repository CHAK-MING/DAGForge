#include "dagforge/client/http/http_client.hpp"
#include "dagforge/client/http/http_types.hpp"
#include "dagforge/core/asio_awaitable.hpp"
#include "dagforge/core/coroutine.hpp"
#include "dagforge/core/error.hpp"
#include "dagforge/core/runtime.hpp"
#include "dagforge/executor/executor.hpp"
#include "dagforge/executor/executor_state.hpp"
#include "dagforge/executor/process_launch.hpp"
#include "dagforge/executor/process_management.hpp"
#include "dagforge/util/log.hpp"
#include "dagforge/util/url.hpp"

#include <boost/algorithm/string/predicate.hpp>
#include <boost/asio/cancel_after.hpp>
#include <boost/process/v2/process.hpp>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <optional>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace dagforge {

namespace {

namespace bp = boost::process::v2;

using SensorShardState = ExecutorShardState<ActiveProcess>;

struct SensorContext {
  SensorShardState *state{};
};

auto check_file_sensor(const std::string &path) -> Result<bool> {
  std::error_code ec;
  bool exists = std::filesystem::exists(path, ec);
  if (ec) {
    return fail(ec);
  }
  return ok(exists);
}

[[nodiscard]] auto parse_http_method(std::string_view method) noexcept
    -> http::HttpMethod {
  using http::HttpMethod;
  static constexpr std::array<std::pair<std::string_view, HttpMethod>, 6>
      kMethods{{
          {"POST", HttpMethod::POST},
          {"PUT", HttpMethod::PUT},
          {"DELETE", HttpMethod::DELETE},
          {"PATCH", HttpMethod::PATCH},
          {"OPTIONS", HttpMethod::OPTIONS},
          {"HEAD", HttpMethod::HEAD},
      }};
  for (auto [name, value] : kMethods) {
    if (boost::algorithm::iequals(method, name))
      return value;
  }
  return HttpMethod::GET;
}

auto is_cancelled(const InstanceId &instance_id, const SensorContext &ctx)
    -> bool {
  return ctx.state->consume_cancelled(instance_id);
}

auto complete_cancelled(const InstanceId &instance_id, ExecutionSink &sink)
    -> void {
  if (sink.on_complete) {
    ExecutorResult result;
    auto *resource = result.error.get_allocator().resource();
    result.exit_code = 1;
    result.error = pmr::string("Sensor cancelled", resource);
    sink.on_complete(instance_id, std::move(result));
  }
}

auto make_result(pmr::memory_resource *resource) -> ExecutorResult {
  return make_executor_result(resource);
}

auto wait_for_command_exit(bp::process &proc,
                           std::chrono::steady_clock::duration timeout)
    -> task<ProcessWaitResult> {
  auto [ec, exit_code] =
      co_await proc.async_wait(boost::asio::cancel_after(timeout, use_nothrow));
  if (!ec) {
    co_return ProcessWaitResult{.exit_code = exit_code};
  }
  if (ec == boost::asio::error::operation_aborted) {
    co_return co_await terminate_and_reap_process(proc, true);
  }
  co_return ProcessWaitResult{.error = ec};
}

auto run_file_sensor(SensorExecutorConfig config, InstanceId instance_id,
                     ExecutionSink sink, SensorContext ctx,
                     pmr::memory_resource *resource) -> spawn_task {
  auto start_time = std::chrono::steady_clock::now();
  auto deadline = start_time + config.execution_timeout;

  while (std::chrono::steady_clock::now() < deadline) {
    if (is_cancelled(instance_id, ctx)) {
      complete_cancelled(instance_id, sink);
      co_return;
    }

    if (sink.on_state) {
      sink.on_state(instance_id, "Checking file: " + config.target);
    }

    auto exists_res = check_file_sensor(config.target);
    if (exists_res.value_or(false)) {
      if (sink.on_complete) {
        auto result = make_result(resource);
        result.exit_code = 0;
        result.stdout_output =
            pmr::string("File exists: " + config.target, resource);
        sink.on_complete(instance_id, std::move(result));
      }
      co_return;
    } else if (!exists_res) {
      log::warn("File sensor error for {}: {}", config.target,
                exists_res.error().message());
    }

    co_await async_sleep(config.poke_interval);
  }

  if (sink.on_complete) {
    auto result = make_result(resource);
    result.exit_code = config.soft_fail ? 100 : 1;
    result.error = pmr::string("File sensor execution_timeout", resource);
    result.timed_out = true;
    sink.on_complete(instance_id, std::move(result));
  }
}

auto run_command_sensor(SensorExecutorConfig config, InstanceId instance_id,
                        ExecutionSink sink, SensorContext ctx,
                        pmr::memory_resource *resource) -> spawn_task {
  auto start_time = std::chrono::steady_clock::now();
  auto deadline = start_time + config.execution_timeout;

  while (std::chrono::steady_clock::now() < deadline) {
    if (is_cancelled(instance_id, ctx)) {
      complete_cancelled(instance_id, sink);
      co_return;
    }

    if (sink.on_state) {
      sink.on_state(instance_id, "Running command sensor");
    }

    std::optional<bp::process> proc;
    try {
      auto &io_ctx = current_io_context();
      ProcessLaunchSpec spec{
          .args = {"-c", config.target},
          .stdio = std::nullopt,
          .env = std::nullopt,
          .working_dir = {}};
      proc.emplace(launch_shell_process(io_ctx, std::move(spec)));
    } catch (const std::exception &ex) {
      log::error("Command sensor spawn failed for instance {}: {}", instance_id,
                 ex.what());
      if (sink.on_complete) {
        auto result = make_result(resource);
        result.exit_code = 1;
        result.error = pmr::string(
            std::format("Command sensor spawn failed: {}", ex.what()),
            resource);
        sink.on_complete(instance_id, std::move(result));
      }
      co_return;
    }

    ctx.state->register_active(instance_id, ActiveProcess{.pid = proc->id()});

    const auto now = std::chrono::steady_clock::now();
    if (now >= deadline) {
      break;
    }
    auto wait_result = co_await wait_for_command_exit(*proc, deadline - now);
    ctx.state->unregister_active(instance_id);

    if (is_cancelled(instance_id, ctx)) {
      complete_cancelled(instance_id, sink);
      co_return;
    }
    if (wait_result.error) {
      if (sink.on_complete) {
        auto result = make_result(resource);
        result.exit_code = 1;
        result.error = pmr::string(
            std::format("Command sensor wait failed: {}",
                        wait_result.error.message()),
            resource);
        sink.on_complete(instance_id, std::move(result));
      }
      co_return;
    }
    if (wait_result.timed_out) {
      break;
    }

    if (wait_result.exit_code == 0) {
      if (sink.on_complete) {
        auto result = make_result(resource);
        result.exit_code = 0;
        result.stdout_output = pmr::string("Command succeeded", resource);
        sink.on_complete(instance_id, std::move(result));
      }
      co_return;
    }

    co_await async_sleep(config.poke_interval);
  }

  if (sink.on_complete) {
    auto result = make_result(resource);
    result.exit_code = config.soft_fail ? 100 : 1;
    result.error =
        pmr::string("Command sensor execution_timeout", resource);
    result.timed_out = true;
    sink.on_complete(instance_id, std::move(result));
  }
}

auto run_http_sensor(SensorExecutorConfig config, InstanceId instance_id,
                     ExecutionSink sink, SensorContext ctx,
                     pmr::memory_resource *resource) -> spawn_task {
  auto start_time = std::chrono::steady_clock::now();
  auto deadline = start_time + config.execution_timeout;

  auto parsed_res = util::parse_http_url(config.target);
  if (!parsed_res) {
    if (sink.on_complete) {
      auto result = make_result(resource);
      result.exit_code = 1;
      result.error = pmr::string(
          std::format("Invalid http sensor url: {}",
                      parsed_res.error().message()),
          resource);
      sink.on_complete(instance_id, std::move(result));
    }
    co_return;
  }
  auto &parsed = *parsed_res;

  const auto method = parse_http_method(config.http_method);
  const auto expected = static_cast<uint16_t>(config.expected_status);

  // Persistent connection outside loop for Keep-Alive optimization
  std::unique_ptr<http::HttpClient> client;

  while (std::chrono::steady_clock::now() < deadline) {
    if (is_cancelled(instance_id, ctx)) {
      complete_cancelled(instance_id, sink);
      co_return;
    }

    if (sink.on_state) {
      sink.on_state(instance_id,
                    std::format("Waiting for http {} {} (expect {})", method,
                                parsed.path, expected));
    }

    // Lazy connection: only connect if client is null or disconnected
    if (!client || !client->is_connected()) {
      auto client_res = co_await http::HttpClient::connect_tcp(
          current_io_context(), parsed.host, parsed.port);
      if (!client_res) {
        log::debug("HTTP sensor connection failed for {}: {}, retrying...",
                   config.target, client_res.error().message());
        co_await async_sleep(std::chrono::milliseconds(100));
        continue;
      }
      client = std::move(*client_res);
    }

    // Graceful recovery: wrap request in try-catch to handle EOF/Broken Pipe
    bool request_failed = false;
    http::HttpResponse resp;
    try {
      http::HttpRequest req;
      req.method = method;
      req.path = parsed.path;
      resp = co_await client->request(std::move(req));
    } catch (const std::exception &ex) {
      log::debug(
          "HTTP sensor request failed (likely EOF/connection reset): {}, "
          "resetting connection...",
          ex.what());
      client.reset(); // Reset client to trigger reconnection on next iteration
      request_failed = true;
    }

    if (!request_failed && static_cast<uint16_t>(resp.status) == expected) {
      if (sink.on_complete) {
        auto result = make_result(resource);
        result.exit_code = 0;
        result.stdout_output = pmr::string(
            std::format("HTTP {} {} returned {}", method, parsed.path,
                        static_cast<uint16_t>(resp.status)),
            resource);
        sink.on_complete(instance_id, std::move(result));
      }
      co_return;
    }

    co_await async_sleep(config.poke_interval);
  }

  if (sink.on_complete) {
    auto result = make_result(resource);
    result.exit_code = config.soft_fail ? 100 : 1;
    result.error = pmr::string("HTTP sensor execution_timeout", resource);
    result.timed_out = true;
    sink.on_complete(instance_id, std::move(result));
  }
}

} // namespace

class SensorExecutor : public IExecutor {
public:
  explicit SensorExecutor(Runtime &runtime)
      : runtime_(&runtime), shard_states_(runtime.shard_count()) {}

  auto start(ExecutorRequest req, ExecutionSink sink) -> Result<void> override {
    auto *config = req.config.as<SensorExecutorConfig>();
    if (!config) {
      return fail(Error::InvalidArgument);
    }

    log::info("SensorExecutor start: instance_id={} type={} target={}",
              req.instance_id,
              config->type == SensorType::File   ? "file"
              : config->type == SensorType::Http ? "http"
                                                 : "command",
              config->target);

    auto sid = runtime_->is_current_shard() ? runtime_->current_shard() : 0;
    SensorContext ctx{.state = &shard_states_[sid]};
    switch (config->type) {
    case SensorType::File: {
      runtime_->spawn(run_file_sensor(*config, req.instance_id, std::move(sink),
                                      ctx, req.resource()));
      break;
    }
    case SensorType::Http: {
      runtime_->spawn(run_http_sensor(*config, req.instance_id, std::move(sink),
                                      ctx, req.resource()));
      break;
    }
    case SensorType::Command: {
      runtime_->spawn(run_command_sensor(*config, req.instance_id,
                                         std::move(sink), ctx, req.resource()));
      break;
    }
    }
    return ok();
  }

  auto cancel(const InstanceId &instance_id) -> void override {
    cancel_on_all_shards(*runtime_, shard_states_, instance_id,
                         [](SensorShardState &state, const InstanceId &id) {
                           state.mark_cancelled(id);
                           auto it = state.find_active_mut(id);
                           if (it != state.active_end() && it->second.pid > 0) {
                             kill_process_group_or_process(it->second.pid);
                           }
                           log::info("SensorExecutor cancel: instance_id={}", id);
                         });
  }

private:
  Runtime *runtime_;
  std::vector<SensorShardState> shard_states_;
};

auto create_sensor_executor(Runtime &rt) -> std::unique_ptr<IExecutor> {
  return std::make_unique<SensorExecutor>(rt);
}

} // namespace dagforge

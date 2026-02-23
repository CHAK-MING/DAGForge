#include "dagforge/client/http/http_client.hpp"
#include "dagforge/client/http/http_types.hpp"
#include "dagforge/core/asio_awaitable.hpp"
#include "dagforge/core/coroutine.hpp"
#include "dagforge/core/error.hpp"
#include "dagforge/core/runtime.hpp"
#include "dagforge/executor/executor.hpp"
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

#include <signal.h>

namespace dagforge {

namespace {

namespace bp = boost::process::v2;

struct SensorContext {
  struct SensorShardState *state{};
};

struct SensorShardState {
  std::unordered_set<InstanceId> cancelled;
  struct ActiveProcess {
    pid_t pid{-1};
  };
  std::unordered_map<InstanceId, ActiveProcess> active_commands;
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
  if (ctx.state->cancelled.contains(instance_id)) {
    ctx.state->cancelled.erase(instance_id);
    return true;
  }
  return false;
}

auto complete_cancelled(const InstanceId &instance_id, ExecutionSink &sink)
    -> void {
  if (sink.on_complete) {
    sink.on_complete(
        instance_id,
        ExecutorResult{.exit_code = 1,
                       .stdout_output = std::pmr::string(""),
                       .stderr_output = std::pmr::string(""),
                       .error = std::pmr::string("Sensor cancelled"),
                       .timed_out = false});
  }
}

struct CommandWaitResult {
  int exit_code{-1};
  bool timed_out{false};
};

auto wait_for_command_exit(bp::process &proc,
                           std::chrono::steady_clock::duration timeout)
    -> task<CommandWaitResult>;

auto wait_for_command_exit(bp::process &proc) -> task<CommandWaitResult> {
  auto [ec, exit_code] = co_await proc.async_wait(use_nothrow);
  if (ec) {
    co_return CommandWaitResult{.exit_code = -1, .timed_out = false};
  }
  co_return CommandWaitResult{.exit_code = exit_code, .timed_out = false};
}

auto wait_for_command_exit(bp::process &proc,
                           std::chrono::steady_clock::duration timeout)
    -> task<CommandWaitResult> {
  auto [ec, exit_code] =
      co_await proc.async_wait(boost::asio::cancel_after(timeout, use_nothrow));
  if (!ec) {
    co_return CommandWaitResult{.exit_code = exit_code, .timed_out = false};
  }
  if (ec == boost::asio::error::operation_aborted) {
    (void)::kill(proc.id(), SIGKILL);
    auto killed = co_await wait_for_command_exit(proc);
    killed.timed_out = true;
    co_return killed;
  }
  co_return CommandWaitResult{.exit_code = -1, .timed_out = false};
}

auto run_file_sensor(SensorExecutorConfig config, InstanceId instance_id,
                     ExecutionSink sink, SensorContext ctx) -> spawn_task {
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
        sink.on_complete(instance_id,
                         ExecutorResult{.exit_code = 0,
                                        .stdout_output = std::pmr::string(
                                            "File exists: " + config.target),
                                        .stderr_output = std::pmr::string(""),
                                        .error = std::pmr::string(""),
                                        .timed_out = false});
      }
      co_return;
    } else if (!exists_res) {
      log::warn("File sensor error for {}: {}", config.target,
                exists_res.error().message());
    }

    co_await async_sleep(config.poke_interval);
  }

  if (sink.on_complete) {
    sink.on_complete(instance_id,
                     ExecutorResult{.exit_code = config.soft_fail ? 100 : 1,
                                    .stdout_output = std::pmr::string(""),
                                    .stderr_output = std::pmr::string(""),
                                    .error = std::pmr::string(
                                        "File sensor execution_timeout"),
                                    .timed_out = true});
  }
}

auto run_command_sensor(SensorExecutorConfig config, InstanceId instance_id,
                        ExecutionSink sink, SensorContext ctx) -> spawn_task {
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
      proc.emplace(io_ctx, "/bin/sh",
                   std::initializer_list<std::string>{"-c", config.target});
    } catch (const std::exception &ex) {
      log::error("Command sensor spawn failed for instance {}: {}", instance_id,
                 ex.what());
      if (sink.on_complete) {
        sink.on_complete(
            instance_id,
            ExecutorResult{.exit_code = 1,
                           .stdout_output = std::pmr::string(""),
                           .stderr_output = std::pmr::string(""),
                           .error = std::pmr::string(std::format(
                               "Command sensor spawn failed: {}", ex.what())),
                           .timed_out = false});
      }
      co_return;
    }

    ctx.state->active_commands[instance_id] = {.pid = proc->id()};

    const auto now = std::chrono::steady_clock::now();
    if (now >= deadline) {
      break;
    }
    auto wait_result = co_await wait_for_command_exit(*proc, deadline - now);
    ctx.state->active_commands.erase(instance_id);

    if (is_cancelled(instance_id, ctx)) {
      complete_cancelled(instance_id, sink);
      co_return;
    }
    if (wait_result.timed_out) {
      break;
    }

    if (wait_result.exit_code == 0) {
      if (sink.on_complete) {
        sink.on_complete(instance_id,
                         ExecutorResult{.exit_code = 0,
                                        .stdout_output = std::pmr::string(
                                            "Command succeeded"),
                                        .stderr_output = std::pmr::string(""),
                                        .error = std::pmr::string(""),
                                        .timed_out = false});
      }
      co_return;
    }

    co_await async_sleep(config.poke_interval);
  }

  if (sink.on_complete) {
    sink.on_complete(instance_id,
                     ExecutorResult{.exit_code = config.soft_fail ? 100 : 1,
                                    .stdout_output = std::pmr::string(""),
                                    .stderr_output = std::pmr::string(""),
                                    .error = std::pmr::string(
                                        "Command sensor execution_timeout"),
                                    .timed_out = true});
  }
}

auto run_http_sensor(SensorExecutorConfig config, InstanceId instance_id,
                     ExecutionSink sink, SensorContext ctx) -> spawn_task {
  auto start_time = std::chrono::steady_clock::now();
  auto deadline = start_time + config.execution_timeout;

  auto parsed_res = util::parse_http_url(config.target);
  if (!parsed_res) {
    if (sink.on_complete) {
      sink.on_complete(instance_id,
                       ExecutorResult{.exit_code = 1,
                                      .stdout_output = std::pmr::string(""),
                                      .stderr_output = std::pmr::string(""),
                                      .error = std::pmr::string(std::format(
                                          "Invalid http sensor url: {}",
                                          parsed_res.error().message())),
                                      .timed_out = false});
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
        sink.on_complete(
            instance_id,
            ExecutorResult{.exit_code = 0,
                           .stdout_output = std::pmr::string(std::format(
                               "HTTP {} {} returned {}", method, parsed.path,
                               static_cast<uint16_t>(resp.status))),
                           .stderr_output = std::pmr::string(""),
                           .error = std::pmr::string(""),
                           .timed_out = false});
      }
      co_return;
    }

    co_await async_sleep(config.poke_interval);
  }

  if (sink.on_complete) {
    sink.on_complete(instance_id,
                     ExecutorResult{.exit_code = config.soft_fail ? 100 : 1,
                                    .stdout_output = std::pmr::string(""),
                                    .stderr_output = std::pmr::string(""),
                                    .error = std::pmr::string(
                                        "HTTP sensor execution_timeout"),
                                    .timed_out = true});
  }
}

} // namespace

class SensorExecutor : public IExecutor {
public:
  explicit SensorExecutor(Runtime &runtime)
      : runtime_(&runtime), shard_states_(runtime.shard_count()) {}

  auto start(ExecutorRequest req, ExecutionSink sink) -> Result<void> override {
    auto *config = std::get_if<SensorExecutorConfig>(&req.config);
    if (!config) {
      return fail(Error::InvalidArgument);
    }

    log::info("SensorExecutor start: instance_id={} type={} target={}",
              req.instance_id,
              config->type == SensorType::File   ? "file"
              : config->type == SensorType::Http ? "http"
                                                 : "command",
              config->target);

    auto owner = owner_shard(req.instance_id);
    SensorContext ctx{.state = &shard_states_[owner]};
    switch (config->type) {
    case SensorType::File: {
      runtime_->spawn_on(owner, run_file_sensor(*config, req.instance_id,
                                                std::move(sink), ctx));
      break;
    }
    case SensorType::Http: {
      runtime_->spawn_on(owner, run_http_sensor(*config, req.instance_id,
                                                std::move(sink), ctx));
      break;
    }
    case SensorType::Command: {
      runtime_->spawn_on(owner, run_command_sensor(*config, req.instance_id,
                                                   std::move(sink), ctx));
      break;
    }
    }
    return ok();
  }

  auto cancel(const InstanceId &instance_id) -> void override {
    auto owner = owner_shard(instance_id);
    runtime_->post_to(owner, [this, owner, instance_id] {
      auto &state = shard_states_[owner];
      state.cancelled.insert(instance_id);
      auto it = state.active_commands.find(instance_id);
      if (it != state.active_commands.end() && it->second.pid > 0) {
        (void)::kill(it->second.pid, SIGKILL);
      }
      log::info("SensorExecutor cancel: instance_id={}", instance_id);
    });
  }

private:
  [[nodiscard]] auto owner_shard(const InstanceId &instance_id) const noexcept
      -> shard_id {
    const auto shards = std::max(1U, runtime_->shard_count());
    return static_cast<shard_id>(std::hash<InstanceId>{}(instance_id) % shards);
  }

  Runtime *runtime_;
  std::vector<SensorShardState> shard_states_;
};

auto create_sensor_executor(Runtime &rt) -> std::unique_ptr<IExecutor> {
  return std::make_unique<SensorExecutor>(rt);
}

} // namespace dagforge

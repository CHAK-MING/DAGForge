#include "dagforge/core/asio_awaitable.hpp"
#include "dagforge/core/coroutine.hpp"
#include "dagforge/executor/executor.hpp"
#include "dagforge/executor/executor_utils.hpp"
#include "dagforge/util/id.hpp"
#include "dagforge/util/log.hpp"

#include <boost/asio/bind_cancellation_slot.hpp>
#include <boost/asio/buffer.hpp>
#include <boost/asio/cancel_after.hpp>
#include <boost/asio/cancellation_signal.hpp>
#include <boost/asio/readable_pipe.hpp>
#include <boost/process/v2/environment.hpp>
#include <boost/process/v2/process.hpp>
#include <boost/process/v2/start_dir.hpp>
#include <boost/process/v2/stdio.hpp>

#include <algorithm>
#include <array>
#include <cctype>
#include <csignal>
#include <cstdlib>
#include <format>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

namespace dagforge {

namespace {

namespace bp = boost::process::v2;

inline constexpr std::size_t kMaxOutputSize = 10UZ * 1024 * 1024;
inline constexpr std::size_t kReadBufferSize = 4096;
inline constexpr std::size_t kInitialOutputReserve = 8192;
inline constexpr int kTimeoutExitCode = kExitCodeTimeout;

// is_valid_env_key is in executor_utils.hpp
using ::dagforge::is_valid_env_key;

template <typename Map>
[[nodiscard]] auto build_process_env(const Map &custom)
    -> bp::process_environment {
  std::vector<bp::environment::key_value_pair> env_vec;
  env_vec.reserve(64);

  for (const auto &entry : bp::environment::current()) {
    auto key_sv = entry.key();
    if (custom.contains(std::string(key_sv.data(), key_sv.size()))) {
      continue;
    }
    env_vec.emplace_back(entry);
  }

  for (const auto &[k, v] : custom) {
    env_vec.emplace_back(bp::environment::key{k}, bp::environment::value{v});
  }

  return bp::process_environment(std::move(env_vec));
}

struct ActiveProcess {
  pid_t pid{-1};
};

struct WaitProcessResult {
  int exit_code{-1};
  bool timed_out{false};
};

struct ShellShardState {
  std::unordered_map<InstanceId, ActiveProcess> active_processes;
};

struct ExecutionContext {
  ShellShardState *state{};

  auto register_process(const InstanceId &id, pid_t pid) const noexcept
      -> void {
    state->active_processes[id] = ActiveProcess{.pid = pid};
  }

  auto unregister_process(const InstanceId &id) const noexcept -> void {
    state->active_processes.erase(id);
  }

  [[nodiscard]] auto find_process(const InstanceId &id) const noexcept
      -> std::optional<ActiveProcess> {
    auto it = state->active_processes.find(id);
    if (it == state->active_processes.end()) {
      return std::nullopt;
    }
    return it->second;
  }
};

[[nodiscard]] auto read_pipe_all(boost::asio::readable_pipe &pipe,
                                 std::pmr::string &out,
                                 boost::asio::cancellation_signal &cancel_sig)
    -> task<void> {
  std::array<char, kReadBufferSize> buffer{};
  while (true) {
    auto [ec, bytes] = co_await pipe.async_read_some(
        boost::asio::buffer(buffer.data(), buffer.size()),
        boost::asio::bind_cancellation_slot(cancel_sig.slot(), use_nothrow));
    if (ec) {
      co_return;
    }
    if (bytes > 0 && out.size() < kMaxOutputSize) {
      const auto remaining = kMaxOutputSize - out.size();
      const auto to_append = std::min<std::size_t>(remaining, bytes);
      out.append(buffer.data(), to_append);
    }
  }
}

[[nodiscard]] auto
wait_process_with_timeout(bp::process &proc, std::chrono::seconds timeout,
                          boost::asio::cancellation_signal &cancel_sig)
    -> task<WaitProcessResult> {
  auto [ec, exit_code] =
      co_await proc.async_wait(boost::asio::cancel_after(timeout, use_nothrow));
  if (!ec) {
    co_return WaitProcessResult{.exit_code = exit_code, .timed_out = false};
  }
  if (ec == boost::asio::error::operation_aborted) {
    cancel_sig.emit(boost::asio::cancellation_type::total);
    boost::system::error_code ignored;
    proc.terminate(ignored);
    const auto pid = proc.id();
    if (pid > 0) {
      (void)::kill(pid, SIGKILL);
    }
    auto [wait_ec, ignored_exit] = co_await proc.async_wait(use_nothrow);
    (void)wait_ec;
    (void)ignored_exit;
    co_return WaitProcessResult{.exit_code = kTimeoutExitCode,
                                .timed_out = true};
  }
  co_return WaitProcessResult{.exit_code = -1, .timed_out = false};
}

auto execute_command(std::string cmd, std::string working_dir,
                     std::optional<bp::process_environment> env,
                     std::chrono::seconds timeout, InstanceId instance_id,
                     ExecutionSink sink, ExecutionContext ctx) -> spawn_task {
  auto &io = current_io_context();
  boost::asio::readable_pipe stdout_pipe(io);
  boost::asio::readable_pipe stderr_pipe(io);
  ExecutorResult result{
      .stdout_output = std::pmr::string(current_memory_resource()),
      .stderr_output = std::pmr::string(current_memory_resource()),
      .error = std::pmr::string(current_memory_resource())};
  result.stdout_output.reserve(kInitialOutputReserve);
  result.stderr_output.reserve(kInitialOutputReserve);

  std::optional<bp::process> proc;
  try {
    std::vector<std::string> args;
    args.emplace_back("-c");
    args.emplace_back(std::move(cmd));
    auto stdio = bp::process_stdio{
        .in = nullptr, .out = stdout_pipe, .err = stderr_pipe};

    const bool has_env = env.has_value();
    const bool has_dir = !working_dir.empty();
    if (has_env && has_dir) {
      proc.emplace(io, "/bin/sh", args, std::move(stdio),
                   bp::process_start_dir{std::move(working_dir)},
                   std::move(*env));
    } else if (has_env) {
      proc.emplace(io, "/bin/sh", args, std::move(stdio), std::move(*env));
    } else if (has_dir) {
      proc.emplace(io, "/bin/sh", args, std::move(stdio),
                   bp::process_start_dir{std::move(working_dir)});
    } else {
      proc.emplace(io, "/bin/sh", args, std::move(stdio));
    }
  } catch (const std::exception &ex) {
    result.exit_code = -1;
    result.error.assign(ex.what());
    if (sink.on_complete) {
      sink.on_complete(instance_id, std::move(result));
    }
    co_return;
  }

  const auto pid = proc->id();
  ctx.register_process(instance_id, pid);
  log::info("shell process started pid={} instance_id={}", pid, instance_id);

  boost::asio::cancellation_signal cancel_sig;
  using namespace boost::asio::experimental::awaitable_operators;
  auto wait_result =
      co_await (read_pipe_all(stdout_pipe, result.stdout_output, cancel_sig) &&
                read_pipe_all(stderr_pipe, result.stderr_output, cancel_sig) &&
                wait_process_with_timeout(*proc, timeout, cancel_sig));

  result.timed_out = wait_result.timed_out;
  result.exit_code = wait_result.exit_code;
  if (result.timed_out) {
    result.error =
        std::pmr::string("Execution timeout", current_memory_resource());
  }

  ctx.unregister_process(instance_id);
  log::info("shell finish: instance_id={} exit_code={} timed_out={} err='{}'",
            instance_id, result.exit_code, result.timed_out, result.error);
  if (sink.on_complete) {
    sink.on_complete(instance_id, std::move(result));
  }
  co_return;
}

} // namespace

class ShellExecutor final : public IExecutor {
public:
  explicit ShellExecutor(Runtime &rt)
      : runtime_{&rt}, shard_states_(rt.shard_count()) {}

  ShellExecutor(ShellExecutor &&) noexcept = delete;
  ShellExecutor &operator=(ShellExecutor &&) = delete;
  ShellExecutor(const ShellExecutor &) = delete;
  ShellExecutor &operator=(const ShellExecutor &) = delete;

  auto start(ExecutorRequest req, ExecutionSink sink) -> Result<void> override {
    const auto *shell = std::get_if<ShellExecutorConfig>(&req.config);
    if (!shell) {
      return fail(Error::InvalidArgument);
    }

    log::info("ShellExecutor start: instance_id={} timeout={}s cmd='{}'",
              req.instance_id, shell->execution_timeout.count(),
              cmd_preview(shell->command));

    std::string cmd = shell->command;
    std::optional<bp::process_environment> env;
    if (!shell->env.empty()) {
      for (const auto &[key, value] : shell->env) {
        if (!is_valid_env_key(key)) {
          log::error("Invalid environment variable key: {}", key);
          return fail(Error::InvalidArgument);
        }
      }
      env = build_process_env(shell->env);
    }

    auto owner = owner_shard(req.instance_id);
    auto t = execute_command(std::move(cmd), shell->working_dir, std::move(env),
                             shell->execution_timeout, req.instance_id,
                             std::move(sink),
                             ExecutionContext{.state = &shard_states_[owner]});
    runtime_->spawn_on(owner, std::move(t));
    return ok();
  }

  auto cancel(const InstanceId &instance_id) -> void override {
    auto owner = owner_shard(instance_id);
    runtime_->post_to(owner, [this, owner, instance_id] {
      auto &active = shard_states_[owner].active_processes;
      auto it = active.find(instance_id);
      if (it == active.end() || it->second.pid <= 0) {
        return;
      }
      (void)::kill(it->second.pid, SIGKILL);
      log::info("Cancelled process for instance {}", instance_id);
    });
  }

private:
  [[nodiscard]] auto owner_shard(const InstanceId &instance_id) const noexcept
      -> shard_id {
    const auto shards = std::max(1U, runtime_->shard_count());
    return static_cast<shard_id>(std::hash<InstanceId>{}(instance_id) % shards);
  }

  Runtime *runtime_;
  std::vector<ShellShardState> shard_states_;
};

auto create_shell_executor(Runtime &rt) -> std::unique_ptr<IExecutor> {
  return std::make_unique<ShellExecutor>(rt);
}

} // namespace dagforge

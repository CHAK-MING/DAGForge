#include "dagforge/core/asio_awaitable.hpp"
#include "dagforge/core/coroutine.hpp"
#include "dagforge/executor/executor.hpp"
#include "dagforge/executor/executor_state.hpp"
#include "dagforge/executor/executor_utils.hpp"
#include "dagforge/executor/process_launch.hpp"
#include "dagforge/executor/process_management.hpp"
#include "dagforge/util/id.hpp"
#include "dagforge/util/log.hpp"

#include <boost/asio/bind_cancellation_slot.hpp>
#include <boost/asio/buffer.hpp>
#include <boost/asio/cancel_after.hpp>
#include <boost/asio/cancellation_signal.hpp>
#include <boost/asio/readable_pipe.hpp>
#include <boost/process/v2/process.hpp>
#include <boost/process/v2/stdio.hpp>

#include <algorithm>
#include <array>
#include <cctype>
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

using ShellShardState = ExecutorShardState<ActiveProcess>;

struct ExecutionContext {
  ShellShardState *state{};

  auto register_process(const InstanceId &id, pid_t pid) const noexcept
      -> void {
    state->register_active(id, ActiveProcess{.pid = pid});
  }

  auto unregister_process(const InstanceId &id) const noexcept -> void {
    state->unregister_active(id);
  }

  [[nodiscard]] auto find_process(const InstanceId &id) const noexcept
      -> std::optional<ActiveProcess> {
    return state->find_active(id);
  }
};

auto emit_stream_line(ExecutionSink &sink, const InstanceId &instance_id,
                      std::string_view stream, std::string_view line,
                      bool &streamed_any) -> void {
  if (stream == "stdout") {
    if (sink.on_stdout) {
      streamed_any = true;
      sink.on_stdout(instance_id, line);
    }
    return;
  }
  if (sink.on_stderr) {
    streamed_any = true;
    sink.on_stderr(instance_id, line);
  }
}

[[nodiscard]] auto read_pipe_all(boost::asio::readable_pipe &pipe,
                                 pmr::string &out,
                                 boost::asio::cancellation_signal &cancel_sig,
                                 const InstanceId &instance_id,
                                 ExecutionSink &sink,
                                 std::string_view stream,
                                 bool &streamed_any)
    -> task<void> {
  std::array<char, kReadBufferSize> buffer{};
  std::string pending_line;
  pending_line.reserve(kReadBufferSize);

  auto flush_complete_lines = [&]() {
    std::size_t start = 0;
    while (true) {
      const auto newline = pending_line.find('\n', start);
      if (newline == std::string::npos) {
        break;
      }
      auto line = std::string_view(pending_line).substr(start, newline - start);
      emit_stream_line(sink, instance_id, stream, line, streamed_any);
      start = newline + 1;
    }
    if (start > 0) {
      pending_line.erase(0, start);
    }
  };

  while (true) {
    auto [ec, bytes] = co_await pipe.async_read_some(
        boost::asio::buffer(buffer.data(), buffer.size()),
        boost::asio::bind_cancellation_slot(cancel_sig.slot(), use_nothrow));
    if (bytes > 0 && out.size() < kMaxOutputSize) {
      const auto remaining = kMaxOutputSize - out.size();
      const auto to_append = std::min<std::size_t>(remaining, bytes);
      out.append(buffer.data(), to_append);
    }
    if (bytes > 0) {
      pending_line.append(buffer.data(), bytes);
      flush_complete_lines();
    }
    if (ec) {
      if (!pending_line.empty()) {
        emit_stream_line(sink, instance_id, stream, pending_line, streamed_any);
      }
      co_return;
    }
  }
}

[[nodiscard]] auto
wait_process_with_timeout(bp::process &proc, std::chrono::seconds timeout,
                          boost::asio::cancellation_signal &cancel_sig)
    -> task<ProcessWaitResult> {
  auto [ec, exit_code] =
      co_await proc.async_wait(boost::asio::cancel_after(timeout, use_nothrow));
  if (!ec) {
    co_return ProcessWaitResult{.exit_code = exit_code};
  }
  if (ec == boost::asio::error::operation_aborted) {
    cancel_sig.emit(boost::asio::cancellation_type::total);
    auto result = co_await terminate_and_reap_process(proc, true);
    result.exit_code = kTimeoutExitCode;
    co_return result;
  }
  co_return ProcessWaitResult{.error = ec};
}

auto execute_command(std::string cmd, std::string working_dir,
                     std::optional<bp::process_environment> env,
                     std::chrono::seconds timeout, InstanceId instance_id,
                     ExecutionSink sink, ExecutionContext ctx,
                     pmr::memory_resource *resource) -> spawn_task {
  auto &io = current_io_context();
  boost::asio::readable_pipe stdout_pipe(io);
  boost::asio::readable_pipe stderr_pipe(io);
  ExecutorResult result = make_executor_result(resource);
  result.stdout_output.reserve(kInitialOutputReserve);
  result.stderr_output.reserve(kInitialOutputReserve);

  std::optional<bp::process> proc;
  try {
    ProcessLaunchSpec spec{
        .args = {"-c", std::move(cmd)},
        .stdio = bp::process_stdio{
            .in = nullptr, .out = stdout_pipe, .err = stderr_pipe},
        .env = std::move(env),
        .working_dir = std::move(working_dir)};
    proc.emplace(launch_shell_process(io, std::move(spec)));
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
  bool stdout_streamed = false;
  bool stderr_streamed = false;
  using namespace boost::asio::experimental::awaitable_operators;
  auto wait_result =
      co_await (read_pipe_all(stdout_pipe, result.stdout_output, cancel_sig,
                              instance_id, sink, "stdout",
                              stdout_streamed) &&
                read_pipe_all(stderr_pipe, result.stderr_output, cancel_sig,
                              instance_id, sink, "stderr",
                              stderr_streamed) &&
                wait_process_with_timeout(*proc, timeout, cancel_sig));

  result.timed_out = wait_result.timed_out;
  result.exit_code = wait_result.exit_code;
  result.stdout_streamed = stdout_streamed;
  result.stderr_streamed = stderr_streamed;
  if (result.timed_out) {
    result.error = pmr::string("Execution timeout", resource);
    if (wait_result.error) {
      result.error = pmr::string(
          std::format("Execution timeout: {}", wait_result.error.message()),
          resource);
    }
  } else if (wait_result.error) {
    result.error = pmr::string(
        std::format("Failed to wait for process: {}",
                    wait_result.error.message()),
        resource);
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
    const auto *shell = req.config.as<ShellExecutorConfig>();
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

    auto sid = runtime_->is_current_shard() ? runtime_->current_shard() : 0;
    auto t = execute_command(std::move(cmd), shell->working_dir, std::move(env),
                             shell->execution_timeout, req.instance_id,
                             std::move(sink),
                             ExecutionContext{.state = &shard_states_[sid]},
                             req.resource());
    runtime_->spawn(std::move(t));
    return ok();
  }

  auto cancel(const InstanceId &instance_id) -> void override {
    cancel_on_all_shards(*runtime_, shard_states_, instance_id,
                         [](ShellShardState &state, const InstanceId &id) {
                           auto it = state.find_active_mut(id);
                           if (it == state.active_end() || it->second.pid <= 0) {
                             return;
                           }
                           kill_process_group_or_process(it->second.pid);
                           log::info("Cancelled process for instance {}", id);
                         });
  }

private:
  Runtime *runtime_;
  std::vector<ShellShardState> shard_states_;
};

auto create_shell_executor(Runtime &rt) -> std::unique_ptr<IExecutor> {
  return std::make_unique<ShellExecutor>(rt);
}

} // namespace dagforge

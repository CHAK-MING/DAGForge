#pragma once

#include "dagforge/core/asio_awaitable.hpp"
#include "dagforge/core/coroutine.hpp"

#include <boost/process/v2/process.hpp>

#include <cerrno>
#include <csignal>
#include <system_error>

#include <sys/types.h>
#include <unistd.h>

namespace dagforge {

namespace bp = boost::process::v2;

struct ActiveProcess {
  pid_t pid{-1};
};

struct ProcessWaitResult {
  int exit_code{-1};
  bool timed_out{false};
  std::error_code error{};
};

inline auto kill_process_group_or_process(pid_t pid) noexcept -> void {
  if (pid <= 0) {
    return;
  }
  if (::kill(-pid, SIGKILL) == 0) {
    return;
  }
  if (errno != ESRCH) {
    (void)::kill(pid, SIGKILL);
    return;
  }
  (void)::kill(pid, SIGKILL);
}

inline auto terminate_process_group_or_process(bp::process &proc) noexcept
    -> void {
  boost::system::error_code ignored;
  proc.terminate(ignored);
  kill_process_group_or_process(proc.id());
}

[[nodiscard]] inline auto reap_process(bp::process &proc)
    -> task<ProcessWaitResult> {
  auto [wait_ec, exit_code] = co_await proc.async_wait(use_nothrow);
  ProcessWaitResult result{.exit_code = exit_code, .error = wait_ec};
  co_return result;
}

[[nodiscard]] inline auto terminate_and_reap_process(bp::process &proc,
                                                     bool timed_out = false)
    -> task<ProcessWaitResult> {
  terminate_process_group_or_process(proc);
  auto result = co_await reap_process(proc);
  result.timed_out = timed_out;
  co_return result;
}

} // namespace dagforge

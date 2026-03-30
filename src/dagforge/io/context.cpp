#include "dagforge/io/context.hpp"
#include "dagforge/core/runtime.hpp"

#include <boost/asio/as_tuple.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/system/system_error.hpp>

#include <chrono>
#include <experimental/scope>

namespace dagforge::io {

template <typename Rep, typename Period>
auto async_sleep(IoContext &ctx, std::chrono::duration<Rep, Period> duration)
    -> spawn_task {
  // Keep this wrapper instead of exposing steady_timer directly so callers get
  // one uniform "cancel is normal, everything else is exceptional" policy.
  boost::asio::steady_timer timer(
      ctx, std::chrono::duration_cast<std::chrono::nanoseconds>(duration));

  Runtime *runtime = nullptr;
  shard_id shard = kInvalidShard;
  if (detail::current_runtime != nullptr &&
      detail::current_shard_id != kInvalidShard) {
    runtime = detail::current_runtime;
    shard = detail::current_shard_id;
    runtime->note_timer_started(shard);
  }
  const auto timer_depth_guard = std::experimental::scope_exit([runtime, shard] {
    if (runtime && shard != kInvalidShard) {
      runtime->note_timer_finished(shard);
    }
  });

  auto [ec] = co_await timer.async_wait(
      boost::asio::as_tuple(boost::asio::use_awaitable));

  if (ec && ec != boost::asio::error::operation_aborted) {
    throw boost::system::system_error(ec);
  }
}

template auto async_sleep(IoContext &, std::chrono::nanoseconds) -> spawn_task;
template auto async_sleep(IoContext &, std::chrono::microseconds) -> spawn_task;
template auto async_sleep(IoContext &, std::chrono::milliseconds) -> spawn_task;
template auto async_sleep(IoContext &, std::chrono::seconds) -> spawn_task;

} // namespace dagforge::io

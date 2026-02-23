#include "dagforge/io/context.hpp"

#include <boost/asio/as_tuple.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/system/system_error.hpp>

namespace dagforge::io {

template <typename Rep, typename Period>
auto async_sleep(IoContext &ctx, std::chrono::duration<Rep, Period> duration)
    -> spawn_task {
  boost::asio::steady_timer timer(
      ctx, std::chrono::duration_cast<std::chrono::nanoseconds>(duration));

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

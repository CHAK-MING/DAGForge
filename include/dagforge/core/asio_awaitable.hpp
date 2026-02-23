#pragma once

#include "dagforge/core/error.hpp"

#include <boost/asio/as_tuple.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/cancel_after.hpp>
#include <boost/asio/experimental/awaitable_operators.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/system/error_code.hpp>

#include <tuple>

namespace dagforge {

inline constexpr auto use_nothrow =
    boost::asio::as_tuple(boost::asio::use_awaitable);
namespace awaitable_ops = boost::asio::experimental::awaitable_operators;

template <typename T>
[[nodiscard]] inline auto
as_result(std::tuple<boost::system::error_code, T> &&v) -> Result<T> {
  auto [ec, value] = std::move(v);
  if (ec) {
    return fail(ec);
  }
  return ok(std::move(value));
}

[[nodiscard]] inline auto as_result(std::tuple<boost::system::error_code> &&v)
    -> Result<void> {
  auto [ec] = std::move(v);
  if (ec) {
    return fail(ec);
  }
  return ok();
}

template <typename T>
[[nodiscard]] inline auto co_as_result(
    boost::asio::awaitable<std::tuple<boost::system::error_code, T>> op)
    -> boost::asio::awaitable<Result<T>> {
  co_return as_result(co_await std::move(op));
}

[[nodiscard]] inline auto
co_as_result(boost::asio::awaitable<std::tuple<boost::system::error_code>> op)
    -> boost::asio::awaitable<Result<void>> {
  co_return as_result(co_await std::move(op));
}

} // namespace dagforge

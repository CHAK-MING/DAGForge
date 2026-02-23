#pragma once

#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/experimental/awaitable_operators.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>

namespace dagforge {

template <typename T = void> using task = boost::asio::awaitable<T>;

/// Convenience alias for fire-and-forget coroutines.
using spawn_task = task<void>;

using boost::asio::co_spawn;
using boost::asio::detached;
using boost::asio::use_awaitable;

namespace awaitable_ops = boost::asio::experimental::awaitable_operators;
} // namespace dagforge

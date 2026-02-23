#pragma once

#include "dagforge/core/coroutine.hpp"

#include <boost/asio/io_context.hpp>

#include <chrono>

namespace dagforge::io {

/// IoContext is directly boost::asio::io_context — no wrapper overhead.
using IoContext = boost::asio::io_context;

template <typename Rep, typename Period>
[[nodiscard]] auto async_sleep(IoContext &ctx,
                               std::chrono::duration<Rep, Period> duration)
    -> spawn_task;

} // namespace dagforge::io

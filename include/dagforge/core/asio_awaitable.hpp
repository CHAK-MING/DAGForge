#pragma once


#if !defined(DAGFORGE_CONSUME_NAMED_MODULES) ||                                  \
    !DAGFORGE_CONSUME_NAMED_MODULES
#include "dagforge/core/error.hpp"
#endif

#include <boost/asio/as_tuple.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/cancel_after.hpp>
#include <boost/asio/experimental/awaitable_operators.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/system/error_code.hpp>

#include <tuple>

#include "dagforge/core/detail/asio_awaitable.inc"

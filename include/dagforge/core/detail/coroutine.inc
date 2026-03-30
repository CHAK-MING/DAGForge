namespace dagforge {

template <typename T = void> using task = boost::asio::awaitable<T>;

using spawn_task = task<void>;

using boost::asio::co_spawn;
using boost::asio::detached;
using boost::asio::use_awaitable;

namespace awaitable_ops = boost::asio::experimental::awaitable_operators;
} // namespace dagforge

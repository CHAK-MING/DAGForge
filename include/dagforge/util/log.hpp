#pragma once

#include <boost/asio/experimental/concurrent_channel.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/system/error_code.hpp>

#include <array>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <format>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <unistd.h>
#include <utility>
#include <vector>

namespace dagforge::log {

enum class Level : std::uint8_t { Trace, Debug, Info, Warn, Error };

inline constexpr std::array<std::string_view, 5> level_names = {
    "trace", "debug", "info", "warn", "error"};

inline constexpr std::array<std::string_view, 5> level_colors = {
    "\o{33}[90m", // trace: gray
    "\o{33}[36m", // debug: cyan
    "\o{33}[32m", // info: green
    "\o{33}[33m", // warn: yellow
    "\o{33}[31m"  // error: red
};

[[nodiscard]] inline auto level_name(Level level) -> std::string_view {
  return level_names.at(std::to_underlying(level));
}

[[nodiscard]] inline auto level_color(Level level) -> std::string_view {
  return level_colors.at(std::to_underlying(level));
}

// Thread-local buffer to reduce allocation
struct alignas(64) ThreadBuffer {
  std::string buffer;
  ThreadBuffer() noexcept { buffer.reserve(4096); }
};

inline thread_local ThreadBuffer t_buffer;

// Async logger using Boost concurrent_channel
class Logger {
  static constexpr std::size_t QUEUE_CAPACITY = 8192;
  using LogChannel = boost::asio::experimental::concurrent_channel<
      boost::asio::io_context::executor_type,
      void(boost::system::error_code, std::string)>;

  std::atomic<Level> level_{Level::Info};
  std::atomic<bool> running_{false};
  std::atomic<bool> accepting_{false};
  std::atomic<FILE *> output_{stdout};
  std::atomic<std::uint64_t> dropped_messages_{0};
  FILE *file_{nullptr};
  boost::asio::io_context queue_ctx_{1};
  std::atomic<std::shared_ptr<LogChannel>> queue_;
  std::atomic<std::size_t> queued_{0};
  std::jthread writer_;

  auto writer_loop(std::shared_ptr<LogChannel> queue) -> void {
    std::vector<std::string> batch;
    batch.reserve(64);

    while (running_.load(std::memory_order_acquire)) {
      std::optional<std::string> first;
      boost::system::error_code recv_ec;
      queue->async_receive(
          [&](const boost::system::error_code &ec, std::string item) {
            recv_ec = ec;
            if (!ec) {
              first = std::move(item);
            }
          });

      queue_ctx_.restart();
      (void)queue_ctx_.run_one();
      if (!running_.load(std::memory_order_acquire)) {
        break;
      }
      if (recv_ec) {
        break;
      }
      if (!first) {
        continue;
      }
      batch.clear();
      queued_.fetch_sub(1, std::memory_order_release);
      batch.push_back(std::move(*first));

      // Try to drain more items synchronously to batch writes
      while (std::ssize(batch) < 64) {
        std::optional<std::string> msg;
        if (queue->try_receive(
                [&](const boost::system::error_code &try_ec, std::string item) {
                  if (!try_ec) {
                    msg = std::move(item);
                  }
                })) {
          if (msg.has_value()) {
            queued_.fetch_sub(1, std::memory_order_release);
            batch.push_back(std::move(*msg));
          }
        } else {
          break;
        }
      }

      auto *out = output_.load(std::memory_order_acquire);
      if (!out) {
        out = stdout;
      }
      for (const auto &msg : batch) {
        if (msg.starts_with("\x01")) {
          if (msg.starts_with("\x01STDOUT:")) {
            output_.store(stdout, std::memory_order_release);
            if (file_) {
              std::fclose(file_);
              file_ = nullptr;
            }
            out = stdout;
          } else if (msg.starts_with("\x01FILE:")) {
            std::string path = msg.substr(6);
            FILE *f = std::fopen(path.c_str(), "a");
            if (f) {
              std::setvbuf(f, nullptr, _IOLBF, 0);
              output_.store(f, std::memory_order_release);
              if (file_)
                std::fclose(file_);
              file_ = f;
              out = f;
            }
          }
          continue;
        }
        std::fwrite(msg.data(), 1, msg.size(), out);
      }
      std::fflush(out);
    }

    // Drain any remaining messages after the loop exits (e.g., on stop)
    auto *out = output_.load(std::memory_order_acquire);
    if (!out) {
      out = stdout;
    }
    for (;;) {
      std::optional<std::string> msg;
      if (!queue->try_receive(
              [&](const boost::system::error_code &ec, std::string item) {
                if (!ec) {
                  msg = std::move(item);
                }
              })) {
        break;
      }
      if (msg.has_value()) {
        queued_.fetch_sub(1, std::memory_order_release);
        std::fwrite(msg->data(), 1, msg->size(), out);
      }
    }
    std::fflush(out);
  }

public:
  Logger() = default;
  ~Logger() {
    stop();
    if (file_) {
      std::fclose(file_);
    }
  }

  Logger(const Logger &) = delete;
  Logger &operator=(const Logger &) = delete;

  auto start() -> void {
    if (running_.exchange(true, std::memory_order_acq_rel))
      return;
    accepting_.store(true, std::memory_order_release);
    queue_ctx_.restart();
    auto initial_channel =
        std::make_shared<LogChannel>(queue_ctx_.get_executor(), QUEUE_CAPACITY);
    queue_.store(initial_channel, std::memory_order_release);

    writer_ = std::jthread([this] {
      auto loaded_channel = queue_.load(std::memory_order_acquire);
      if (loaded_channel) {
        writer_loop(std::move(loaded_channel));
      }
    });
  }

  auto stop() -> void {
    accepting_.store(false, std::memory_order_release);

    if (!running_.exchange(false, std::memory_order_acq_rel))
      return;

    auto queue = queue_.exchange(nullptr, std::memory_order_acq_rel);
    if (queue) {
      queue->close();
    }
    queue_ctx_.stop();
    if (writer_.joinable()) {
      writer_.join();
    }
  }

  auto set_level(Level level) noexcept -> void {
    level_.store(level, std::memory_order_release);
  }

  auto set_output_stderr() noexcept -> void {
    output_.store(stderr, std::memory_order_release);
  }

  auto set_output_file(std::string_view path) -> bool {
    auto q = queue_.load(std::memory_order_acquire);
    if (!q) {
      if (path.empty()) {
        output_.store(stdout, std::memory_order_release);
        if (file_) {
          std::fclose(file_);
          file_ = nullptr;
        }
        return true;
      }
      FILE *f = std::fopen(std::string(path).c_str(), "a");
      if (!f)
        return false;
      std::setvbuf(f, nullptr, _IOLBF, 0);
      output_.store(f, std::memory_order_release);
      if (file_)
        std::fclose(file_);
      file_ = f;
      return true;
    }

    std::string cmd =
        path.empty() ? "\x01STDOUT:" : "\x01FILE:" + std::string(path);
    q->try_send(boost::system::error_code{}, cmd);
    return true;
  }

  [[nodiscard]] auto level() const noexcept -> Level {
    return level_.load(std::memory_order_acquire);
  }

  [[nodiscard]] auto should_drop_on_overflow(FILE *out) const noexcept -> bool {
    if (out == nullptr) {
      return true;
    }
    const int fd = ::fileno(out);
    if (fd < 0) {
      return true;
    }
    return ::isatty(fd) == 0;
  }

  template <typename... Args>
  auto log(Level level, std::format_string<Args...> fmt, Args &&...args)
      -> void {
    if (level < level_.load(std::memory_order_acquire))
      return;

    if (!accepting_.load(std::memory_order_acquire)) {
      auto now = std::chrono::system_clock::now();
      auto time = std::chrono::floor<std::chrono::milliseconds>(now);
      auto tid =
          std::hash<std::thread::id>{}(std::this_thread::get_id()) % 1000000;
      auto message =
          std::format("[{:%Y-%m-%d %H:%M:%S}] [{}{}{}] [{}] {}\n", time,
                      level_color(level), level_name(level), "\o{33}[0m", tid,
                      std::format(fmt, std::forward<Args>(args)...));
      auto *out = output_.load(std::memory_order_acquire);
      if (!out) {
        out = stdout;
      }
      std::fwrite(message.data(), 1, message.size(), out);
      std::fflush(out);
      return;
    }

    auto now = std::chrono::system_clock::now();
    auto time = std::chrono::floor<std::chrono::milliseconds>(now);
    auto tid =
        std::hash<std::thread::id>{}(std::this_thread::get_id()) % 1000000;

    auto &buf = t_buffer.buffer;
    buf.clear();
    std::format_to(std::back_inserter(buf),
                   "[{:%Y-%m-%d %H:%M:%S}] [{}{}{}] [{}] {}\n", time,
                   level_color(level), level_name(level), "\o{33}[0m", tid,
                   std::format(fmt, std::forward<Args>(args)...));

    auto queue = queue_.load(std::memory_order_acquire);
    if (!queue ||
        !queue->try_send(boost::system::error_code{}, std::string(buf))) {
      auto *out = output_.load(std::memory_order_acquire);
      if (!out) {
        out = stdout;
      }
      // Avoid ctest pipe backpressure deadlocks: if not attached to a TTY,
      // prefer dropping logs over blocking runtime threads.
      if (should_drop_on_overflow(out)) {
        dropped_messages_.fetch_add(1, std::memory_order_relaxed);
        return;
      }
      std::fwrite(buf.data(), 1, buf.size(), out);
      std::fflush(out);
    } else {
      queued_.fetch_add(1, std::memory_order_release);
    }
  }
};

inline Logger &logger() {
  static Logger instance;
  return instance;
}

inline auto set_level(Level level) noexcept -> void {
  logger().set_level(level);
}

inline auto set_output_file(std::string_view path) -> bool {
  return logger().set_output_file(path);
}

inline auto set_output_stderr() noexcept -> void {
  logger().set_output_stderr();
}

inline auto set_level(std::string_view name) noexcept -> void {
  const auto *it = std::ranges::find(level_names, name);
  auto level = (it != level_names.end())
                   ? static_cast<Level>(std::distance(level_names.begin(), it))
                   : Level::Info;
  logger().set_level(level);
}

inline auto start() -> void { logger().start(); }
template <typename T> inline auto start(T &) -> void { logger().start(); }
inline auto stop() -> void { logger().stop(); }

template <typename... Args>
auto trace(std::format_string<Args...> fmt, Args &&...args) -> void {
  logger().log(Level::Trace, fmt, std::forward<Args>(args)...);
}

template <typename... Args>
auto debug(std::format_string<Args...> fmt, Args &&...args) -> void {
  logger().log(Level::Debug, fmt, std::forward<Args>(args)...);
}

template <typename... Args>
auto info(std::format_string<Args...> fmt, Args &&...args) -> void {
  logger().log(Level::Info, fmt, std::forward<Args>(args)...);
}

template <typename... Args>
auto warn(std::format_string<Args...> fmt, Args &&...args) -> void {
  logger().log(Level::Warn, fmt, std::forward<Args>(args)...);
}

template <typename... Args>
auto error(std::format_string<Args...> fmt, Args &&...args) -> void {
  logger().log(Level::Error, fmt, std::forward<Args>(args)...);
}

} // namespace dagforge::log

#include "dagforge/util/log.hpp"

#include <boost/asio/experimental/concurrent_channel.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/system/error_code.hpp>

#include <atomic>
#include <cstdio>
#include <optional>
#include <thread>
#include <unistd.h>
#include <utility>
#include <vector>

namespace dagforge::log {

namespace {

struct alignas(64) ThreadBuffer {
  std::string buffer;
  ThreadBuffer() noexcept { buffer.reserve(4096); }
};

thread_local ThreadBuffer t_buffer;

} // namespace

struct Logger::Impl {
  static constexpr std::size_t queue_capacity = 8192;
  using LogChannel = boost::asio::experimental::concurrent_channel<
      boost::asio::io_context::executor_type,
      void(boost::system::error_code, std::string)>;

  std::atomic<Level> level{Level::Info};
  std::atomic<bool> running{false};
  std::atomic<bool> accepting{false};
  std::atomic<FILE *> output{stdout};
  std::atomic<std::uint64_t> dropped_messages{0};
  FILE *file{nullptr};
  boost::asio::io_context queue_ctx{1};
  std::atomic<std::shared_ptr<LogChannel>> queue;
  std::atomic<std::size_t> queued{0};
  std::jthread writer;

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

  auto write_immediately(std::string_view message) -> void {
    auto *out = output.load(std::memory_order_acquire);
    if (!out) {
      out = stdout;
    }
    std::fwrite(message.data(), 1, message.size(), out);
    std::fflush(out);
  }

  auto handle_control_message(std::string_view msg, FILE *&out) -> void {
    if (msg.starts_with("\x01STDOUT:")) {
      output.store(stdout, std::memory_order_release);
      if (file) {
        std::fclose(file);
        file = nullptr;
      }
      out = stdout;
      return;
    }
    if (msg.starts_with("\x01FILE:")) {
      std::string path = std::string(msg.substr(6));
      FILE *f = std::fopen(path.c_str(), "a");
      if (f) {
        std::setvbuf(f, nullptr, _IOLBF, 0);
        output.store(f, std::memory_order_release);
        if (file) {
          std::fclose(file);
        }
        file = f;
        out = f;
      }
    }
  }

  auto writer_loop(std::shared_ptr<LogChannel> channel) -> void {
    std::vector<std::string> batch;
    batch.reserve(64);

    while (running.load(std::memory_order_acquire)) {
      struct ReceiveState {
        std::optional<std::string> first;
        boost::system::error_code recv_ec;
      };
      auto receive_state = std::make_shared<ReceiveState>();
      channel->async_receive(
          [receive_state](const boost::system::error_code &ec,
                          std::string item) {
            receive_state->recv_ec = ec;
            if (!ec) {
              receive_state->first = std::move(item);
            }
          });

      queue_ctx.restart();
      (void)queue_ctx.run_one();
      if (!running.load(std::memory_order_acquire) ||
          receive_state->recv_ec) {
        break;
      }
      if (!receive_state->first) {
        continue;
      }

      batch.clear();
      queued.fetch_sub(1, std::memory_order_release);
      batch.push_back(std::move(*receive_state->first));

      while (std::ssize(batch) < 64) {
        std::optional<std::string> msg;
        if (!channel->try_receive(
                [&](const boost::system::error_code &ec, std::string item) {
                  if (!ec) {
                    msg = std::move(item);
                  }
                })) {
          break;
        }
        if (msg) {
          queued.fetch_sub(1, std::memory_order_release);
          batch.push_back(std::move(*msg));
        }
      }

      auto *out = output.load(std::memory_order_acquire);
      if (!out) {
        out = stdout;
      }
      for (const auto &msg : batch) {
        if (msg.starts_with("\x01")) {
          handle_control_message(msg, out);
          continue;
        }
        std::fwrite(msg.data(), 1, msg.size(), out);
      }
      std::fflush(out);
    }

    auto *out = output.load(std::memory_order_acquire);
    if (!out) {
      out = stdout;
    }
    for (;;) {
      std::optional<std::string> msg;
      if (!channel->try_receive(
              [&](const boost::system::error_code &ec, std::string item) {
                if (!ec) {
                  msg = std::move(item);
                }
              })) {
        break;
      }
      if (!msg) {
        continue;
      }
      queued.fetch_sub(1, std::memory_order_release);
      if (msg->starts_with("\x01")) {
        handle_control_message(*msg, out);
        continue;
      }
      std::fwrite(msg->data(), 1, msg->size(), out);
    }
    std::fflush(out);
  }
};

Logger::Logger() : impl_(std::make_unique<Impl>()) {}

Logger::~Logger() {
  stop();
  if (impl_ && impl_->file) {
    std::fclose(impl_->file);
  }
}

Logger::Logger(Logger &&) noexcept = default;

auto Logger::operator=(Logger &&) noexcept -> Logger & = default;

auto Logger::start() -> void {
  if (impl_->running.exchange(true, std::memory_order_acq_rel)) {
    return;
  }
  impl_->accepting.store(true, std::memory_order_release);
  impl_->queue_ctx.restart();
  auto channel = std::make_shared<Impl::LogChannel>(impl_->queue_ctx.get_executor(),
                                                    Impl::queue_capacity);
  impl_->queue.store(channel, std::memory_order_release);
  impl_->writer = std::jthread([this] {
    auto loaded_channel = impl_->queue.load(std::memory_order_acquire);
    if (loaded_channel) {
      impl_->writer_loop(std::move(loaded_channel));
    }
  });
}

auto Logger::stop() -> void {
  impl_->accepting.store(false, std::memory_order_release);
  if (!impl_->running.exchange(false, std::memory_order_acq_rel)) {
    return;
  }
  auto queue = impl_->queue.exchange(nullptr, std::memory_order_acq_rel);
  if (queue) {
    queue->close();
  }
  impl_->queue_ctx.stop();
  if (impl_->writer.joinable()) {
    impl_->writer.join();
  }
}

auto Logger::set_level(Level level) noexcept -> void {
  impl_->level.store(level, std::memory_order_release);
}

auto Logger::set_output_stderr() noexcept -> void {
  impl_->output.store(stderr, std::memory_order_release);
}

auto Logger::set_output_file(std::string_view path) -> bool {
  auto queue = impl_->queue.load(std::memory_order_acquire);
  if (!queue) {
    if (path.empty()) {
      impl_->output.store(stdout, std::memory_order_release);
      if (impl_->file) {
        std::fclose(impl_->file);
        impl_->file = nullptr;
      }
      return true;
    }
    FILE *f = std::fopen(std::string(path).c_str(), "a");
    if (!f) {
      return false;
    }
    std::setvbuf(f, nullptr, _IOLBF, 0);
    impl_->output.store(f, std::memory_order_release);
    if (impl_->file) {
      std::fclose(impl_->file);
    }
    impl_->file = f;
    return true;
  }

  std::string cmd =
      path.empty() ? "\x01STDOUT:" : "\x01FILE:" + std::string(path);
  queue->try_send(boost::system::error_code{}, std::move(cmd));
  return true;
}

auto Logger::level() const noexcept -> Level {
  return impl_->level.load(std::memory_order_acquire);
}

auto Logger::should_log(Level level) const noexcept -> bool {
  return static_cast<std::uint8_t>(level) >=
         static_cast<std::uint8_t>(impl_->level.load(std::memory_order_acquire));
}

auto Logger::enqueue(std::string message) -> void {
  auto queue = impl_->queue.load(std::memory_order_acquire);
  if (!queue || !impl_->accepting.load(std::memory_order_acquire)) {
    impl_->write_immediately(message);
    return;
  }

  auto *out = impl_->output.load(std::memory_order_acquire);
  const auto queued_before = impl_->queued.load(std::memory_order_relaxed);
  if (queued_before >= Impl::queue_capacity) {
    if (impl_->should_drop_on_overflow(out)) {
      impl_->dropped_messages.fetch_add(1, std::memory_order_relaxed);
      return;
    }
    impl_->write_immediately(message);
    return;
  }

  impl_->queued.fetch_add(1, std::memory_order_release);
  if (!queue->try_send(boost::system::error_code{}, std::move(message))) {
    impl_->queued.fetch_sub(1, std::memory_order_release);
    if (impl_->should_drop_on_overflow(out)) {
      impl_->dropped_messages.fetch_add(1, std::memory_order_relaxed);
      return;
    }
    impl_->write_immediately(message);
  }
}

Logger &logger() {
  static Logger instance;
  return instance;
}

} // namespace dagforge::log

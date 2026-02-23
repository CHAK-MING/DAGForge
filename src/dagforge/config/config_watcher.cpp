#include "dagforge/config/config_watcher.hpp"

#include "dagforge/core/constants.hpp"
#include "dagforge/core/coroutine.hpp"
#include "dagforge/core/runtime.hpp"
#include "dagforge/util/log.hpp"

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/posix/stream_descriptor.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <sys/inotify.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <cstring>
#include <future>
#include <string_view>
#include <utility>

namespace dagforge {

struct ConfigWatcher::WatchState {
  std::filesystem::path directory;
  std::atomic<bool> running{false};
  int inotify_fd{-1};
  int watch_fd{-1};
  FileChangeCallback on_file_changed;
  FileRemoveCallback on_file_removed;

  std::unique_ptr<boost::asio::posix::stream_descriptor> inotify_stream;
};

ConfigWatcher::ConfigWatcher(Runtime &runtime, std::string_view directory)
    : runtime_(&runtime), directory_(directory) {}

ConfigWatcher::~ConfigWatcher() { stop(); }

auto ConfigWatcher::start() -> Result<void> {
  if (running_.exchange(true)) {
    return ok();
  }

  auto state = std::make_shared<WatchState>();
  state->directory = directory_;
  state->on_file_changed = std::move(on_file_changed_);
  state->on_file_removed = std::move(on_file_removed_);

  auto fd = sys_check(inotify_init1(IN_NONBLOCK));
  if (!fd) {
    log::error("Failed to initialize inotify: {}", fd.error().message());
    running_.store(false);
    return fail(fd.error());
  }
  state->inotify_fd = *fd;

  auto wd = sys_check(inotify_add_watch(
      state->inotify_fd, state->directory.c_str(),
      IN_CREATE | IN_CLOSE_WRITE | IN_DELETE | IN_MOVED_FROM | IN_MOVED_TO));
  if (!wd) {
    log::error("Failed to add watch on {}: {}", state->directory.string(),
               wd.error().message());
    close(state->inotify_fd);
    state->inotify_fd = -1;
    running_.store(false);
    return fail(wd.error());
  }
  state->watch_fd = *wd;
  state->running.store(true, std::memory_order_release);
  watch_state_ = state;

  auto &io = runtime_->shard(shard_id{0}).ctx();
  state->inotify_stream =
      std::make_unique<boost::asio::posix::stream_descriptor>(
          io, state->inotify_fd);

  boost::asio::co_spawn(
      io,
      [state]() -> task<void> {
        std::array<char, io::kEventBufferSize> buffer{};
        while (state->running.load(std::memory_order_acquire)) {
          if (!state->inotify_stream) {
            break;
          }
          boost::system::error_code ec;
          auto bytes_read = co_await state->inotify_stream->async_read_some(
              boost::asio::buffer(buffer),
              boost::asio::redirect_error(boost::asio::use_awaitable, ec));
          if (ec == boost::asio::error::operation_aborted) {
            break;
          }
          if (ec) {
            if (ec != boost::asio::error::bad_descriptor) {
              log::warn("ConfigWatcher async_read error: {}", ec.message());
            }
            break;
          }
          if (bytes_read > 0) {
            process_events(*state, buffer.data(),
                           static_cast<ssize_t>(bytes_read));
          }
        }
      },
      boost::asio::detached);

  log::info("ConfigWatcher started for: {} (mode: async)", directory_.string());
  return ok();
}

auto ConfigWatcher::stop_state(WatchState &state) noexcept -> void {
  state.running.store(false, std::memory_order_release);

  if (state.inotify_stream) {
    boost::system::error_code ec;
    state.inotify_stream->cancel(ec);
    state.inotify_stream->close(ec);
    state.inotify_stream.reset();
    // stream_descriptor owned and closed the native fd.
    state.inotify_fd = -1;
    state.watch_fd = -1;
    return;
  }

  if (state.watch_fd >= 0 && state.inotify_fd >= 0) {
    inotify_rm_watch(state.inotify_fd, state.watch_fd);
    state.watch_fd = -1;
  }
  if (state.inotify_fd >= 0) {
    close(state.inotify_fd);
    state.inotify_fd = -1;
  }
}

auto ConfigWatcher::stop() noexcept -> void {
  if (!running_.exchange(false)) {
    return;
  }

  auto state = std::move(watch_state_);
  if (!state) {
    return;
  }
  state->running.store(false, std::memory_order_release);

  auto shutdown = [state]() mutable {
    if (state->inotify_stream) {
      boost::system::error_code ec;
      state->inotify_stream->cancel(ec);
    }
    stop_state(*state);
  };

  if (runtime_ == nullptr || !runtime_->is_running()) {
    shutdown();
    return;
  }

  if (runtime_->is_current_shard() &&
      runtime_->current_shard() == shard_id{0}) {
    shutdown();
    return;
  }

  std::promise<void> done;
  auto done_fut = done.get_future();
  runtime_->post_to(shard_id{0}, [shutdown = std::move(shutdown),
                                  done = std::move(done)]() mutable {
    shutdown();
    done.set_value();
  });
  done_fut.wait();
}

auto ConfigWatcher::is_running() const noexcept -> bool {
  return running_.load(std::memory_order_acquire);
}

auto ConfigWatcher::set_on_file_changed(FileChangeCallback cb) -> void {
  if (watch_state_ && runtime_ != nullptr) {
    auto weak = std::weak_ptr<WatchState>(watch_state_);
    auto *rt = runtime_;
    boost::asio::post(rt->shard(shard_id{0}).ctx(),
                      [weak, cb = std::move(cb)]() mutable {
                        if (auto state = weak.lock()) {
                          state->on_file_changed = std::move(cb);
                        }
                      });
    return;
  }
  on_file_changed_ = std::move(cb);
}

auto ConfigWatcher::set_on_file_removed(FileRemoveCallback cb) -> void {
  if (watch_state_ && runtime_ != nullptr) {
    auto weak = std::weak_ptr<WatchState>(watch_state_);
    auto *rt = runtime_;
    boost::asio::post(rt->shard(shard_id{0}).ctx(),
                      [weak, cb = std::move(cb)]() mutable {
                        if (auto state = weak.lock()) {
                          state->on_file_removed = std::move(cb);
                        }
                      });
    return;
  }
  on_file_removed_ = std::move(cb);
}

auto ConfigWatcher::directory() const -> const std::filesystem::path & {
  return directory_;
}

auto ConfigWatcher::process_events(WatchState &state, const char *buf,
                                   ssize_t len) -> void {
  auto is_transient_file = [](std::string_view name) -> bool {
    if (name.empty() || name == "." || name == "..")
      return true;
    if (name.ends_with("~"))
      return true;

    constexpr std::array<std::string_view, 7> kTransientSuffixes{
        ".swp", ".swo", ".swx", ".tmp", ".temp", ".bak", ".part"};
    for (auto suffix : kTransientSuffixes) {
      if (name.ends_with(suffix)) {
        return true;
      }
    }

    // Hidden editor/runtime files are transient. Keep this check exact so
    // names like "api_backup.swp.toml" are not filtered accidentally.
    if (name.starts_with('.') && !name.ends_with(".toml"))
      return true;

    return false;
  };

  ssize_t i = 0;
  while (i < len) {
    const auto *event = reinterpret_cast<const inotify_event *>(buf + i);

    if (event->len > 0) {
      std::string_view name{event->name};
      if (is_transient_file(name)) {
        i += static_cast<ssize_t>(sizeof(inotify_event) + event->len);
        continue;
      }

      std::filesystem::path file_path = state.directory / event->name;
      auto ext = file_path.extension().string();

      if (ext == ".toml") {
        if (event->mask & (IN_CREATE | IN_CLOSE_WRITE | IN_MOVED_TO)) {
          log::info("File changed: {}", file_path.string());
          if (state.on_file_changed) {
            state.on_file_changed(file_path);
          }
        } else if (event->mask & (IN_DELETE | IN_MOVED_FROM)) {
          log::info("File removed: {}", file_path.string());
          if (state.on_file_removed) {
            state.on_file_removed(file_path);
          }
        }
      }
    }

    i += static_cast<ssize_t>(sizeof(inotify_event) + event->len);
  }
}

} // namespace dagforge

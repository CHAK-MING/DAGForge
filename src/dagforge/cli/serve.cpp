#include "dagforge/app/application.hpp"
#include "dagforge/cli/commands.hpp"
#include "dagforge/config/config.hpp"
#include "dagforge/util/daemon.hpp"
#include "dagforge/util/json.hpp"
#include "dagforge/util/log.hpp"

#include <algorithm>
#include <chrono>
#include <csignal>
#include <filesystem>
#include <print>
#include <string>

namespace dagforge::cli {
namespace {

auto resolve_pid_file(const Config &config) -> std::string {
  if (!config.scheduler.pid_file.empty()) {
    return config.scheduler.pid_file;
  }
  return "/tmp/dagforge.pid";
}

auto load_config_or_print(std::string_view path) -> Result<Config> {
  return ConfigLoader::load_from_file(path).or_else(
      [&](std::error_code ec) -> Result<Config> {
        std::println(stderr, "Error: {}", ec.message());
        return fail(ec);
      });
}

} // namespace

auto cmd_serve_start(const ServeStartOptions &opts) -> int {
  auto config_res = load_config_or_print(opts.config_file);
  if (!config_res) {
    return 1;
  }
  auto config = std::move(*config_res);

  if (!config.dag_source.directory.empty()) {
    std::filesystem::path dag_dir{config.dag_source.directory};
    if (dag_dir.is_relative()) {
      std::filesystem::path base = std::filesystem::current_path();
      std::filesystem::path cfg_path{opts.config_file};
      if (!cfg_path.empty()) {
        if (cfg_path.is_relative()) {
          cfg_path = std::filesystem::absolute(cfg_path);
        }
        if (!cfg_path.parent_path().empty()) {
          base = cfg_path.parent_path();
        }
      }
      config.dag_source.directory =
          std::filesystem::weakly_canonical(base / dag_dir).string();
    }
  }

  config.api.enabled = !opts.no_api;
  if (opts.log_level.has_value()) {
    config.scheduler.log_level = *opts.log_level;
  }

  if (opts.shards.has_value()) {
    log::info("Shard count override: {}", *opts.shards);
  }

  const auto log_file = opts.log_file.value_or(config.scheduler.log_file);
  if (opts.daemon && log_file.empty()) {
    std::println(
        stderr,
        "Error: --daemon requires log_file (set in config or --log-file)");
    return 1;
  }
  if (!log_file.empty() && !log::set_output_file(log_file)) {
    std::println(stderr, "Error: Failed to open log file: {}", log_file);
    return 1;
  }

  if (opts.daemon) {
    if (auto r = daemonize(); !r) {
      std::println(stderr, "Error: Failed to daemonize - {}",
                   r.error().message());
      return 1;
    }
  }

  log::set_level(config.scheduler.log_level);

  const auto pid_file = resolve_pid_file(config);
  auto pid_guard = PidFileGuard::acquire(pid_file);
  if (!pid_guard) {
    if (pid_guard.error() == make_error_code(Error::AlreadyExists)) {
      std::println(stderr,
                   "Error: DAGForge is already running (pid file locked: {})",
                   pid_file);
    } else {
      std::println(stderr, "Error: Failed to acquire pid file '{}': {}",
                   pid_file, pid_guard.error().message());
    }
    return 1;
  }

  Application app(std::move(config));

  if (auto r = app.init(); !r.has_value()) {
    log::error("Initialization failed: {}", r.error().message());
    return 1;
  }
  if (auto r = app.start(); !r.has_value()) {
    log::error("Failed to start: {}", r.error().message());
    return 1;
  }

  setup_signal_handlers();

  if (auto r = app.recover_from_crash(); !r.has_value()) {
    log::warn("Recovery failed: {}", r.error().message());
  }

  const auto &cfg = app.config();
  if (opts.no_api) {
    log::info("DAGForge started (scheduler only). pid_file={}", pid_file);
  } else {
    log::info("DAGForge started on {}:{} (pid_file={})", cfg.api.host,
              cfg.api.port, pid_file);
  }

  wait_for_shutdown();
  app.stop();
  log::info("DAGForge stopped.");
  log::stop();
  return 0;
}

auto cmd_serve_stop(const ServeStopOptions &opts) -> int {
  auto config_res = load_config_or_print(opts.config_file);
  if (!config_res) {
    return 1;
  }
  const auto pid_file = resolve_pid_file(*config_res);

  auto pid_res = read_pid_file(pid_file);
  if (!pid_res) {
    if (pid_res.error() == make_error_code(Error::FileNotFound)) {
      std::println("DAGForge is not running (no pid file: {}).", pid_file);
      return 0;
    }
    std::println(stderr, "Error: Failed to read pid file '{}': {}", pid_file,
                 pid_res.error().message());
    return 1;
  }
  const std::int64_t pid = *pid_res;

  if (!is_process_alive(pid)) {
    (void)remove_pid_file(pid_file);
    std::println("DAGForge is not running (stale pid file removed).");
    return 0;
  }

  if (auto r = send_signal(pid, SIGTERM); !r) {
    std::println(stderr, "Error: Failed to send SIGTERM to pid {}: {}", pid,
                 r.error().message());
    return 1;
  }

  const auto timeout = std::chrono::seconds(std::max(opts.timeout_sec, 1));
  if (wait_for_process_exit(pid, timeout)) {
    (void)remove_pid_file(pid_file);
    std::println("DAGForge stopped (pid={}).", pid);
    return 0;
  }

  if (!opts.force) {
    std::println(stderr,
                 "Error: Timed out waiting for DAGForge to stop (pid={}). "
                 "Retry with --force.",
                 pid);
    return 1;
  }

  if (auto r = send_signal(pid, SIGKILL); !r) {
    std::println(stderr, "Error: Failed to send SIGKILL to pid {}: {}", pid,
                 r.error().message());
    return 1;
  }
  if (!wait_for_process_exit(pid, std::chrono::seconds(2))) {
    std::println(stderr, "Error: Process {} did not exit after SIGKILL.", pid);
    return 1;
  }
  (void)remove_pid_file(pid_file);
  std::println("DAGForge killed (pid={}).", pid);
  return 0;
}

auto cmd_serve_status(const ServeStatusOptions &opts) -> int {
  auto config_res = load_config_or_print(opts.config_file);
  if (!config_res) {
    return 1;
  }
  const auto pid_file = resolve_pid_file(*config_res);

  bool running = false;
  bool stale = false;
  std::int64_t pid = 0;

  auto pid_res = read_pid_file(pid_file);
  if (pid_res) {
    pid = *pid_res;
    running = is_process_alive(pid);
    stale = !running;
  } else if (pid_res.error() != make_error_code(Error::FileNotFound)) {
    std::println(stderr, "Error: Failed to read pid file '{}': {}", pid_file,
                 pid_res.error().message());
    return 1;
  }

  if (opts.json) {
    JsonValue obj{
        {"running", running},
        {"pid", pid},
        {"stale_pid_file", stale},
        {"pid_file", pid_file},
    };
    std::println("{}", dump_json(obj));
    return 0;
  }

  if (running) {
    std::println("DAGForge is running.");
    std::println("  pid: {}", pid);
    std::println("  pid_file: {}", pid_file);
    return 0;
  }
  if (stale) {
    std::println("DAGForge is stopped (stale pid file).");
    std::println("  pid_file: {}", pid_file);
    std::println("  stale_pid: {}", pid);
    return 1;
  }

  std::println("DAGForge is stopped.");
  std::println("  pid_file: {}", pid_file);
  return 1;
}

} // namespace dagforge::cli

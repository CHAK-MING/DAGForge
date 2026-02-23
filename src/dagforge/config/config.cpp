#include "dagforge/config/config.hpp"
#include "dagforge/config/toml_util.hpp"

#include "dagforge/core/error.hpp"
#include "dagforge/util/log.hpp"

#include <boost/lexical_cast.hpp>
#include <cstdlib>
#include <string>
#include <string_view>

namespace dagforge {
namespace detail {

struct DatabaseToml {
  std::string host{"127.0.0.1"};
  uint16_t port{3306};
  std::string username{"dagforge"};
  std::string password;
  std::string database{"dagforge"};
  uint16_t pool_size{4};
  uint16_t connect_timeout{5};
};

struct SchedulerToml {
  std::string log_level{"info"};
  std::string log_file;
  std::string pid_file;
  int tick_interval_ms{1000};
  int max_concurrency{10};
  int shards{0};
  int zombie_reaper_interval_sec{60};
  int zombie_heartbeat_timeout_sec{75};
};

struct ApiToml {
  bool enabled{false};
  uint16_t port{8080};
  std::string host{"127.0.0.1"};
  bool reuse_port{false};
  bool tls_enabled{false};
  std::string tls_cert_file;
  std::string tls_key_file;
};

struct DAGSourceToml {
  std::string mode{"file"};
  std::string directory{"./dags"};
  int scan_interval_sec{30};
};

struct SystemToml {
  DatabaseToml database{};
  SchedulerToml scheduler{};
  ApiToml api{};
  DAGSourceToml dag_source{};
};

} // namespace detail
} // namespace dagforge

namespace glz {
template <> struct meta<dagforge::detail::DatabaseToml> {
  using T = dagforge::detail::DatabaseToml;
  static constexpr auto value =
      object("host", &T::host, "port", &T::port, "username", &T::username,
             "password", &T::password, "database", &T::database, "pool_size",
             &T::pool_size, "connect_timeout", &T::connect_timeout);
};

template <> struct meta<dagforge::detail::SchedulerToml> {
  using T = dagforge::detail::SchedulerToml;
  static constexpr auto value =
      object("log_level", &T::log_level, "log_file", &T::log_file, "pid_file",
             &T::pid_file, "tick_interval_ms", &T::tick_interval_ms,
             "max_concurrency", &T::max_concurrency, "shards", &T::shards,
             "zombie_reaper_interval_sec", &T::zombie_reaper_interval_sec,
             "zombie_heartbeat_timeout_sec", &T::zombie_heartbeat_timeout_sec);
};

template <> struct meta<dagforge::detail::ApiToml> {
  using T = dagforge::detail::ApiToml;
  static constexpr auto value = object(
      "enabled", &T::enabled, "port", &T::port, "host", &T::host, "reuse_port",
      &T::reuse_port, "tls_enabled", &T::tls_enabled, "tls_cert_file",
      &T::tls_cert_file, "tls_key_file", &T::tls_key_file);
};

template <> struct meta<dagforge::detail::DAGSourceToml> {
  using T = dagforge::detail::DAGSourceToml;
  static constexpr auto value =
      object("mode", &T::mode, "directory", &T::directory, "scan_interval_sec",
             &T::scan_interval_sec);
};

template <> struct meta<dagforge::detail::SystemToml> {
  using T = dagforge::detail::SystemToml;
  static constexpr auto value =
      object("database", &T::database, "scheduler", &T::scheduler, "api",
             &T::api, "dag_source", &T::dag_source);
};
} // namespace glz

namespace dagforge {
namespace {

[[nodiscard]] auto convert_toml(std::string_view toml_text)
    -> Result<SystemConfig> {
  auto raw_result = toml_util::parse_toml<detail::SystemToml>(toml_text);
  if (!raw_result)
    return fail(raw_result.error());
  auto &raw = *raw_result;

  SystemConfig cfg{};
  cfg.database.host = std::move(raw.database.host);
  cfg.database.port = raw.database.port;
  cfg.database.username = std::move(raw.database.username);
  cfg.database.password = std::move(raw.database.password);
  cfg.database.database = std::move(raw.database.database);
  cfg.database.pool_size = raw.database.pool_size;
  cfg.database.connect_timeout = raw.database.connect_timeout;

  cfg.scheduler.log_level = std::move(raw.scheduler.log_level);
  cfg.scheduler.log_file = std::move(raw.scheduler.log_file);
  cfg.scheduler.pid_file = std::move(raw.scheduler.pid_file);
  cfg.scheduler.tick_interval_ms = raw.scheduler.tick_interval_ms;
  cfg.scheduler.max_concurrency = raw.scheduler.max_concurrency;
  cfg.scheduler.shards = raw.scheduler.shards;
  cfg.scheduler.zombie_reaper_interval_sec =
      raw.scheduler.zombie_reaper_interval_sec;
  cfg.scheduler.zombie_heartbeat_timeout_sec =
      raw.scheduler.zombie_heartbeat_timeout_sec;

  cfg.api.enabled = raw.api.enabled;
  cfg.api.port = raw.api.port;
  cfg.api.host = std::move(raw.api.host);
  cfg.api.reuse_port = raw.api.reuse_port;
  cfg.api.tls_enabled = raw.api.tls_enabled;
  cfg.api.tls_cert_file = std::move(raw.api.tls_cert_file);
  cfg.api.tls_key_file = std::move(raw.api.tls_key_file);

  cfg.dag_source.mode = parse<DAGSourceMode>(raw.dag_source.mode);
  cfg.dag_source.directory = std::move(raw.dag_source.directory);
  cfg.dag_source.scan_interval_sec = raw.dag_source.scan_interval_sec;

  if (const char *v = std::getenv("DAGFORGE_API_PORT"); v != nullptr) {
    cfg.api.port = boost::lexical_cast<uint16_t>(v);
  }
  if (const char *v = std::getenv("DAGFORGE_API_HOST"); v != nullptr) {
    cfg.api.host = v;
  }
  if (const char *v = std::getenv("DAGFORGE_API_ENABLED"); v != nullptr) {
    cfg.api.enabled =
        (std::string_view(v) == "1" || std::string_view(v) == "true");
  }
  if (const char *v = std::getenv("DAGFORGE_API_REUSEPORT"); v != nullptr) {
    cfg.api.reuse_port =
        (std::string_view(v) == "1" || std::string_view(v) == "true");
  }
  if (const char *v = std::getenv("DAGFORGE_API_TLS_ENABLED"); v != nullptr) {
    cfg.api.tls_enabled =
        (std::string_view(v) == "1" || std::string_view(v) == "true");
  }
  if (const char *v = std::getenv("DAGFORGE_API_TLS_CERT_FILE"); v != nullptr) {
    cfg.api.tls_cert_file = v;
  }
  if (const char *v = std::getenv("DAGFORGE_API_TLS_KEY_FILE"); v != nullptr) {
    cfg.api.tls_key_file = v;
  }
  if (const char *v = std::getenv("DAGFORGE_SCHEDULER_SHARDS"); v != nullptr) {
    cfg.scheduler.shards = boost::lexical_cast<int>(v);
  }
  if (const char *v = std::getenv("DAGFORGE_SCHEDULER_MAX_CONCURRENCY");
      v != nullptr) {
    cfg.scheduler.max_concurrency = boost::lexical_cast<int>(v);
  }
  if (const char *v = std::getenv("DAGFORGE_ZOMBIE_REAPER_INTERVAL_SEC");
      v != nullptr) {
    cfg.scheduler.zombie_reaper_interval_sec = boost::lexical_cast<int>(v);
  }
  if (const char *v = std::getenv("DAGFORGE_ZOMBIE_HEARTBEAT_TIMEOUT_SEC");
      v != nullptr) {
    cfg.scheduler.zombie_heartbeat_timeout_sec = boost::lexical_cast<int>(v);
  }
  if (const char *v = std::getenv("DAGFORGE_LOG_LEVEL"); v != nullptr) {
    cfg.scheduler.log_level = v;
  }
  if (const char *v = std::getenv("DAGFORGE_DB_HOST"); v != nullptr) {
    cfg.database.host = v;
  }
  if (const char *v = std::getenv("DAGFORGE_DB_PORT"); v != nullptr) {
    cfg.database.port = boost::lexical_cast<uint16_t>(v);
  }
  if (const char *v = std::getenv("DAGFORGE_DB_USERNAME"); v != nullptr) {
    cfg.database.username = v;
  }
  if (const char *v = std::getenv("DAGFORGE_DB_PASSWORD"); v != nullptr) {
    cfg.database.password = v;
  }
  if (const char *v = std::getenv("DAGFORGE_DB_DATABASE"); v != nullptr) {
    cfg.database.database = v;
  }
  if (const char *v = std::getenv("DAGFORGE_DB_POOL_SIZE"); v != nullptr) {
    cfg.database.pool_size = boost::lexical_cast<uint16_t>(v);
  }
  if (const char *v = std::getenv("DAGFORGE_DB_CONNECT_TIMEOUT");
      v != nullptr) {
    cfg.database.connect_timeout = boost::lexical_cast<uint16_t>(v);
  }
  if (const char *v = std::getenv("DAGFORGE_DAG_DIRECTORY"); v != nullptr) {
    cfg.dag_source.directory = v;
  }

  const bool reaper_disabled = cfg.scheduler.zombie_reaper_interval_sec == 0 &&
                               cfg.scheduler.zombie_heartbeat_timeout_sec == 0;
  const bool reaper_enabled_valid =
      cfg.scheduler.zombie_reaper_interval_sec > 0 &&
      cfg.scheduler.zombie_heartbeat_timeout_sec > 0 &&
      cfg.scheduler.zombie_heartbeat_timeout_sec >
          cfg.scheduler.zombie_reaper_interval_sec;

  if (cfg.scheduler.tick_interval_ms <= 0 ||
      cfg.scheduler.max_concurrency <= 0 || cfg.scheduler.shards < 0 ||
      (!reaper_disabled && !reaper_enabled_valid)) {
    return fail(Error::ParseError);
  }
  return ok(std::move(cfg));
}

} // namespace

auto ConfigLoader::load_from_file(std::string_view path)
    -> Result<SystemConfig> {
  auto text = toml_util::read_file(path);
  if (!text) {
    return fail(text.error());
  }
  return load_from_string(*text);
}

auto ConfigLoader::load_from_string(std::string_view toml_str)
    -> Result<SystemConfig> {
  try {
    return convert_toml(toml_str);
  } catch (const std::exception &e) {
    log::error("Failed to parse TOML system configuration: {}", e.what());
    return fail(Error::ParseError);
  } catch (...) {
    return fail(Error::ParseError);
  }
}

} // namespace dagforge

#pragma once

#include "dagforge/util/enum.hpp"
#include <boost/describe/enum.hpp>
#include <cstdint>
#include <string>

namespace dagforge {

struct DatabaseConfig {
  std::string host{"127.0.0.1"};
  uint16_t port{3306};
  std::string username{"dagforge"};
  std::string password{"dagforge"};
  std::string database{"dagforge"};
  uint16_t pool_size{4};       // per-shard pool size
  uint16_t connect_timeout{5}; // seconds

  auto operator==(const DatabaseConfig &) const -> bool = default;
};

struct SchedulerConfig {
  std::string log_level{"info"};
  std::string log_file;
  std::string pid_file;
  int tick_interval_ms{1000};
  int max_concurrency{10};
  int shards{0}; // 0 = auto (hardware_concurrency)
  int zombie_reaper_interval_sec{0};
  int zombie_heartbeat_timeout_sec{0};

  auto operator==(const SchedulerConfig &) const -> bool = default;
};

struct ApiConfig {
  bool enabled{false};
  uint16_t port{8080};
  std::string host{"127.0.0.1"};
  bool reuse_port{false};
  bool tls_enabled{false};
  std::string tls_cert_file;
  std::string tls_key_file;

  auto operator==(const ApiConfig &) const -> bool = default;
};

enum class DAGSourceMode : std::uint8_t { File, Api, Hybrid };
BOOST_DESCRIBE_ENUM(DAGSourceMode, File, Api, Hybrid)
DAGFORGE_DEFINE_ENUM_SERDE(DAGSourceMode, DAGSourceMode::File)

struct DAGSourceConfig {
  DAGSourceMode mode{DAGSourceMode::File};
  std::string directory{"./dags"};
  int scan_interval_sec{30};

  auto operator==(const DAGSourceConfig &) const -> bool = default;
};

struct SystemConfig {
  DatabaseConfig database;
  SchedulerConfig scheduler;
  ApiConfig api;
  DAGSourceConfig dag_source;

  auto operator==(const SystemConfig &) const -> bool = default;
};

} // namespace dagforge

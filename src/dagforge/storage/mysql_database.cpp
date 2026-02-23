#include "dagforge/storage/mysql_database.hpp"

#include "dagforge/config/task_config.hpp"
#include "dagforge/core/arena.hpp"
#include "dagforge/storage/mysql_schema.hpp"
#include "dagforge/util/log.hpp"
#include "dagforge/util/time.hpp"

#include <boost/asio/cancel_after.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/dispatch.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/mysql/any_connection.hpp>
#include <boost/mysql/connect_params.hpp>
#include <boost/mysql/format_sql.hpp>
#include <boost/mysql/pipeline.hpp>
#include <boost/mysql/results.hpp>
#include <boost/mysql/sequence.hpp>
#include <boost/mysql/with_params.hpp>

#include <algorithm>
#include <cstdint>
#include <memory_resource>
#include <optional>
#include <sstream>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace dagforge::persistence_dto {

struct SensorExecutorConfigJson {
  std::string type;
  std::string target;
  std::int64_t poke_interval{30};
  bool soft_fail{false};
  std::int64_t expected_status{200};
  std::string http_method{"GET"};
};

struct DockerExecutorConfigJson {
  std::string image;
  std::string socket{"/var/run/docker.sock"};
  std::string pull_policy;
};

} // namespace dagforge::persistence_dto

namespace glz {

template <> struct meta<dagforge::persistence_dto::SensorExecutorConfigJson> {
  using T = dagforge::persistence_dto::SensorExecutorConfigJson;
  static constexpr auto value =
      object("type", &T::type, "target", &T::target, "poke_interval",
             &T::poke_interval, "soft_fail", &T::soft_fail, "expected_status",
             &T::expected_status, "http_method", &T::http_method);
};

template <> struct meta<dagforge::persistence_dto::DockerExecutorConfigJson> {
  using T = dagforge::persistence_dto::DockerExecutorConfigJson;
  static constexpr auto value = object("image", &T::image, "socket", &T::socket,
                                       "pull_policy", &T::pull_policy);
};

} // namespace glz

namespace dagforge::storage {
namespace {

using boost::asio::use_awaitable;

[[nodiscard]] auto to_millis(std::chrono::system_clock::time_point tp)
    -> std::int64_t {
  return util::to_unix_millis(tp);
}

[[nodiscard]] auto from_millis(std::int64_t ts)
    -> std::chrono::system_clock::time_point {
  return std::chrono::system_clock::time_point(std::chrono::milliseconds(ts));
}

[[nodiscard]] auto executor_config_to_json(const ExecutorTaskConfig &cfg)
    -> std::string {
  if (const auto *sensor = std::get_if<SensorTaskConfig>(&cfg)) {
    persistence_dto::SensorExecutorConfigJson j{
        .type = enum_to_string(sensor->type),
        .target = sensor->target,
        .poke_interval = sensor->poke_interval.count(),
        .soft_fail = sensor->soft_fail,
        .expected_status = sensor->expected_status,
        .http_method = sensor->http_method,
    };
    if (auto out = glz::write_json(j); out) {
      return *out;
    }
    return "{}";
  }
  if (const auto *docker = std::get_if<DockerTaskConfig>(&cfg)) {
    persistence_dto::DockerExecutorConfigJson j{
        .image = docker->image,
        .socket = docker->socket,
        .pull_policy = enum_to_string(docker->pull_policy),
    };
    if (auto out = glz::write_json(j); out) {
      return *out;
    }
    return "{}";
  }
  return "{}";
}

[[nodiscard]] auto executor_config_from_json(std::string_view json,
                                             ExecutorType executor)
    -> ExecutorTaskConfig {
  auto make_default = [&]() -> ExecutorTaskConfig {
    switch (executor) {
    case ExecutorType::Sensor:
      return SensorTaskConfig{};
    case ExecutorType::Docker:
      return DockerTaskConfig{};
    default:
      return ShellTaskConfig{};
    }
  };

  if (json.empty() || json == "{}") {
    return make_default();
  }

  auto parse_sensor =
      [&](std::string_view input) -> std::optional<SensorTaskConfig> {
    persistence_dto::SensorExecutorConfigJson j{};
    constexpr auto kOpts = glz::opts{.null_terminated = false};
    if (auto ec = glz::read<kOpts>(j, input); ec) {
      return std::nullopt;
    }
    SensorTaskConfig cfg{};
    if (!j.type.empty()) {
      cfg.type = parse<SensorType>(j.type);
    }
    cfg.target = std::move(j.target);
    cfg.poke_interval = std::chrono::seconds(j.poke_interval);
    cfg.soft_fail = j.soft_fail;
    cfg.expected_status = static_cast<int>(j.expected_status);
    cfg.http_method = std::move(j.http_method);
    if (cfg.http_method.empty()) {
      cfg.http_method = "GET";
    }
    return cfg;
  };

  auto parse_docker =
      [&](std::string_view input) -> std::optional<DockerTaskConfig> {
    persistence_dto::DockerExecutorConfigJson j{};
    constexpr auto kOpts = glz::opts{.null_terminated = false};
    if (auto ec = glz::read<kOpts>(j, input); ec) {
      return std::nullopt;
    }
    DockerTaskConfig cfg{};
    cfg.image = std::move(j.image);
    cfg.socket = std::move(j.socket);
    if (cfg.socket.empty()) {
      cfg.socket = "/var/run/docker.sock";
    }
    if (!j.pull_policy.empty()) {
      cfg.pull_policy = parse<ImagePullPolicy>(j.pull_policy);
    }
    return cfg;
  };

  switch (executor) {
  case ExecutorType::Sensor:
    if (auto cfg = parse_sensor(json); cfg) {
      return *cfg;
    }
    break;
  case ExecutorType::Docker:
    if (auto cfg = parse_docker(json); cfg) {
      return *cfg;
    }
    break;
  default:
    break;
  }

  log::warn("Corrupt executor_config JSON, using default for {}: {}",
            to_string_view(executor), json);
  return make_default();
}

[[nodiscard]] auto split_sql_statements(std::string_view input)
    -> std::vector<std::string> {
  std::vector<std::string> out;
  std::string current;
  current.reserve(input.size());

  bool in_single = false;
  bool in_double = false;
  for (char c : input) {
    if (c == '\'' && !in_double) {
      in_single = !in_single;
    } else if (c == '"' && !in_single) {
      in_double = !in_double;
    }

    if (c == ';' && !in_single && !in_double) {
      auto first = current.find_first_not_of(" \n\r\t");
      if (first != std::string::npos) {
        auto last = current.find_last_not_of(" \n\r\t");
        out.emplace_back(current.substr(first, last - first + 1));
      }
      current.clear();
      continue;
    }
    current.push_back(c);
  }

  auto first = current.find_first_not_of(" \n\r\t");
  if (first != std::string::npos) {
    auto last = current.find_last_not_of(" \n\r\t");
    out.emplace_back(current.substr(first, last - first + 1));
  }

  return out;
}

[[nodiscard]] auto make_pool_params(const DatabaseConfig &cfg)
    -> boost::mysql::pool_params {
  boost::mysql::pool_params params;
  params.server_address.emplace_host_and_port(cfg.host, cfg.port);
  params.username = cfg.username;
  params.password = cfg.password;
  params.database = cfg.database;
  params.initial_size = 1;
  params.max_size = std::max<std::size_t>(1, cfg.pool_size);
  params.thread_safe = true;
  params.connect_timeout = std::chrono::seconds(cfg.connect_timeout);
  params.ssl = boost::mysql::ssl_mode::disable;
  return params;
}

[[nodiscard]] auto as_i64(const boost::mysql::field_view &f) -> std::int64_t {
  if (f.is_int64()) {
    return f.as_int64();
  }
  if (f.is_uint64()) {
    return static_cast<std::int64_t>(f.as_uint64());
  }
  if (f.is_string()) {
    return std::stoll(std::string(f.as_string()));
  }
  return 0;
}

[[nodiscard]] auto as_bool(const boost::mysql::field_view &f) -> bool {
  return as_i64(f) != 0;
}

[[nodiscard]] auto as_sv(const boost::mysql::field_view &f)
    -> std::string_view {
  if (!f.is_string()) {
    return {};
  }
  auto s = f.as_string();
  return std::string_view(s.data(), s.size());
}

[[nodiscard]] auto as_str(const boost::mysql::field_view &f) -> std::string {
  if (f.is_string()) {
    return std::string(as_sv(f));
  }
  if (f.is_int64()) {
    return std::to_string(f.as_int64());
  }
  if (f.is_uint64()) {
    return std::to_string(f.as_uint64());
  }
  return {};
}

[[nodiscard]] auto decode_dag_run_state(const boost::mysql::field_view &f)
    -> DAGRunState {
  if (f.is_int64() || f.is_uint64()) {
    return util::parse_enum_code(static_cast<int>(as_i64(f)),
                                 DAGRunState::Running);
  }
  const auto raw = as_sv(f);
  if (raw.empty()) {
    return DAGRunState::Running;
  }
  if (std::all_of(raw.begin(), raw.end(),
                  [](char c) { return c >= '0' && c <= '9'; })) {
    return util::parse_enum_code(std::stoi(std::string(raw)),
                                 DAGRunState::Running);
  }
  return parse<DAGRunState>(raw);
}

[[nodiscard]] auto decode_task_state(const boost::mysql::field_view &f)
    -> TaskState {
  if (f.is_int64() || f.is_uint64()) {
    return util::parse_enum_code(static_cast<int>(as_i64(f)),
                                 TaskState::Pending);
  }
  const auto raw = as_sv(f);
  if (raw.empty()) {
    return TaskState::Pending;
  }
  if (std::all_of(raw.begin(), raw.end(),
                  [](char c) { return c >= '0' && c <= '9'; })) {
    return util::parse_enum_code(std::stoi(std::string(raw)),
                                 TaskState::Pending);
  }
  return parse<TaskState>(raw);
}

[[nodiscard]] auto decode_trigger_type(const boost::mysql::field_view &f)
    -> TriggerType {
  if (f.is_int64() || f.is_uint64()) {
    return util::parse_enum_code(static_cast<int>(as_i64(f)),
                                 TriggerType::Manual);
  }
  const auto raw = as_sv(f);
  if (raw.empty()) {
    return TriggerType::Manual;
  }
  if (std::all_of(raw.begin(), raw.end(),
                  [](char c) { return c >= '0' && c <= '9'; })) {
    return util::parse_enum_code(std::stoi(std::string(raw)),
                                 TriggerType::Manual);
  }
  return parse<TriggerType>(raw);
}

[[nodiscard]] auto to_task_config(const boost::mysql::row_view &row)
    -> TaskConfig {
  auto exec_type = parse<ExecutorType>(as_sv(row.at(6)));
  auto builder =
      TaskConfig::builder()
          .id(as_str(row.at(1)))
          .name(as_str(row.at(2)))
          .command(as_str(row.at(3)))
          .working_dir(as_str(row.at(4)))
          .executor(exec_type)
          .config(executor_config_from_json(as_sv(row.at(7)), exec_type))
          .timeout(std::chrono::seconds(as_i64(row.at(8))))
          .retry(static_cast<int>(as_i64(row.at(10))),
                 std::chrono::seconds(as_i64(row.at(9))));
  auto task = std::move(builder).build().value_or(TaskConfig{});
  task.task_rowid = as_i64(row.at(0));
  task.trigger_rule = parse<TriggerRule>(as_sv(row.at(11)));
  return task;
}

[[nodiscard]] auto to_dag_info(const boost::mysql::row_view &row) -> DAGInfo {
  return DAGInfo{
      .dag_rowid = as_i64(row.at(0)),
      .dag_id = DAGId{as_str(row.at(1))},
      .version = static_cast<int>(as_i64(row.at(2))),
      .name = as_str(row.at(3)),
      .description = as_str(row.at(4)),
      .tags = as_str(row.at(5)),
      .cron = as_str(row.at(6)),
      .timezone = as_str(row.at(7)),
      .max_concurrent_runs = static_cast<int>(as_i64(row.at(8))),
      .start_date = as_i64(row.at(12)) > 0
                        ? std::optional{from_millis(as_i64(row.at(12)))}
                        : std::nullopt,
      .end_date = as_i64(row.at(13)) > 0
                      ? std::optional{from_millis(as_i64(row.at(13)))}
                      : std::nullopt,
      .catchup = as_bool(row.at(9)),
      .is_paused = as_bool(row.at(11)),
      .retention_days = static_cast<int>(as_i64(row.at(16))),
      .created_at = from_millis(as_i64(row.at(14))),
      .updated_at = from_millis(as_i64(row.at(15))),
      .tasks = {},
      .task_index = {},
  };
}

[[nodiscard]] auto to_run_history_entry(const boost::mysql::row_view &row)
    -> DatabaseService::RunHistoryEntry {
  return DatabaseService::RunHistoryEntry{
      .dag_run_id = DAGRunId{as_str(row.at(0))},
      .dag_id = DAGId{as_str(row.at(1))},
      .dag_rowid = as_i64(row.at(2)),
      .run_rowid = as_i64(row.at(3)),
      .dag_version = static_cast<int>(as_i64(row.at(4))),
      .state = decode_dag_run_state(row.at(5)),
      .trigger_type = decode_trigger_type(row.at(6)),
      .scheduled_at = from_millis(as_i64(row.at(7))),
      .started_at = from_millis(as_i64(row.at(8))),
      .finished_at = from_millis(as_i64(row.at(9))),
      .execution_date = from_millis(as_i64(row.at(10))),
  };
}

template <typename F>
auto mysql_try(F &&f) -> task<typename std::invoke_result_t<F>::value_type> {
  try {
    co_return co_await std::forward<F>(f)();
  } catch (const std::exception &e) {
    log::error("MySQL operation failed: {}", e.what());
    co_return fail(Error::DatabaseQueryFailed);
  } catch (...) {
    log::error("MySQL operation failed: unknown exception");
    co_return fail(Error::DatabaseQueryFailed);
  }
}

} // namespace

MySQLDatabase::MySQLDatabase(boost::asio::any_io_executor executor,
                             const DatabaseConfig &config)
    : cfg_(config), pool_(executor, make_pool_params(config)) {}

MySQLDatabase::~MySQLDatabase() { pool_.cancel(); }

auto MySQLDatabase::ensure_database_exists() -> task<Result<void>> {
  std::string direct_connect_error;
  try {
    // Prefer connecting to the configured database directly.
    // This avoids requiring CREATE DATABASE privilege for normal
    // operation/tests.
    boost::mysql::connect_params params;
    params.server_address.emplace_host_and_port(cfg_.host, cfg_.port);
    params.username = cfg_.username;
    params.password = cfg_.password;
    params.database = cfg_.database;
    params.ssl = boost::mysql::ssl_mode::disable;
    params.multi_queries = false;

    boost::mysql::any_connection conn(pool_.get_executor());
    co_await conn.async_connect(
        params, boost::asio::cancel_after(
                    std::chrono::seconds(cfg_.connect_timeout), use_awaitable));

    co_await conn.async_close(use_awaitable);
    co_return ok();
  } catch (const std::exception &e) {
    direct_connect_error = e.what();
  }

  try {
    // Fallback for first run: connect without a default DB and create it.
    boost::mysql::connect_params params;
    params.server_address.emplace_host_and_port(cfg_.host, cfg_.port);
    params.username = cfg_.username;
    params.password = cfg_.password;
    params.database.clear();
    params.ssl = boost::mysql::ssl_mode::disable;
    params.multi_queries = false;

    boost::mysql::any_connection conn(pool_.get_executor());
    co_await conn.async_connect(
        params, boost::asio::cancel_after(
                    std::chrono::seconds(cfg_.connect_timeout), use_awaitable));

    boost::mysql::results res;
    co_await conn.async_execute(
        boost::mysql::with_params("CREATE DATABASE IF NOT EXISTS {:i}",
                                  cfg_.database),
        res, use_awaitable);
    co_await conn.async_close(use_awaitable);
    co_return ok();
  } catch (const std::exception &e) {
    log::error(
        "MySQL ensure database failed: direct_connect='{}', create_db='{}'",
        direct_connect_error, e.what());
    co_return fail(Error::DatabaseOpenFailed);
  }
}

auto MySQLDatabase::open() -> task<Result<void>> {
  if (open_) {
    co_return ok();
  }

  auto db_res = co_await ensure_database_exists();
  if (!db_res) {
    co_return fail(db_res.error());
  }

  open_ = true;
  pool_.async_run(boost::asio::detached);

  auto conn_res = co_await get_connection();
  if (!conn_res) {
    open_ = false;
    co_return fail(conn_res.error());
  }

  auto schema_res = co_await ensure_schema(conn_res->get());
  if (!schema_res) {
    open_ = false;
    co_return fail(schema_res.error());
  }

  conn_res->return_without_reset();

  log::info("MySQL database opened: {}:{} / {}", cfg_.host, cfg_.port,
            cfg_.database);
  co_return ok();
}

auto MySQLDatabase::close() -> task<void> {
  if (open_) {
    pool_.cancel();
    open_ = false;
  }
  co_return;
}

auto MySQLDatabase::is_open() const noexcept -> bool { return open_; }

auto MySQLDatabase::get_connection()
    -> task<Result<boost::mysql::pooled_connection>> {
  if (!open_) {
    co_return fail(Error::SystemNotRunning);
  }

  try {
    auto conn = co_await pool_.async_get_connection(boost::asio::cancel_after(
        std::chrono::seconds(cfg_.connect_timeout), use_awaitable));
    co_return ok(std::move(conn));
  } catch (const std::exception &e) {
    log::error("MySQL get connection failed: {}", e.what());
    co_return fail(Error::DatabaseOpenFailed);
  }
}

auto MySQLDatabase::ensure_schema(boost::mysql::any_connection &conn)
    -> task<Result<void>> {
  try {
    const auto stmts = split_sql_statements(schema::V1_SCHEMA);
    boost::mysql::pipeline_request req;
    for (const auto &stmt_sql : stmts) {
      if (stmt_sql.empty()) {
        continue;
      }
      req.add_execute(stmt_sql);
    }

    req.add_execute("INSERT IGNORE INTO schema_version(version) VALUES (" +
                    std::to_string(schema::CURRENT_SCHEMA_VERSION) + ")");

    std::vector<boost::mysql::stage_response> stage_responses;
    co_await conn.async_run_pipeline(req, stage_responses, use_awaitable);

    co_return ok();
  } catch (const std::exception &e) {
    log::error("MySQL schema ensure failed: {}", e.what());
    co_return fail(Error::DatabaseOpenFailed);
  }
}

auto MySQLDatabase::get_dag_rowid(boost::mysql::any_connection &conn,
                                  const DAGId &dag_id)
    -> task<Result<int64_t>> {
  try {
    boost::mysql::results res;
    co_await conn.async_execute(
        boost::mysql::with_params(
            "SELECT dag_rowid FROM dags WHERE dag_id = {}", dag_id.str()),
        res, use_awaitable);
    if (res.rows().empty()) {
      co_return fail(Error::NotFound);
    }
    co_return ok(as_i64(res.rows().at(0).at(0)));
  } catch (const std::exception &e) {
    log::error("MySQL get_dag_rowid failed: {}", e.what());
    co_return fail(Error::DatabaseQueryFailed);
  }
}

auto MySQLDatabase::save_dag(const DAGInfo &dag) -> task<Result<int64_t>> {
  co_return co_await mysql_try([&]() -> task<Result<int64_t>> {
    auto conn_res = co_await get_connection();
    if (!conn_res) {
      co_return fail(conn_res.error());
    }
    auto &conn = conn_res->get();

    boost::mysql::results res;
    co_await conn.async_execute(
        boost::mysql::with_params(
            "INSERT INTO dags(dag_id, version, name, description, tags, cron, "
            "timezone, "
            "max_concurrent_runs, catchup, is_active, is_paused, start_date, "
            "end_date, "
            "created_at, updated_at, retention_days) "
            "VALUES({}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, "
            "{}, {}) "
            "ON DUPLICATE KEY UPDATE "
            "version=VALUES(version), name=VALUES(name), "
            "description=VALUES(description), "
            "tags=VALUES(tags), cron=VALUES(cron), timezone=VALUES(timezone), "
            "max_concurrent_runs=VALUES(max_concurrent_runs), "
            "catchup=VALUES(catchup), "
            "is_paused=VALUES(is_paused), start_date=VALUES(start_date), "
            "end_date=VALUES(end_date), "
            "updated_at=VALUES(updated_at), "
            "retention_days=VALUES(retention_days)",
            dag.dag_id.str(), dag.version, dag.name, dag.description,
            dag.tags.empty() ? "[]" : dag.tags, dag.cron, dag.timezone,
            dag.max_concurrent_runs, dag.catchup ? 1 : 0, 1,
            dag.is_paused ? 1 : 0,
            dag.start_date ? to_millis(*dag.start_date) : 0,
            dag.end_date ? to_millis(*dag.end_date) : 0,
            to_millis(dag.created_at), to_millis(dag.updated_at),
            dag.retention_days),
        res, use_awaitable);

    auto rowid_res = co_await get_dag_rowid(conn, dag.dag_id);
    if (!rowid_res) {
      co_return fail(rowid_res.error());
    }

    conn_res->return_without_reset();
    co_return ok(*rowid_res);
  });
}

auto MySQLDatabase::set_dag_active(const DAGId &dag_id, bool active)
    -> task<Result<void>> {
  co_return co_await mysql_try([&]() -> task<Result<void>> {
    auto conn_res = co_await get_connection();
    if (!conn_res) {
      co_return fail(conn_res.error());
    }

    boost::mysql::results res;
    co_await conn_res->get().async_execute(
        boost::mysql::with_params(
            "UPDATE dags SET is_active = {} WHERE dag_id = {}", active ? 1 : 0,
            dag_id.str()),
        res, use_awaitable);

    if (res.affected_rows() == 0) {
      boost::mysql::results exists_res;
      co_await conn_res->get().async_execute(
          boost::mysql::with_params(
              "SELECT EXISTS(SELECT 1 FROM dags WHERE dag_id = {})",
              dag_id.str()),
          exists_res, use_awaitable);
      if (!as_bool(exists_res.rows().at(0).at(0))) {
        co_return fail(Error::NotFound);
      }
    }
    conn_res->return_without_reset();
    co_return ok();
  });
}

auto MySQLDatabase::set_dag_paused(const DAGId &dag_id, bool paused)
    -> task<Result<void>> {
  co_return co_await mysql_try([&]() -> task<Result<void>> {
    auto conn_res = co_await get_connection();
    if (!conn_res) {
      co_return fail(conn_res.error());
    }

    boost::mysql::results res;
    co_await conn_res->get().async_execute(
        boost::mysql::with_params(
            "UPDATE dags SET is_paused = {} WHERE dag_id = {}", paused ? 1 : 0,
            dag_id.str()),
        res, use_awaitable);

    if (res.affected_rows() == 0) {
      boost::mysql::results exists_res;
      co_await conn_res->get().async_execute(
          boost::mysql::with_params(
              "SELECT EXISTS(SELECT 1 FROM dags WHERE dag_id = {})",
              dag_id.str()),
          exists_res, use_awaitable);
      if (!as_bool(exists_res.rows().at(0).at(0))) {
        co_return fail(Error::NotFound);
      }
    }
    conn_res->return_without_reset();
    co_return ok();
  });
}

auto MySQLDatabase::get_dag_active(const DAGId &dag_id) -> task<Result<bool>> {
  co_return co_await mysql_try([&]() -> task<Result<bool>> {
    auto conn_res = co_await get_connection();
    if (!conn_res) {
      co_return fail(conn_res.error());
    }

    boost::mysql::results res;
    co_await conn_res->get().async_execute(
        boost::mysql::with_params(
            "SELECT is_active FROM dags WHERE dag_id = {}", dag_id.str()),
        res, use_awaitable);
    if (res.rows().empty()) {
      co_return fail(Error::NotFound);
    }
    auto active = as_bool(res.rows().at(0).at(0));
    conn_res->return_without_reset();
    co_return ok(active);
  });
}

auto MySQLDatabase::delete_dag(const DAGId &dag_id) -> task<Result<void>> {
  co_return co_await mysql_try([&]() -> task<Result<void>> {
    auto conn_res = co_await get_connection();
    if (!conn_res) {
      co_return fail(conn_res.error());
    }

    boost::mysql::results res;
    co_await conn_res->get().async_execute(
        boost::mysql::with_params("DELETE FROM dags WHERE dag_id = {}",
                                  dag_id.str()),
        res, use_awaitable);
    conn_res->return_without_reset();
    if (res.affected_rows() == 0) {
      co_return fail(Error::NotFound);
    }
    co_return ok();
  });
}

auto MySQLDatabase::get_tasks(const DAGId &dag_id)
    -> task<Result<std::vector<TaskConfig>>> {
  co_return co_await mysql_try([&]() -> task<Result<std::vector<TaskConfig>>> {
    auto conn_res = co_await get_connection();
    if (!conn_res) {
      co_return fail(conn_res.error());
    }

    auto dag_rowid_res = co_await get_dag_rowid(conn_res->get(), dag_id);
    if (!dag_rowid_res) {
      co_return fail(dag_rowid_res.error());
    }

    boost::mysql::results res;
    co_await conn_res->get().async_execute(
        boost::mysql::with_params(
            "SELECT task_rowid, task_id, name, command, working_dir, "
            "dag_rowid, executor, "
            "executor_config, timeout, retry_interval, max_retries, "
            "trigger_rule "
            "FROM dag_tasks WHERE dag_rowid = {} ORDER BY task_rowid",
            *dag_rowid_res),
        res, use_awaitable);

    std::vector<TaskConfig> out;
    out.reserve(res.rows().size());
    for (auto row : res.rows()) {
      out.emplace_back(to_task_config(row));
    }
    conn_res->return_without_reset();
    co_return ok(std::move(out));
  });
}

auto MySQLDatabase::get_task_dependencies(const DAGId &dag_id)
    -> task<Result<std::vector<std::pair<TaskId, TaskId>>>> {
  co_return co_await mysql_try(
      [&]() -> task<Result<std::vector<std::pair<TaskId, TaskId>>>> {
        auto conn_res = co_await get_connection();
        if (!conn_res) {
          co_return fail(conn_res.error());
        }

        auto dag_rowid_res = co_await get_dag_rowid(conn_res->get(), dag_id);
        if (!dag_rowid_res) {
          co_return fail(dag_rowid_res.error());
        }

        boost::mysql::results res;
        co_await conn_res->get().async_execute(
            boost::mysql::with_params(
                "SELECT t.task_id, u.task_id "
                "FROM task_dependencies d "
                "JOIN dag_tasks t ON t.task_rowid = d.task_rowid "
                "JOIN dag_tasks u ON u.task_rowid = d.depends_on_task_rowid "
                "WHERE d.dag_rowid = {}",
                *dag_rowid_res),
            res, use_awaitable);

        std::vector<std::pair<TaskId, TaskId>> out;
        out.reserve(res.rows().size());
        for (auto row : res.rows()) {
          out.emplace_back(TaskId{as_str(row.at(0))},
                           TaskId{as_str(row.at(1))});
        }
        conn_res->return_without_reset();
        co_return ok(std::move(out));
      });
}

auto MySQLDatabase::get_dag(const DAGId &dag_id) -> task<Result<DAGInfo>> {
  co_return co_await mysql_try([&]() -> task<Result<DAGInfo>> {
    auto conn_res = co_await get_connection();
    if (!conn_res) {
      co_return fail(conn_res.error());
    }

    boost::mysql::results res;
    co_await conn_res->get().async_execute(
        boost::mysql::with_params("SELECT dag_rowid, dag_id, version, name, "
                                  "description, tags, cron, timezone, "
                                  "max_concurrent_runs, catchup, is_active, "
                                  "is_paused, start_date, end_date, "
                                  "created_at, updated_at, retention_days "
                                  "FROM dags WHERE dag_id = {}",
                                  dag_id.str()),
        res, use_awaitable);

    if (res.rows().empty()) {
      co_return fail(Error::NotFound);
    }

    auto dag = to_dag_info(res.rows().at(0));

    auto tasks_res = co_await get_tasks(dag_id);
    if (!tasks_res) {
      co_return fail(tasks_res.error());
    }
    dag.tasks = std::move(*tasks_res);

    auto deps_res = co_await get_task_dependencies(dag_id);
    if (deps_res) {
      Arena<16384> arena;
      std::pmr::unordered_map<TaskId, std::size_t> idx(arena.resource());
      idx.reserve(dag.tasks.size());
      for (std::size_t i = 0; i < dag.tasks.size(); ++i) {
        idx.emplace(dag.tasks[i].task_id, i);
      }
      for (const auto &[task_id, dep_id] : *deps_res) {
        auto it = idx.find(task_id);
        if (it != idx.end()) {
          dag.tasks[it->second].dependencies.emplace_back(
              TaskDependency{.task_id = dep_id.clone(), .label = ""});
        }
      }
    }

    dag.rebuild_task_index();
    conn_res->return_without_reset();
    co_return ok(std::move(dag));
  });
}

auto MySQLDatabase::get_dag_by_rowid(int64_t dag_rowid)
    -> task<Result<DAGInfo>> {
  co_return co_await mysql_try([&]() -> task<Result<DAGInfo>> {
    auto conn_res = co_await get_connection();
    if (!conn_res) {
      co_return fail(conn_res.error());
    }

    boost::mysql::results res;
    co_await conn_res->get().async_execute(
        boost::mysql::with_params(
            "SELECT dag_id FROM dags WHERE dag_rowid = {}", dag_rowid),
        res, use_awaitable);
    if (res.rows().empty()) {
      co_return fail(Error::NotFound);
    }
    auto dag_id = DAGId{as_str(res.rows().at(0).at(0))};
    conn_res->return_without_reset();
    co_return co_await get_dag(dag_id);
  });
}

auto MySQLDatabase::list_dags() -> task<Result<std::vector<DAGInfo>>> {
  co_return co_await mysql_try([&]() -> task<Result<std::vector<DAGInfo>>> {
    auto conn_res = co_await get_connection();
    if (!conn_res) {
      co_return fail(conn_res.error());
    }

    boost::mysql::results res;
    co_await conn_res->get().async_execute(
        "SELECT dag_rowid, dag_id, version, name, description, tags, cron, "
        "timezone, "
        "max_concurrent_runs, catchup, is_active, is_paused, start_date, "
        "end_date, "
        "created_at, updated_at, retention_days "
        "FROM dags ORDER BY dag_id",
        res, use_awaitable);

    std::vector<DAGInfo> out;
    out.reserve(res.rows().size());
    for (auto row : res.rows()) {
      out.emplace_back(to_dag_info(row));
    }

    conn_res->return_without_reset();

    for (auto &dag : out) {
      auto tasks_res = co_await get_tasks(dag.dag_id);
      if (!tasks_res) {
        co_return fail(tasks_res.error());
      }
      dag.tasks = std::move(*tasks_res);

      auto deps_res = co_await get_task_dependencies(dag.dag_id);
      if (!deps_res) {
        co_return fail(deps_res.error());
      }

      Arena<16384> arena;
      std::pmr::unordered_map<TaskId, std::size_t> idx(arena.resource());
      idx.reserve(dag.tasks.size());
      for (std::size_t i = 0; i < dag.tasks.size(); ++i) {
        idx.emplace(dag.tasks[i].task_id, i);
      }

      for (const auto &[task_id, dep_id] : *deps_res) {
        auto it = idx.find(task_id);
        if (it != idx.end()) {
          dag.tasks[it->second].dependencies.emplace_back(
              TaskDependency{.task_id = dep_id.clone(), .label = ""});
        }
      }

      dag.rebuild_task_index();
    }

    co_return ok(std::move(out));
  });
}

auto MySQLDatabase::save_task(const DAGId &dag_id, const TaskConfig &t)
    -> task<Result<int64_t>> {
  co_return co_await mysql_try([&]() -> task<Result<int64_t>> {
    auto conn_res = co_await get_connection();
    if (!conn_res) {
      co_return fail(conn_res.error());
    }
    auto dag_rowid_res = co_await get_dag_rowid(conn_res->get(), dag_id);
    if (!dag_rowid_res) {
      co_return fail(dag_rowid_res.error());
    }

    boost::mysql::results res;
    co_await conn_res->get().async_execute(
        boost::mysql::with_params(
            "INSERT INTO dag_tasks(dag_rowid, task_id, name, command, "
            "working_dir, executor, "
            "executor_config, timeout, retry_interval, max_retries, "
            "trigger_rule) "
            "VALUES({}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}) "
            "ON DUPLICATE KEY UPDATE name=VALUES(name), "
            "command=VALUES(command), "
            "working_dir=VALUES(working_dir), executor=VALUES(executor), "
            "executor_config=VALUES(executor_config), timeout=VALUES(timeout), "
            "retry_interval=VALUES(retry_interval), "
            "max_retries=VALUES(max_retries), "
            "trigger_rule=VALUES(trigger_rule)",
            *dag_rowid_res, t.task_id.str(), t.name, t.command, t.working_dir,
            t.executor, executor_config_to_json(t.executor_config),
            static_cast<int>(t.execution_timeout.count()),
            static_cast<int>(t.retry_interval.count()), t.max_retries,
            enum_to_string(t.trigger_rule)),
        res, use_awaitable);

    boost::mysql::results get_res;
    co_await conn_res->get().async_execute(
        boost::mysql::with_params("SELECT task_rowid FROM dag_tasks WHERE "
                                  "dag_rowid = {} AND task_id = {}",
                                  *dag_rowid_res, t.task_id.str()),
        get_res, use_awaitable);
    if (get_res.rows().empty()) {
      co_return fail(Error::DatabaseQueryFailed);
    }

    auto rowid = as_i64(get_res.rows().at(0).at(0));
    conn_res->return_without_reset();
    co_return ok(rowid);
  });
}

auto MySQLDatabase::delete_task(const DAGId &dag_id, const TaskId &task_id)
    -> task<Result<void>> {
  co_return co_await mysql_try([&]() -> task<Result<void>> {
    auto conn_res = co_await get_connection();
    if (!conn_res) {
      co_return fail(conn_res.error());
    }

    auto dag_rowid_res = co_await get_dag_rowid(conn_res->get(), dag_id);
    if (!dag_rowid_res) {
      co_return fail(dag_rowid_res.error());
    }

    boost::mysql::results res;
    co_await conn_res->get().async_execute(
        boost::mysql::with_params(
            "DELETE FROM dag_tasks WHERE dag_rowid = {} AND task_id = {}",
            *dag_rowid_res, task_id.str()),
        res, use_awaitable);

    conn_res->return_without_reset();
    if (res.affected_rows() == 0) {
      co_return fail(Error::NotFound);
    }
    co_return ok();
  });
}

auto MySQLDatabase::save_task_dependencies(
    const DAGId &dag_id, const TaskId &task_id,
    const std::vector<TaskId> &dep_task_ids, std::string_view dependency_type)
    -> task<Result<void>> {
  co_return co_await mysql_try([&]() -> task<Result<void>> {
    auto conn_res = co_await get_connection();
    if (!conn_res) {
      co_return fail(conn_res.error());
    }

    auto dag_rowid_res = co_await get_dag_rowid(conn_res->get(), dag_id);
    if (!dag_rowid_res) {
      co_return fail(dag_rowid_res.error());
    }

    std::vector<std::string> lookup_task_ids;
    lookup_task_ids.reserve(dep_task_ids.size() + 1);
    lookup_task_ids.emplace_back(task_id.str());
    for (const auto &dep_id : dep_task_ids) {
      lookup_task_ids.emplace_back(dep_id.str());
    }

    auto format_task_id = [](const std::string &id,
                             boost::mysql::format_context_base &ctx) {
      boost::mysql::format_sql_to(ctx, "{}", id);
    };

    boost::mysql::results rows_res;
    co_await conn_res->get().async_execute(
        boost::mysql::with_params(
            "SELECT task_id, task_rowid FROM dag_tasks "
            "WHERE dag_rowid = {} AND task_id IN ({})",
            *dag_rowid_res,
            boost::mysql::sequence(std::cref(lookup_task_ids), format_task_id)),
        rows_res, use_awaitable);

    std::unordered_map<std::string, std::int64_t> task_rowid_by_id;
    task_rowid_by_id.reserve(rows_res.rows().size());
    for (auto row : rows_res.rows()) {
      task_rowid_by_id.emplace(as_str(row.at(0)), as_i64(row.at(1)));
    }

    auto it_task_rowid = task_rowid_by_id.find(task_id.str());
    if (it_task_rowid == task_rowid_by_id.end()) {
      co_return fail(Error::NotFound);
    }
    const auto task_rowid = it_task_rowid->second;

    boost::mysql::results del_res;
    co_await conn_res->get().async_execute(
        boost::mysql::with_params("DELETE FROM task_dependencies WHERE "
                                  "dag_rowid = {} AND task_rowid = {}",
                                  *dag_rowid_res, task_rowid),
        del_res, use_awaitable);

    if (dep_task_ids.empty()) {
      conn_res->return_without_reset();
      co_return ok();
    }

    std::vector<std::int64_t> dep_rowids;
    dep_rowids.reserve(dep_task_ids.size());
    std::unordered_set<std::int64_t> unique_dep_rowids;
    unique_dep_rowids.reserve(dep_task_ids.size());
    for (const auto &dep_id : dep_task_ids) {
      if (auto it = task_rowid_by_id.find(dep_id.str());
          it != task_rowid_by_id.end() &&
          unique_dep_rowids.insert(it->second).second) {
        dep_rowids.emplace_back(it->second);
      }
    }

    if (dep_rowids.empty()) {
      conn_res->return_without_reset();
      co_return ok();
    }

    const std::string dep_type(dependency_type);
    auto format_dep = [dag_rowid = *dag_rowid_res, task_rowid,
                       &dep_type](const std::int64_t &dep_rowid,
                                  boost::mysql::format_context_base &ctx) {
      boost::mysql::format_sql_to(ctx, "({}, {}, {}, {})", dag_rowid,
                                  task_rowid, dep_rowid, dep_type);
    };

    boost::mysql::results ins_res;
    co_await conn_res->get().async_execute(
        boost::mysql::with_params(
            "INSERT INTO task_dependencies(dag_rowid, task_rowid, "
            "depends_on_task_rowid, "
            "dependency_type) VALUES {} "
            "ON DUPLICATE KEY UPDATE dependency_type=VALUES(dependency_type)",
            boost::mysql::sequence(std::cref(dep_rowids), format_dep)),
        ins_res, use_awaitable);

    conn_res->return_without_reset();
    co_return ok();
  });
}

auto MySQLDatabase::clear_task_dependencies(const DAGId &dag_id)
    -> task<Result<void>> {
  co_return co_await mysql_try([&]() -> task<Result<void>> {
    auto conn_res = co_await get_connection();
    if (!conn_res) {
      co_return fail(conn_res.error());
    }
    auto dag_rowid_res = co_await get_dag_rowid(conn_res->get(), dag_id);
    if (!dag_rowid_res) {
      co_return fail(dag_rowid_res.error());
    }

    boost::mysql::results res;
    co_await conn_res->get().async_execute(
        boost::mysql::with_params(
            "DELETE FROM task_dependencies WHERE dag_rowid = {}",
            *dag_rowid_res),
        res, use_awaitable);
    conn_res->return_without_reset();
    co_return ok();
  });
}

auto MySQLDatabase::save_dag_run(const DAGRun &run) -> task<Result<int64_t>> {
  co_return co_await mysql_try([&]() -> task<Result<int64_t>> {
    auto conn_res = co_await get_connection();
    if (!conn_res) {
      co_return fail(conn_res.error());
    }

    boost::mysql::results res;
    co_await conn_res->get().async_execute(
        boost::mysql::with_params(
            "INSERT INTO dag_runs(dag_run_id, dag_rowid, dag_version, state, "
            "trigger_type, "
            "scheduled_at, started_at, finished_at, execution_date) "
            "VALUES({}, {}, {}, {}, {}, {}, {}, {}, {}) "
            "ON DUPLICATE KEY UPDATE state=VALUES(state), "
            "trigger_type=VALUES(trigger_type), "
            "scheduled_at=VALUES(scheduled_at), started_at=VALUES(started_at), "
            "finished_at=VALUES(finished_at), "
            "execution_date=VALUES(execution_date)",
            run.id().str(), run.dag_rowid(), run.dag_version(),
            util::enum_to_code(run.state()),
            util::enum_to_code(run.trigger_type()),
            to_millis(run.scheduled_at()), to_millis(run.started_at()),
            to_millis(run.finished_at()), to_millis(run.execution_date())),
        res, use_awaitable);

    boost::mysql::results get_res;
    co_await conn_res->get().async_execute(
        boost::mysql::with_params(
            "SELECT run_rowid FROM dag_runs WHERE dag_run_id = {}",
            run.id().str()),
        get_res, use_awaitable);
    if (get_res.rows().empty()) {
      co_return fail(Error::DatabaseQueryFailed);
    }

    auto rowid = as_i64(get_res.rows().at(0).at(0));
    conn_res->return_without_reset();
    co_return ok(rowid);
  });
}

auto MySQLDatabase::update_dag_run_state(const DAGRunId &id, DAGRunState state)
    -> task<Result<void>> {
  co_return co_await mysql_try([&]() -> task<Result<void>> {
    auto conn_res = co_await get_connection();
    if (!conn_res) {
      co_return fail(conn_res.error());
    }
    boost::mysql::results res;
    co_await conn_res->get().async_execute(
        boost::mysql::with_params(
            "UPDATE dag_runs SET state = {} WHERE dag_run_id = {}",
            util::enum_to_code(state), id.str()),
        res, use_awaitable);

    conn_res->return_without_reset();
    if (res.affected_rows() == 0) {
      co_return fail(Error::NotFound);
    }
    co_return ok();
  });
}

auto MySQLDatabase::get_dag_run_state(const DAGRunId &id)
    -> task<Result<DAGRunState>> {
  co_return co_await mysql_try([&]() -> task<Result<DAGRunState>> {
    auto conn_res = co_await get_connection();
    if (!conn_res) {
      co_return fail(conn_res.error());
    }
    boost::mysql::results res;
    co_await conn_res->get().async_execute(
        boost::mysql::with_params(
            "SELECT state FROM dag_runs WHERE dag_run_id = {}", id.str()),
        res, use_awaitable);
    if (res.rows().empty()) {
      co_return fail(Error::NotFound);
    }
    auto state = decode_dag_run_state(res.rows().at(0).at(0));
    conn_res->return_without_reset();
    co_return ok(state);
  });
}

auto MySQLDatabase::get_incomplete_dag_runs()
    -> task<Result<std::vector<DAGRunId>>> {
  co_return co_await mysql_try([&]() -> task<Result<std::vector<DAGRunId>>> {
    auto conn_res = co_await get_connection();
    if (!conn_res) {
      co_return fail(conn_res.error());
    }

    boost::mysql::results res;
    co_await conn_res->get().async_execute(
        boost::mysql::with_params(
            "SELECT dag_run_id FROM dag_runs WHERE state IN ({}, {})",
            util::enum_to_code(DAGRunState::Queued),
            util::enum_to_code(DAGRunState::Running)),
        res, use_awaitable);

    std::vector<DAGRunId> out;
    out.reserve(res.rows().size());
    for (auto row : res.rows()) {
      out.emplace_back(as_str(row.at(0)));
    }
    conn_res->return_without_reset();
    co_return ok(std::move(out));
  });
}

auto MySQLDatabase::save_task_instance(const DAGRunId &run_id,
                                       const TaskInstanceInfo &info)
    -> task<Result<void>> {
  co_return co_await update_task_instance(run_id, info);
}

auto MySQLDatabase::update_task_instance(const DAGRunId &run_id,
                                         const TaskInstanceInfo &info)
    -> task<Result<void>> {
  co_return co_await mysql_try([&]() -> task<Result<void>> {
    auto conn_res = co_await get_connection();
    if (!conn_res) {
      co_return fail(conn_res.error());
    }

    boost::mysql::results run_res;
    co_await conn_res->get().async_execute(
        boost::mysql::with_params(
            "SELECT run_rowid FROM dag_runs WHERE dag_run_id = {}",
            run_id.str()),
        run_res, use_awaitable);
    if (run_res.rows().empty()) {
      co_return fail(Error::NotFound);
    }
    auto run_rowid = as_i64(run_res.rows().at(0).at(0));
    const auto now_ms = to_millis(std::chrono::system_clock::now());
    const auto heartbeat_ms =
        info.state == TaskState::Running
            ? std::max<std::int64_t>(to_millis(info.started_at), now_ms)
            : 0;

    boost::mysql::results res;
    co_await conn_res->get().async_execute(
        boost::mysql::with_params(
            "INSERT INTO task_instances(run_rowid, task_rowid, attempt, state, "
            "worker_id, last_heartbeat, "
            "started_at, "
            "finished_at, updated_at, exit_code, error_message, error_type) "
            "VALUES({}, {}, {}, {}, '', {}, {}, {}, {}, {}, {}, {}) "
            "ON DUPLICATE KEY UPDATE state=VALUES(state), "
            "last_heartbeat=IF(VALUES(state)={}, VALUES(last_heartbeat), "
            "last_heartbeat), "
            "started_at=VALUES(started_at), "
            "finished_at=VALUES(finished_at), updated_at=VALUES(updated_at), "
            "exit_code=VALUES(exit_code), "
            "error_message=VALUES(error_message), "
            "error_type=VALUES(error_type)",
            run_rowid, info.task_rowid, info.attempt,
            util::enum_to_code(info.state), heartbeat_ms,
            to_millis(info.started_at), to_millis(info.finished_at), now_ms,
            info.exit_code, info.error_message, info.error_type,
            util::enum_to_code(TaskState::Running)),
        res, use_awaitable);

    conn_res->return_without_reset();
    co_return ok();
  });
}

auto MySQLDatabase::get_task_instances(const DAGRunId &run_id)
    -> task<Result<std::vector<TaskInstanceInfo>>> {
  co_return co_await mysql_try(
      [&]() -> task<Result<std::vector<TaskInstanceInfo>>> {
        auto conn_res = co_await get_connection();
        if (!conn_res) {
          co_return fail(conn_res.error());
        }

        boost::mysql::results run_res;
        co_await conn_res->get().async_execute(
            boost::mysql::with_params(
                "SELECT run_rowid FROM dag_runs WHERE dag_run_id = {}",
                run_id.str()),
            run_res, use_awaitable);
        if (run_res.rows().empty()) {
          co_return fail(Error::NotFound);
        }
        auto run_rowid = as_i64(run_res.rows().at(0).at(0));

        boost::mysql::results res;
        co_await conn_res->get().async_execute(
            boost::mysql::with_params("SELECT task_rowid, attempt, state, "
                                      "started_at, finished_at, exit_code, "
                                      "error_message, error_type FROM "
                                      "task_instances WHERE run_rowid = {}",
                                      run_rowid),
            res, use_awaitable);

        std::vector<TaskInstanceInfo> out;
        out.reserve(res.rows().size());
        for (auto row : res.rows()) {
          TaskInstanceInfo info;
          info.task_rowid = as_i64(row.at(0));
          info.attempt = static_cast<int>(as_i64(row.at(1)));
          info.state = decode_task_state(row.at(2));
          info.started_at = from_millis(as_i64(row.at(3)));
          info.finished_at = from_millis(as_i64(row.at(4)));
          info.exit_code = static_cast<int>(as_i64(row.at(5)));
          info.error_message = as_str(row.at(6));
          info.error_type = as_str(row.at(7));
          info.run_rowid = run_rowid;
          out.emplace_back(std::move(info));
        }

        conn_res->return_without_reset();
        co_return ok(std::move(out));
      });
}

auto MySQLDatabase::save_task_instances_batch(
    const DAGRunId &run_id, const std::vector<TaskInstanceInfo> &instances)
    -> task<Result<void>> {
  co_return co_await mysql_try([&]() -> task<Result<void>> {
    auto conn_res = co_await get_connection();
    if (!conn_res) {
      co_return fail(conn_res.error());
    }

    boost::mysql::results run_res;
    co_await conn_res->get().async_execute(
        boost::mysql::with_params(
            "SELECT run_rowid FROM dag_runs WHERE dag_run_id = {}",
            run_id.str()),
        run_res, use_awaitable);
    if (run_res.rows().empty()) {
      co_return fail(Error::NotFound);
    }
    const auto run_rowid = as_i64(run_res.rows().at(0).at(0));

    if (instances.empty()) {
      conn_res->return_without_reset();
      co_return ok();
    }

    const auto now_ms = to_millis(std::chrono::system_clock::now());
    auto format_ti = [run_rowid,
                      now_ms](const TaskInstanceInfo &ti,
                              boost::mysql::format_context_base &ctx) {
      const auto heartbeat_ms =
          ti.state == TaskState::Running
              ? std::max<std::int64_t>(to_millis(ti.started_at), now_ms)
              : 0;
      boost::mysql::format_sql_to(
          ctx, "({}, {}, {}, {}, '', {}, {}, {}, {}, {}, {}, {})", run_rowid,
          ti.task_rowid, ti.attempt, util::enum_to_code(ti.state), heartbeat_ms,
          to_millis(ti.started_at), to_millis(ti.finished_at), now_ms,
          ti.exit_code, ti.error_message, ti.error_type);
    };

    boost::mysql::results upsert_res;
    co_await conn_res->get().async_execute(
        boost::mysql::with_params(
            "INSERT INTO task_instances(run_rowid, task_rowid, attempt, state, "
            "worker_id, last_heartbeat, "
            "started_at, "
            "finished_at, updated_at, exit_code, error_message, error_type) "
            "VALUES {} "
            "ON DUPLICATE KEY UPDATE state=VALUES(state), "
            "last_heartbeat=IF(VALUES(state)={}, VALUES(last_heartbeat), "
            "last_heartbeat), "
            "started_at=VALUES(started_at), "
            "finished_at=VALUES(finished_at), updated_at=VALUES(updated_at), "
            "exit_code=VALUES(exit_code), "
            "error_message=VALUES(error_message), "
            "error_type=VALUES(error_type)",
            boost::mysql::sequence(std::cref(instances), format_ti),
            util::enum_to_code(TaskState::Running)),
        upsert_res, use_awaitable);

    conn_res->return_without_reset();
    co_return ok();
  });
}

auto MySQLDatabase::list_run_history(std::size_t limit)
    -> task<Result<std::vector<RunHistoryEntry>>> {
  co_return co_await mysql_try(
      [&]() -> task<Result<std::vector<RunHistoryEntry>>> {
        auto conn_res = co_await get_connection();
        if (!conn_res) {
          co_return fail(conn_res.error());
        }

        boost::mysql::results res;
        co_await conn_res->get().async_execute(
            boost::mysql::with_params(
                "SELECT r.dag_run_id, d.dag_id, r.dag_rowid, r.run_rowid, "
                "r.dag_version, "
                "r.state, r.trigger_type, r.scheduled_at, r.started_at, "
                "r.finished_at, "
                "r.execution_date "
                "FROM dag_runs r JOIN dags d ON d.dag_rowid = r.dag_rowid "
                "ORDER BY r.execution_date DESC LIMIT {}",
                limit),
            res, use_awaitable);

        std::vector<RunHistoryEntry> out;
        out.reserve(res.rows().size());
        for (auto row : res.rows()) {
          out.emplace_back(to_run_history_entry(row));
        }
        conn_res->return_without_reset();
        co_return ok(std::move(out));
      });
}

auto MySQLDatabase::list_dag_run_history(const DAGId &dag_id, std::size_t limit)
    -> task<Result<std::vector<RunHistoryEntry>>> {
  co_return co_await mysql_try(
      [&]() -> task<Result<std::vector<RunHistoryEntry>>> {
        auto conn_res = co_await get_connection();
        if (!conn_res) {
          co_return fail(conn_res.error());
        }

        boost::mysql::results res;
        co_await conn_res->get().async_execute(
            boost::mysql::with_params(
                "SELECT r.dag_run_id, d.dag_id, r.dag_rowid, r.run_rowid, "
                "r.dag_version, "
                "r.state, r.trigger_type, r.scheduled_at, r.started_at, "
                "r.finished_at, "
                "r.execution_date "
                "FROM dag_runs r JOIN dags d ON d.dag_rowid = r.dag_rowid "
                "WHERE d.dag_id = {} "
                "ORDER BY r.execution_date DESC LIMIT {}",
                dag_id.str(), limit),
            res, use_awaitable);

        std::vector<RunHistoryEntry> out;
        out.reserve(res.rows().size());
        for (auto row : res.rows()) {
          out.emplace_back(to_run_history_entry(row));
        }

        conn_res->return_without_reset();
        co_return ok(std::move(out));
      });
}

auto MySQLDatabase::get_run_history(const DAGRunId &run_id)
    -> task<Result<RunHistoryEntry>> {
  co_return co_await mysql_try([&]() -> task<Result<RunHistoryEntry>> {
    auto conn_res = co_await get_connection();
    if (!conn_res) {
      co_return fail(conn_res.error());
    }

    boost::mysql::results res;
    co_await conn_res->get().async_execute(
        boost::mysql::with_params(
            "SELECT r.dag_run_id, d.dag_id, r.dag_rowid, r.run_rowid, "
            "r.dag_version, "
            "r.state, r.trigger_type, r.scheduled_at, r.started_at, "
            "r.finished_at, "
            "r.execution_date "
            "FROM dag_runs r JOIN dags d ON d.dag_rowid = r.dag_rowid "
            "WHERE r.dag_run_id = {} LIMIT 1",
            run_id.str()),
        res, use_awaitable);

    if (res.rows().empty()) {
      co_return fail(Error::NotFound);
    }

    auto entry = to_run_history_entry(res.rows().at(0));
    conn_res->return_without_reset();
    co_return ok(std::move(entry));
  });
}

auto MySQLDatabase::save_xcom(const DAGRunId &run_id, const TaskId &task_id,
                              std::string_view key, const JsonValue &value)
    -> task<Result<void>> {
  co_return co_await mysql_try([&]() -> task<Result<void>> {
    auto conn_res = co_await get_connection();
    if (!conn_res) {
      co_return fail(conn_res.error());
    }

    boost::mysql::results key_res;
    co_await conn_res->get().async_execute(
        boost::mysql::with_params(
            "SELECT r.run_rowid, t.task_rowid "
            "FROM dag_runs r "
            "JOIN dags d ON d.dag_rowid = r.dag_rowid "
            "JOIN dag_tasks t ON t.dag_rowid = d.dag_rowid "
            "WHERE r.dag_run_id = {} AND t.task_id = {}",
            run_id.str(), task_id.str()),
        key_res, use_awaitable);
    if (key_res.rows().empty()) {
      co_return fail(Error::NotFound);
    }

    const auto run_rowid = as_i64(key_res.rows().at(0).at(0));
    const auto task_rowid = as_i64(key_res.rows().at(0).at(1));
    const auto dumped = dump_json(value);

    boost::mysql::results res;
    co_await conn_res->get().async_execute(
        boost::mysql::with_params(
            "INSERT INTO xcom_values(run_rowid, task_rowid, `key`, value, "
            "value_type, byte_size, "
            "created_at, expires_at) VALUES({}, {}, {}, {}, "
            "'json', {}, {}, 0) "
            "ON DUPLICATE KEY UPDATE value=VALUES(value), "
            "byte_size=VALUES(byte_size), "
            "created_at=VALUES(created_at)",
            run_rowid, task_rowid, std::string(key), dumped,
            static_cast<int>(dumped.size()),
            to_millis(std::chrono::system_clock::now())),
        res, use_awaitable);

    conn_res->return_without_reset();
    co_return ok();
  });
}

auto MySQLDatabase::get_xcom(const DAGRunId &run_id, const TaskId &task_id,
                             std::string_view key) -> task<Result<XComEntry>> {
  co_return co_await mysql_try([&]() -> task<Result<XComEntry>> {
    auto conn_res = co_await get_connection();
    if (!conn_res) {
      co_return fail(conn_res.error());
    }

    boost::mysql::results res;
    co_await conn_res->get().async_execute(
        boost::mysql::with_params(
            "SELECT x.`key`, CAST(x.value AS CHAR), x.byte_size, x.created_at "
            "FROM xcom_values x "
            "JOIN dag_runs r ON r.run_rowid = x.run_rowid "
            "JOIN dag_tasks t ON t.task_rowid = x.task_rowid "
            "WHERE r.dag_run_id = {} AND t.task_id = {} AND x.`key` = {}",
            run_id.str(), task_id.str(), std::string(key)),
        res, use_awaitable);

    if (res.rows().empty()) {
      co_return fail(Error::NotFound);
    }

    const auto &row = res.rows().at(0);
    auto parsed = parse_json(as_str(row.at(1)));
    if (!parsed) {
      co_return fail(parsed.error());
    }

    XComEntry entry;
    entry.key = as_str(row.at(0));
    entry.value = std::move(*parsed);
    entry.byte_size = static_cast<std::size_t>(as_i64(row.at(2)));
    entry.created_at = from_millis(as_i64(row.at(3)));

    conn_res->return_without_reset();
    co_return ok(std::move(entry));
  });
}

auto MySQLDatabase::get_task_xcoms(const DAGRunId &run_id,
                                   const TaskId &task_id)
    -> task<Result<std::vector<XComEntry>>> {
  co_return co_await mysql_try([&]() -> task<Result<std::vector<XComEntry>>> {
    auto conn_res = co_await get_connection();
    if (!conn_res) {
      co_return fail(conn_res.error());
    }

    boost::mysql::results res;
    co_await conn_res->get().async_execute(
        boost::mysql::with_params(
            "SELECT x.`key`, CAST(x.value AS CHAR), x.byte_size, x.created_at "
            "FROM xcom_values x "
            "JOIN dag_runs r ON r.run_rowid = x.run_rowid "
            "JOIN dag_tasks t ON t.task_rowid = x.task_rowid "
            "WHERE r.dag_run_id = {} AND t.task_id = {}",
            run_id.str(), task_id.str()),
        res, use_awaitable);

    std::vector<XComEntry> out;
    out.reserve(res.rows().size());

    for (auto row : res.rows()) {
      auto parsed = parse_json(as_str(row.at(1)));
      if (!parsed) {
        continue;
      }
      out.emplace_back(XComEntry{
          .key = as_str(row.at(0)),
          .value = std::move(*parsed),
          .byte_size = static_cast<std::size_t>(as_i64(row.at(2))),
          .created_at = from_millis(as_i64(row.at(3))),
      });
    }

    conn_res->return_without_reset();
    co_return ok(std::move(out));
  });
}

auto MySQLDatabase::get_run_xcoms(const DAGRunId &run_id)
    -> task<Result<std::vector<XComTaskEntry>>> {
  co_return co_await mysql_try(
      [&]() -> task<Result<std::vector<XComTaskEntry>>> {
        auto conn_res = co_await get_connection();
        if (!conn_res) {
          co_return fail(conn_res.error());
        }

        boost::mysql::results res;
        co_await conn_res->get().async_execute(
            boost::mysql::with_params(
                "SELECT t.task_id, x.`key`, CAST(x.value AS CHAR), "
                "x.byte_size, x.created_at "
                "FROM xcom_values x "
                "JOIN dag_runs r ON r.run_rowid = x.run_rowid "
                "JOIN dag_tasks t ON t.task_rowid = x.task_rowid "
                "WHERE r.dag_run_id = {}",
                run_id.str()),
            res, use_awaitable);

        std::vector<XComTaskEntry> out;
        out.reserve(res.rows().size());

        for (auto row : res.rows()) {
          auto parsed = parse_json(as_str(row.at(2)));
          if (!parsed) {
            continue;
          }
          out.emplace_back(XComTaskEntry{
              .task_id = TaskId{as_str(row.at(0))},
              .key = as_str(row.at(1)),
              .value = std::move(*parsed),
              .byte_size = static_cast<std::size_t>(as_i64(row.at(3))),
              .created_at = from_millis(as_i64(row.at(4))),
          });
        }

        conn_res->return_without_reset();
        co_return ok(std::move(out));
      });
}

auto MySQLDatabase::delete_run_xcoms(const DAGRunId &run_id)
    -> task<Result<void>> {
  co_return co_await mysql_try([&]() -> task<Result<void>> {
    auto conn_res = co_await get_connection();
    if (!conn_res) {
      co_return fail(conn_res.error());
    }

    boost::mysql::results run_res;
    co_await conn_res->get().async_execute(
        boost::mysql::with_params(
            "SELECT run_rowid FROM dag_runs WHERE dag_run_id = {}",
            run_id.str()),
        run_res, use_awaitable);
    if (run_res.rows().empty()) {
      co_return fail(Error::NotFound);
    }

    boost::mysql::results del_res;
    co_await conn_res->get().async_execute(
        boost::mysql::with_params(
            "DELETE FROM xcom_values WHERE run_rowid = {}",
            as_i64(run_res.rows().at(0).at(0))),
        del_res, use_awaitable);

    conn_res->return_without_reset();
    co_return ok();
  });
}

auto MySQLDatabase::get_last_execution_date(const DAGId &dag_id)
    -> task<Result<TimePoint>> {
  co_return co_await mysql_try([&]() -> task<Result<TimePoint>> {
    auto conn_res = co_await get_connection();
    if (!conn_res) {
      co_return fail(conn_res.error());
    }

    boost::mysql::results res;
    co_await conn_res->get().async_execute(
        boost::mysql::with_params(
            "SELECT MAX(r.execution_date) FROM dag_runs r "
            "JOIN dags d ON d.dag_rowid = r.dag_rowid WHERE d.dag_id = {}",
            dag_id.str()),
        res, use_awaitable);

    if (res.rows().empty() || res.rows().at(0).at(0).is_null()) {
      co_return fail(Error::NotFound);
    }

    conn_res->return_without_reset();
    co_return ok(from_millis(as_i64(res.rows().at(0).at(0))));
  });
}

auto MySQLDatabase::run_exists(const DAGId &dag_id, TimePoint execution_time)
    -> task<Result<bool>> {
  co_return co_await has_dag_run(dag_id, execution_time);
}

auto MySQLDatabase::has_dag_run(const DAGId &dag_id, TimePoint execution_date)
    -> task<Result<bool>> {
  co_return co_await mysql_try([&]() -> task<Result<bool>> {
    auto conn_res = co_await get_connection();
    if (!conn_res) {
      co_return fail(conn_res.error());
    }

    boost::mysql::results res;
    co_await conn_res->get().async_execute(
        boost::mysql::with_params(
            "SELECT EXISTS("
            "SELECT 1 FROM dag_runs r "
            "JOIN dags d ON d.dag_rowid = r.dag_rowid "
            "WHERE d.dag_id = {} AND r.execution_date = {} LIMIT 1"
            ")",
            dag_id.str(), to_millis(execution_date)),
        res, use_awaitable);

    auto exists = !res.rows().empty() && as_bool(res.rows().at(0).at(0));
    conn_res->return_without_reset();
    co_return ok(exists);
  });
}

auto MySQLDatabase::save_watermark(const DAGId &dag_id, TimePoint ts)
    -> task<Result<void>> {
  co_return co_await mysql_try([&]() -> task<Result<void>> {
    auto conn_res = co_await get_connection();
    if (!conn_res) {
      co_return fail(conn_res.error());
    }
    auto dag_rowid_res = co_await get_dag_rowid(conn_res->get(), dag_id);
    if (!dag_rowid_res) {
      co_return fail(dag_rowid_res.error());
    }

    boost::mysql::results res;
    co_await conn_res->get().async_execute(
        boost::mysql::with_params("INSERT INTO dag_watermarks(dag_rowid, "
                                  "last_scheduled_at, last_success_at, "
                                  "last_failure_at) VALUES({}, {}, 0, 0) "
                                  "ON DUPLICATE KEY UPDATE "
                                  "last_scheduled_at=VALUES(last_scheduled_at)",
                                  *dag_rowid_res, to_millis(ts)),
        res, use_awaitable);

    conn_res->return_without_reset();
    co_return ok();
  });
}

auto MySQLDatabase::get_watermark(const DAGId &dag_id)
    -> task<Result<TimePoint>> {
  co_return co_await mysql_try([&]() -> task<Result<TimePoint>> {
    auto conn_res = co_await get_connection();
    if (!conn_res) {
      co_return fail(conn_res.error());
    }
    auto dag_rowid_res = co_await get_dag_rowid(conn_res->get(), dag_id);
    if (!dag_rowid_res) {
      co_return fail(dag_rowid_res.error());
    }

    boost::mysql::results res;
    co_await conn_res->get().async_execute(
        boost::mysql::with_params(
            "SELECT last_scheduled_at FROM dag_watermarks WHERE dag_rowid = {}",
            *dag_rowid_res),
        res, use_awaitable);

    if (res.rows().empty()) {
      co_return fail(Error::NotFound);
    }

    conn_res->return_without_reset();
    co_return ok(from_millis(as_i64(res.rows().at(0).at(0))));
  });
}

auto MySQLDatabase::update_watermark_success(const DAGId &dag_id, TimePoint ts)
    -> task<Result<void>> {
  co_return co_await mysql_try([&]() -> task<Result<void>> {
    auto conn_res = co_await get_connection();
    if (!conn_res) {
      co_return fail(conn_res.error());
    }
    auto dag_rowid_res = co_await get_dag_rowid(conn_res->get(), dag_id);
    if (!dag_rowid_res) {
      co_return fail(dag_rowid_res.error());
    }

    boost::mysql::results res;
    co_await conn_res->get().async_execute(
        boost::mysql::with_params(
            "INSERT INTO dag_watermarks(dag_rowid, last_scheduled_at, "
            "last_success_at, "
            "last_failure_at) VALUES({}, 0, {}, 0) "
            "ON DUPLICATE KEY UPDATE last_success_at=VALUES(last_success_at)",
            *dag_rowid_res, to_millis(ts)),
        res, use_awaitable);

    conn_res->return_without_reset();
    co_return ok();
  });
}

auto MySQLDatabase::update_watermark_failure(const DAGId &dag_id, TimePoint ts)
    -> task<Result<void>> {
  co_return co_await mysql_try([&]() -> task<Result<void>> {
    auto conn_res = co_await get_connection();
    if (!conn_res) {
      co_return fail(conn_res.error());
    }
    auto dag_rowid_res = co_await get_dag_rowid(conn_res->get(), dag_id);
    if (!dag_rowid_res) {
      co_return fail(dag_rowid_res.error());
    }

    boost::mysql::results res;
    co_await conn_res->get().async_execute(
        boost::mysql::with_params(
            "INSERT INTO dag_watermarks(dag_rowid, last_scheduled_at, "
            "last_success_at, "
            "last_failure_at) VALUES({}, 0, 0, {}) "
            "ON DUPLICATE KEY UPDATE last_failure_at=VALUES(last_failure_at)",
            *dag_rowid_res, to_millis(ts)),
        res, use_awaitable);

    conn_res->return_without_reset();
    co_return ok();
  });
}

auto MySQLDatabase::get_previous_task_state(const DAGId &dag_id,
                                            const TaskId &task_id,
                                            TimePoint current_execution_date,
                                            const DAGRunId &current_run_id)
    -> task<Result<TaskState>> {
  co_return co_await mysql_try([&]() -> task<Result<TaskState>> {
    auto conn_res = co_await get_connection();
    if (!conn_res) {
      co_return fail(conn_res.error());
    }

    boost::mysql::results res;
    co_await conn_res->get().async_execute(
        boost::mysql::with_params(
            "SELECT ti.state "
            "FROM task_instances ti "
            "JOIN dag_runs r ON r.run_rowid = ti.run_rowid "
            "JOIN dags d ON d.dag_rowid = r.dag_rowid "
            "JOIN dag_tasks t ON t.task_rowid = ti.task_rowid "
            "WHERE d.dag_id = {} AND t.task_id = {} "
            "AND r.execution_date < {} AND r.dag_run_id <> {} "
            "ORDER BY r.execution_date DESC, ti.attempt DESC LIMIT 1",
            dag_id.str(), task_id.str(), to_millis(current_execution_date),
            current_run_id.str()),
        res, use_awaitable);

    if (res.rows().empty()) {
      conn_res->return_without_reset();
      co_return fail(Error::NotFound);
    }

    conn_res->return_without_reset();
    co_return ok(decode_task_state(res.rows().at(0).at(0)));
  });
}

auto MySQLDatabase::clear_all_dag_data() -> task<Result<void>> {
  co_return co_await mysql_try([&]() -> task<Result<void>> {
    auto conn_res = co_await get_connection();
    if (!conn_res) {
      co_return fail(conn_res.error());
    }

    boost::mysql::pipeline_request req;
    for (const auto *sql :
         {"DELETE FROM xcom_values", "DELETE FROM task_instances",
          "DELETE FROM dag_runs", "DELETE FROM task_dependencies",
          "DELETE FROM dag_tasks", "DELETE FROM dag_watermarks",
          "DELETE FROM dags"}) {
      req.add_execute(sql);
    }

    std::vector<boost::mysql::stage_response> stage_responses;
    co_await conn_res->get().async_run_pipeline(req, stage_responses,
                                                use_awaitable);

    conn_res->return_without_reset();
    co_return ok();
  });
}

auto MySQLDatabase::mark_incomplete_runs_failed() -> task<Result<std::size_t>> {
  co_return co_await mysql_try([&]() -> task<Result<std::size_t>> {
    auto conn_res = co_await get_connection();
    if (!conn_res) {
      co_return fail(conn_res.error());
    }

    boost::mysql::results res;
    co_await conn_res->get().async_execute(
        boost::mysql::with_params(
            "UPDATE task_instances ti "
            "JOIN dag_runs r ON r.run_rowid = ti.run_rowid "
            "SET ti.state = {}, "
            "ti.finished_at = CAST(UNIX_TIMESTAMP(CURRENT_TIMESTAMP(3)) * "
            "1000 AS UNSIGNED), "
            "ti.exit_code = IF(ti.exit_code = 0, 1, ti.exit_code), "
            "ti.error_type = IF(ti.error_type = '', 'orphan_recovery', "
            "ti.error_type), "
            "ti.error_message = IF(ti.error_message = '', "
            "'Marked failed during crash recovery', ti.error_message) "
            "WHERE r.state IN ({}, {}) AND ti.state IN ({})",
            util::enum_to_code(TaskState::Failed),
            util::enum_to_code(DAGRunState::Queued),
            util::enum_to_code(DAGRunState::Running),
            util::enum_to_code(TaskState::Running)),
        res, use_awaitable);

    boost::mysql::results run_res;
    co_await conn_res->get().async_execute(
        boost::mysql::with_params(
            "UPDATE dag_runs SET state = {} WHERE state IN ({}, {})",
            util::enum_to_code(DAGRunState::Failed),
            util::enum_to_code(DAGRunState::Queued),
            util::enum_to_code(DAGRunState::Running)),
        run_res, use_awaitable);

    conn_res->return_without_reset();
    co_return ok(static_cast<std::size_t>(run_res.affected_rows()));
  });
}

auto MySQLDatabase::claim_task_instances(std::size_t limit,
                                         std::string_view worker_id)
    -> task<Result<std::vector<ClaimedTaskInstance>>> {
  co_return co_await mysql_try([&]() -> task<Result<
                                         std::vector<ClaimedTaskInstance>>> {
    auto conn_res = co_await get_connection();
    if (!conn_res) {
      co_return fail(conn_res.error());
    }

    auto &conn = conn_res->get();
    boost::mysql::results tx_res;
    co_await conn.async_execute("START TRANSACTION", tx_res, use_awaitable);

    boost::mysql::results sel_res;
    co_await conn.async_execute(
        boost::mysql::with_params(
            "SELECT r.dag_run_id, ti.run_rowid, ti.task_rowid, ti.attempt "
            "FROM task_instances ti "
            "JOIN dag_runs r ON r.run_rowid = ti.run_rowid "
            "WHERE ti.state IN ({}, {}) "
            "ORDER BY r.execution_date ASC "
            "LIMIT {} FOR UPDATE SKIP LOCKED",
            util::enum_to_code(TaskState::Retrying),
            util::enum_to_code(TaskState::Pending), limit),
        sel_res, use_awaitable);

    if (sel_res.rows().empty()) {
      boost::mysql::results commit_res;
      co_await conn.async_execute("COMMIT", commit_res, use_awaitable);
      conn_res->return_without_reset();
      co_return ok(std::vector<ClaimedTaskInstance>{});
    }

    std::vector<ClaimedTaskInstance> claimed;
    claimed.reserve(sel_res.rows().size());
    std::vector<std::tuple<int64_t, int64_t, int>> claim_keys;
    claim_keys.reserve(sel_res.rows().size());
    const auto now_ms = to_millis(std::chrono::system_clock::now());

    for (auto row : sel_res.rows()) {
      ClaimedTaskInstance c{
          .dag_run_id = DAGRunId{as_str(row.at(0))},
          .run_rowid = as_i64(row.at(1)),
          .task_rowid = as_i64(row.at(2)),
          .attempt = static_cast<int>(as_i64(row.at(3))),
      };
      claim_keys.emplace_back(c.run_rowid, c.task_rowid, c.attempt);
      claimed.emplace_back(std::move(c));
    }

    auto format_claim_key = [](const std::tuple<int64_t, int64_t, int> &k,
                               boost::mysql::format_context_base &ctx) {
      boost::mysql::format_sql_to(ctx, "({}, {}, {})", std::get<0>(k),
                                  std::get<1>(k), std::get<2>(k));
    };

    boost::mysql::results upd_res;
    co_await conn.async_execute(
        boost::mysql::with_params(
            "UPDATE task_instances "
            "SET state={}, worker_id={}, last_heartbeat={}, "
            "updated_at={}, started_at=IF(started_at=0, {}, started_at) "
            "WHERE (run_rowid, task_rowid, attempt) IN ({}) "
            "AND state IN ({}, {})",
            util::enum_to_code(TaskState::Running), worker_id, now_ms, now_ms,
            now_ms,
            boost::mysql::sequence(std::cref(claim_keys), format_claim_key),
            util::enum_to_code(TaskState::Retrying),
            util::enum_to_code(TaskState::Pending)),
        upd_res, use_awaitable);

    if (upd_res.affected_rows() != claim_keys.size()) {
      boost::mysql::results verify_res;
      co_await conn.async_execute(
          boost::mysql::with_params(
              "SELECT r.dag_run_id, ti.run_rowid, ti.task_rowid, ti.attempt "
              "FROM task_instances ti "
              "JOIN dag_runs r ON r.run_rowid = ti.run_rowid "
              "WHERE (ti.run_rowid, ti.task_rowid, ti.attempt) IN ({}) "
              "AND ti.state = {}",
              boost::mysql::sequence(std::cref(claim_keys), format_claim_key),
              util::enum_to_code(TaskState::Running)),
          verify_res, use_awaitable);
      claimed.clear();
      claimed.reserve(verify_res.rows().size());
      for (auto row : verify_res.rows()) {
        claimed.emplace_back(ClaimedTaskInstance{
            .dag_run_id = DAGRunId{as_str(row.at(0))},
            .run_rowid = as_i64(row.at(1)),
            .task_rowid = as_i64(row.at(2)),
            .attempt = static_cast<int>(as_i64(row.at(3))),
        });
      }
    }

    boost::mysql::results commit_res;
    co_await conn.async_execute("COMMIT", commit_res, use_awaitable);
    conn_res->return_without_reset();
    co_return ok(std::move(claimed));
  });
}

auto MySQLDatabase::touch_task_heartbeat(const DAGRunId &run_id,
                                         int64_t task_rowid, int attempt)
    -> task<Result<void>> {
  co_return co_await mysql_try([&]() -> task<Result<void>> {
    auto conn_res = co_await get_connection();
    if (!conn_res) {
      co_return fail(conn_res.error());
    }

    boost::mysql::results run_res;
    co_await conn_res->get().async_execute(
        boost::mysql::with_params(
            "SELECT run_rowid FROM dag_runs WHERE dag_run_id = {}",
            run_id.str()),
        run_res, use_awaitable);
    if (run_res.rows().empty()) {
      co_return fail(Error::NotFound);
    }

    const auto run_rowid = as_i64(run_res.rows().at(0).at(0));
    const auto now_ms = to_millis(std::chrono::system_clock::now());

    boost::mysql::results upd_res;
    co_await conn_res->get().async_execute(
        boost::mysql::with_params(
            "UPDATE task_instances SET last_heartbeat={}, updated_at={} "
            "WHERE run_rowid={} AND task_rowid={} AND attempt={} "
            "AND state={}",
            now_ms, now_ms, run_rowid, task_rowid, attempt,
            util::enum_to_code(TaskState::Running)),
        upd_res, use_awaitable);

    if (upd_res.affected_rows() == 0) {
      // MySQL may report 0 affected rows when values are unchanged (e.g. same
      // millisecond heartbeat). Probe existence before treating as not found.
      boost::mysql::results exists_res;
      co_await conn_res->get().async_execute(
          boost::mysql::with_params(
              "SELECT 1 FROM task_instances "
              "WHERE run_rowid={} AND task_rowid={} AND attempt={} "
              "AND state={} LIMIT 1",
              run_rowid, task_rowid, attempt,
              util::enum_to_code(TaskState::Running)),
          exists_res, use_awaitable);
      conn_res->return_without_reset();
      if (exists_res.rows().empty()) {
        co_return fail(Error::NotFound);
      }
      co_return ok();
    }

    conn_res->return_without_reset();
    co_return ok();
  });
}

auto MySQLDatabase::reap_zombie_task_instances(
    std::int64_t heartbeat_timeout_ms) -> task<Result<std::size_t>> {
  co_return co_await mysql_try([&]() -> task<Result<std::size_t>> {
    auto conn_res = co_await get_connection();
    if (!conn_res) {
      co_return fail(conn_res.error());
    }

    const auto now_ms = to_millis(std::chrono::system_clock::now());
    const auto cutoff_ms = now_ms - heartbeat_timeout_ms;

    boost::mysql::results res;
    co_await conn_res->get().async_execute(
        boost::mysql::with_params(
            "UPDATE task_instances "
            "SET state={}, finished_at={}, updated_at={}, "
            "exit_code=IF(exit_code=0,1,exit_code), "
            "error_type=IF(error_type='', 'zombie_reaper', error_type), "
            "error_message=IF(error_message='', "
            "'Zombie task reaped due to heartbeat timeout', error_message) "
            "WHERE state={} "
            "AND ((last_heartbeat > 0 AND last_heartbeat < {}) "
            "OR (last_heartbeat = 0 AND started_at > 0 AND started_at < {}))",
            util::enum_to_code(TaskState::Failed), now_ms, now_ms,
            util::enum_to_code(TaskState::Running), cutoff_ms, cutoff_ms),
        res, use_awaitable);

    conn_res->return_without_reset();
    co_return ok(static_cast<std::size_t>(res.affected_rows()));
  });
}

auto MySQLDatabase::append_task_log(const DAGRunId &run_id,
                                    const TaskId &task_id, int attempt,
                                    std::string_view stream,
                                    std::string_view content)
    -> task<Result<void>> {
  co_return co_await mysql_try([&]() -> task<Result<void>> {
    auto conn_res = co_await get_connection();
    if (!conn_res)
      co_return fail(conn_res.error());

    boost::mysql::results run_res;
    co_await conn_res->get().async_execute(
        boost::mysql::with_params(
            "SELECT r.run_rowid, t.task_rowid "
            "FROM dag_runs r "
            "JOIN dag_tasks t ON t.dag_rowid = r.dag_rowid AND t.task_id = {} "
            "WHERE r.dag_run_id = {} LIMIT 1",
            task_id.str(), run_id.str()),
        run_res, use_awaitable);

    if (run_res.rows().empty())
      co_return fail(Error::NotFound);

    const auto run_rowid = as_i64(run_res.rows().at(0).at(0));
    const auto task_rowid = as_i64(run_res.rows().at(0).at(1));
    const auto now_ms = to_millis(std::chrono::system_clock::now());

    boost::mysql::results ins_res;
    co_await conn_res->get().async_execute(
        boost::mysql::with_params(
            "INSERT INTO task_logs "
            "(run_rowid, task_rowid, attempt, stream, logged_at, content) "
            "VALUES ({}, {}, {}, {}, {}, {})",
            run_rowid, task_rowid, attempt, std::string(stream), now_ms,
            std::string(content)),
        ins_res, use_awaitable);

    conn_res->return_without_reset();
    co_return ok();
  });
}

auto MySQLDatabase::get_task_logs(const DAGRunId &run_id, const TaskId &task_id,
                                  int attempt, std::size_t limit)
    -> task<Result<std::vector<orm::TaskLogEntry>>> {
  co_return co_await mysql_try([&]()
                                   -> task<
                                       Result<std::vector<orm::TaskLogEntry>>> {
    auto conn_res = co_await get_connection();
    if (!conn_res)
      co_return fail(conn_res.error());

    boost::mysql::results res;
    co_await conn_res->get().async_execute(
        boost::mysql::with_params(
            "SELECT tl.log_rowid, tl.attempt, tl.stream, tl.logged_at, "
            "tl.content "
            "FROM task_logs tl "
            "JOIN dag_runs r ON r.run_rowid = tl.run_rowid "
            "JOIN dag_tasks t ON t.task_rowid = tl.task_rowid "
            "WHERE r.dag_run_id = {} AND t.task_id = {} AND tl.attempt = {} "
            "ORDER BY tl.logged_at ASC, tl.log_rowid ASC "
            "LIMIT {}",
            run_id.str(), task_id.str(), attempt,
            static_cast<std::int64_t>(limit)),
        res, use_awaitable);

    std::vector<orm::TaskLogEntry> out;
    out.reserve(res.rows().size());
    for (auto row : res.rows()) {
      out.emplace_back(orm::TaskLogEntry{
          .log_rowid = as_i64(row.at(0)),
          .dag_run_id = run_id.clone(),
          .task_id = task_id.clone(),
          .attempt = static_cast<int>(as_i64(row.at(1))),
          .stream = as_str(row.at(2)),
          .logged_at = from_millis(as_i64(row.at(3))),
          .content = as_str(row.at(4)),
      });
    }
    conn_res->return_without_reset();
    co_return ok(std::move(out));
  });
}

auto MySQLDatabase::get_run_logs(const DAGRunId &run_id, std::size_t limit)
    -> task<Result<std::vector<orm::TaskLogEntry>>> {
  co_return co_await mysql_try(
      [&]() -> task<Result<std::vector<orm::TaskLogEntry>>> {
        auto conn_res = co_await get_connection();
        if (!conn_res)
          co_return fail(conn_res.error());

        boost::mysql::results res;
        co_await conn_res->get().async_execute(
            boost::mysql::with_params(
                "SELECT tl.log_rowid, t.task_id, tl.attempt, tl.stream, "
                "tl.logged_at, tl.content "
                "FROM task_logs tl "
                "JOIN dag_runs r ON r.run_rowid = tl.run_rowid "
                "JOIN dag_tasks t ON t.task_rowid = tl.task_rowid "
                "WHERE r.dag_run_id = {} "
                "ORDER BY tl.logged_at ASC, tl.log_rowid ASC "
                "LIMIT {}",
                run_id.str(), static_cast<std::int64_t>(limit)),
            res, use_awaitable);

        std::vector<orm::TaskLogEntry> out;
        out.reserve(res.rows().size());
        for (auto row : res.rows()) {
          out.emplace_back(orm::TaskLogEntry{
              .log_rowid = as_i64(row.at(0)),
              .dag_run_id = run_id.clone(),
              .task_id = TaskId{as_str(row.at(1))},
              .attempt = static_cast<int>(as_i64(row.at(2))),
              .stream = as_str(row.at(3)),
              .logged_at = from_millis(as_i64(row.at(4))),
              .content = as_str(row.at(5)),
          });
        }
        conn_res->return_without_reset();
        co_return ok(std::move(out));
      });
}

} // namespace dagforge::storage

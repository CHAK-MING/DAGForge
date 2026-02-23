#pragma once

#include <cstddef>
#include <optional>
#include <string>

namespace dagforge::cli {
struct ServeStartOptions {
  std::string config_file;
  std::optional<std::string> log_file;
  std::optional<std::string> log_level;
  bool no_api{false};
  bool daemon{false};
  std::optional<int> shards;
};

struct ServeStopOptions {
  std::string config_file;
  int timeout_sec{10};
  bool force{false};
};

struct ServeStatusOptions {
  std::string config_file;
  bool json{false};
};

struct TriggerOptions {
  std::string config_file;
  std::string dag_id;
  std::string execution_date;
  bool no_api{false};
  bool wait{false};
  bool json{false};
};

struct ListDagsOptions {
  std::string config_file;
  std::string output{"table"}; // table|json
  bool json{false};
  bool include_stale{false};
  std::size_t limit{50};
};

struct ListRunsOptions {
  std::string config_file;
  std::string dag_id;          // Optional: filter by DAG
  std::string state;           // Optional: success/failed/running
  std::string output{"table"}; // table|json
  bool json{false};
  std::size_t limit{50};
};

struct ListTasksOptions {
  std::string config_file;
  std::string dag_id;
  std::string output{"table"}; // table|json
  bool json{false};
};

struct ClearOptions {
  std::string config_file;
  std::string dag_id;
  std::string run_id;
  std::optional<std::string> task;
  bool failed_only{false};
  bool all_tasks{false};
  bool downstream{false};
  bool dry_run{false};
  bool json{false};
};

struct PauseOptions {
  std::string config_file;
  std::string dag_id;
  bool json{false};
};

struct UnpauseOptions {
  std::string config_file;
  std::string dag_id;
  bool json{false};
};

struct InspectOptions {
  std::string config_file;
  std::string dag_id;
  std::string run_id;
  bool latest{false};  // Use the most recent run
  bool xcom{false};    // Show XCom variables
  bool details{false}; // Show execution time histogram
  bool json{false};
};

struct LogsOptions {
  std::string config_file;
  std::string dag_id;
  std::string run_id;
  std::string task_id;    // Optional: filter by task
  int attempt{1};         // Attempt number (default: 1)
  bool latest{false};     // Use the most recent run
  bool follow{false};     // Poll for new log lines
  bool short_time{false}; // Compact timestamp display
  bool json{false};
};

struct DbOptions {
  std::string config_file;
  bool dry_run{false};
};

struct ValidateOptions {
  std::string config_file;
  std::optional<std::string> file; // Specific TOML file
  bool json{false};
};

struct TestTaskOptions {
  std::string config_file;
  std::string dag_id;
  std::string task_id;
  bool json{false};
};

[[nodiscard]] auto cmd_serve_start(const ServeStartOptions &opts) -> int;
[[nodiscard]] auto cmd_serve_stop(const ServeStopOptions &opts) -> int;
[[nodiscard]] auto cmd_serve_status(const ServeStatusOptions &opts) -> int;
[[nodiscard]] auto cmd_trigger(const TriggerOptions &opts) -> int;
[[nodiscard]] auto cmd_list_dags(const ListDagsOptions &opts) -> int;
[[nodiscard]] auto cmd_list_runs(const ListRunsOptions &opts) -> int;
[[nodiscard]] auto cmd_list_tasks(const ListTasksOptions &opts) -> int;
[[nodiscard]] auto cmd_clear(const ClearOptions &opts) -> int;
[[nodiscard]] auto cmd_pause(const PauseOptions &opts) -> int;
[[nodiscard]] auto cmd_unpause(const UnpauseOptions &opts) -> int;
[[nodiscard]] auto cmd_inspect(const InspectOptions &opts) -> int;
[[nodiscard]] auto cmd_validate(const ValidateOptions &opts) -> int;
[[nodiscard]] auto cmd_db_init(const DbOptions &opts) -> int;
[[nodiscard]] auto cmd_db_migrate(const DbOptions &opts) -> int;
[[nodiscard]] auto cmd_db_prune_stale(const DbOptions &opts) -> int;
[[nodiscard]] auto cmd_logs(const LogsOptions &opts) -> int;
[[nodiscard]] auto cmd_test_task(const TestTaskOptions &opts) -> int;

} // namespace dagforge::cli

#include "dagforge/cli/commands.hpp"
#include "dagforge/util/log.hpp"

#include <CLI/CLI.hpp>

#include <cstdio>
#include <cstdlib>
#include <exception>
#include <string>
#include <string_view>

namespace {

using namespace dagforge::cli;

struct CliOptionStore {
  ServeStartOptions serve_start;
  ServeStatusOptions serve_status;
  ServeStopOptions serve_stop;
  TriggerOptions trigger;
  TestTaskOptions test_task;
  ListDagsOptions list_dags;
  ListRunsOptions list_runs;
  ListTasksOptions list_tasks;
  ClearOptions clear;
  PauseOptions pause;
  UnpauseOptions unpause;
  InspectOptions inspect;
  LogsOptions logs;
  ValidateOptions validate;
  DbOptions db_init;
  DbOptions db_migrate;
  DbOptions db_prune_stale;
};

auto default_config() -> std::string {
  if (const char *env = std::getenv("DAGFORGE_CONFIG"); env && *env) {
    return env;
  }
  return {};
}

template <typename Options>
auto bind_config_option(CLI::App &cmd, Options &opts,
                        const std::string &env_config,
                        bool required_if_env_missing = true)
    -> CLI::Option * {
  opts.config_file = env_config;
  auto *opt =
      cmd.add_option("-c,--config", opts.config_file, "System config file")
          ->check(CLI::ExistingFile);
  if (required_if_env_missing && env_config.empty()) {
    opt->required();
  }
  return opt;
}

template <typename Options>
auto bind_json_flag(CLI::App &cmd, Options &opts) -> void {
  cmd.add_flag("--json", opts.json, "Output JSON");
}

auto bind_output_option(CLI::App &cmd, std::string &output) -> void {
  cmd.add_option("--output", output, "Output format: table|json")
      ->check(CLI::IsMember({"table", "json"}, CLI::ignore_case));
}

auto bind_limit_option(CLI::App &cmd, std::size_t &limit) -> void {
  cmd.add_option("--limit", limit, "Max records to display (default: 50)")
      ->check(CLI::PositiveNumber);
}

template <typename Func>
auto bind_exit_callback(CLI::App &cmd, Func &&func) -> void {
  cmd.callback(
      [func = std::forward<Func>(func)]() mutable { std::exit(func()); });
}

auto configure_terminate_handler() -> void {
  std::set_terminate([] {
    if (auto eptr = std::current_exception(); eptr) {
      try {
        std::rethrow_exception(eptr);
      } catch (const std::exception &e) {
        dagforge::log::error("Unhandled fatal exception: {}", e.what());
      } catch (...) {
        dagforge::log::error("Unhandled fatal non-std exception");
      }
    } else {
      dagforge::log::error("std::terminate called without active exception");
    }
    std::abort();
  });
}

auto register_serve_commands(CLI::App &app, const std::string &env_config,
                             CliOptionStore &store) -> void {
  auto *serve = app.add_subcommand("serve", "Service lifecycle operations");
  serve->require_subcommand(1);
  serve->footer("\nExamples:\n"
                "  dagforge serve start -c system_config.toml\n"
                "  dagforge serve start -c system_config.toml --daemon "
                "--log-file dagforge.log\n"
                "  dagforge serve status -c system_config.toml\n"
                "  dagforge serve stop -c system_config.toml");

  auto *serve_start = serve->add_subcommand("start", "Start DAGForge service");
  bind_config_option(*serve_start, store.serve_start, env_config);
  serve_start->add_flag("--no-api", store.serve_start.no_api,
                        "Disable REST API");
  serve_start->add_option("--pid-file", store.serve_start.pid_file,
                          "PID file path override");
  serve_start->add_option("--log-file", store.serve_start.log_file,
                          "Log file path (required for --daemon)");
  serve_start->add_option("--log-level", store.serve_start.log_level,
                          "Log level override: trace|debug|info|warn|error");
  serve_start->add_flag("-d,--daemon", store.serve_start.daemon,
                        "Run as daemon");
  serve_start
      ->add_option("--shards", store.serve_start.shards,
                   "Number of shards (default: auto-detect CPU cores)")
      ->check(CLI::PositiveNumber);
  bind_exit_callback(*serve_start,
                     [&opts = store.serve_start]() { return cmd_serve_start(opts); });

  auto *serve_status =
      serve->add_subcommand("status", "Show DAGForge service status");
  bind_config_option(*serve_status, store.serve_status, env_config);
  serve_status->add_option("--pid-file", store.serve_status.pid_file,
                           "PID file path override");
  bind_json_flag(*serve_status, store.serve_status);
  bind_exit_callback(*serve_status,
                     [&opts = store.serve_status]() { return cmd_serve_status(opts); });

  auto *serve_stop = serve->add_subcommand("stop", "Stop DAGForge service");
  bind_config_option(*serve_stop, store.serve_stop, env_config);
  serve_stop->add_option("--pid-file", store.serve_stop.pid_file,
                         "PID file path override");
  serve_stop
      ->add_option("--timeout", store.serve_stop.timeout_sec,
                   "Seconds to wait before failing or forcing stop")
      ->check(CLI::PositiveNumber);
  serve_stop->add_flag("--force", store.serve_stop.force,
                       "Send SIGKILL if graceful stop times out");
  bind_exit_callback(*serve_stop,
                     [&opts = store.serve_stop]() { return cmd_serve_stop(opts); });
}

auto register_run_commands(CLI::App &app, const std::string &env_config,
                           CliOptionStore &store) -> void {
  auto *trigger = app.add_subcommand("trigger", "Trigger a DAG run");
  trigger->footer(
      "\nExamples:\n"
      "  dagforge trigger -c system_config.toml my_pipeline\n"
      "  dagforge trigger -c system_config.toml my_pipeline --wait\n"
      "  dagforge trigger -c system_config.toml my_pipeline --json");
  bind_config_option(*trigger, store.trigger, env_config);
  trigger->add_option("dag_id", store.trigger.dag_id, "DAG ID")->required();
  trigger->add_option("-e,--execution-date", store.trigger.execution_date,
                      "Logical execution time (ISO8601 or 'now')");
  trigger->add_flag("--no-api", store.trigger.no_api,
                    "Skip API trigger path and force local one-shot execution");
  trigger->add_flag("--wait", store.trigger.wait,
                    "Block until run completes and return final status");
  bind_json_flag(*trigger, store.trigger);
  bind_exit_callback(*trigger,
                     [&opts = store.trigger]() { return cmd_trigger(opts); });

  auto *test_task = app.add_subcommand(
      "test", "Run one task in isolation (Airflow-style test)");
  test_task->footer(
      "\nExamples:\n"
      "  dagforge test -c system_config.toml my_pipeline task_a\n"
      "  dagforge test -c system_config.toml my_pipeline task_a --json");
  bind_config_option(*test_task, store.test_task, env_config);
  test_task->add_option("dag_id", store.test_task.dag_id, "DAG ID")
      ->required();
  test_task->add_option("task_id", store.test_task.task_id, "Task ID")
      ->required();
  bind_json_flag(*test_task, store.test_task);
  bind_exit_callback(*test_task,
                     [&opts = store.test_task]() { return cmd_test_task(opts); });
}

auto register_list_commands(CLI::App &app, const std::string &env_config,
                            CliOptionStore &store) -> void {
  auto *list = app.add_subcommand("list", "List resources");
  list->require_subcommand(1);
  list->footer(
      "\nExamples:\n"
      "  dagforge list dags -c system_config.toml\n"
      "  dagforge list dags -c system_config.toml --output json\n"
      "  dagforge list runs -c system_config.toml my_pipeline --state failed\n"
      "  dagforge list tasks -c system_config.toml my_pipeline");

  auto *list_dags = list->add_subcommand("dags", "List all DAGs");
  bind_config_option(*list_dags, store.list_dags, env_config);
  bind_json_flag(*list_dags, store.list_dags);
  bind_output_option(*list_dags, store.list_dags.output);
  list_dags->add_flag("--include-stale", store.list_dags.include_stale,
                      "Include DB-only stale DAGs in output");
  bind_limit_option(*list_dags, store.list_dags.limit);
  bind_exit_callback(*list_dags,
                     [&opts = store.list_dags]() { return cmd_list_dags(opts); });

  auto *list_runs = list->add_subcommand("runs", "List DAG runs");
  bind_config_option(*list_runs, store.list_runs, env_config);
  list_runs->add_option("dag_id", store.list_runs.dag_id,
                        "Filter by DAG ID (optional)");
  list_runs->add_option("--state", store.list_runs.state,
                        "Filter by state (success, failed, running)");
  bind_json_flag(*list_runs, store.list_runs);
  bind_output_option(*list_runs, store.list_runs.output);
  bind_limit_option(*list_runs, store.list_runs.limit);
  bind_exit_callback(*list_runs,
                     [&opts = store.list_runs]() { return cmd_list_runs(opts); });

  auto *list_tasks =
      list->add_subcommand("tasks", "List task definitions in a DAG");
  bind_config_option(*list_tasks, store.list_tasks, env_config);
  list_tasks->add_option(
      "dag_id", store.list_tasks.dag_id,
      "DAG ID (optional; lists tasks for all DAGs if omitted)");
  bind_json_flag(*list_tasks, store.list_tasks);
  bind_output_option(*list_tasks, store.list_tasks.output);
  bind_exit_callback(*list_tasks,
                     [&opts = store.list_tasks]() { return cmd_list_tasks(opts); });
}

auto register_state_commands(CLI::App &app, const std::string &env_config,
                             CliOptionStore &store) -> void {
  auto *clear = app.add_subcommand("clear", "Clear task states for re-execution");
  clear->footer(
      "\nExamples:\n"
      "  dagforge clear -c system_config.toml my_dag --run <run_id> --failed\n"
      "  dagforge clear -c system_config.toml my_dag --run <run_id> --failed "
      "--dry-run\n"
      "  dagforge clear -c system_config.toml my_dag --run <run_id> --task "
      "task_a");
  bind_config_option(*clear, store.clear, env_config);
  clear->add_option("dag_id", store.clear.dag_id, "DAG ID")->required();
  clear->add_option("--run", store.clear.run_id, "Run ID")->required();
  clear->add_option("--task", store.clear.task,
                    "Clear specific task (by task_id)");
  clear->add_flag("--failed", store.clear.failed_only,
                  "Only clear failed tasks");
  clear->add_flag("--all", store.clear.all_tasks, "Clear all tasks in the run");
  clear->add_flag("--downstream", store.clear.downstream,
                  "Also clear downstream tasks");
  clear->add_flag("--dry-run", store.clear.dry_run,
                  "Show what would be cleared without making changes");
  bind_json_flag(*clear, store.clear);
  bind_exit_callback(*clear,
                     [&opts = store.clear]() { return cmd_clear(opts); });

  auto *pause_cmd = app.add_subcommand("pause", "Pause DAG scheduling");
  pause_cmd->footer("\nExamples:\n"
                    "  dagforge pause -c system_config.toml my_dag");
  bind_config_option(*pause_cmd, store.pause, env_config);
  pause_cmd->add_option("dag_id", store.pause.dag_id, "DAG ID")->required();
  bind_json_flag(*pause_cmd, store.pause);
  bind_exit_callback(*pause_cmd,
                     [&opts = store.pause]() { return cmd_pause(opts); });

  auto *unpause_cmd = app.add_subcommand("unpause", "Resume DAG scheduling");
  unpause_cmd->footer("\nExamples:\n"
                      "  dagforge unpause -c system_config.toml my_dag");
  bind_config_option(*unpause_cmd, store.unpause, env_config);
  unpause_cmd->add_option("dag_id", store.unpause.dag_id, "DAG ID")
      ->required();
  bind_json_flag(*unpause_cmd, store.unpause);
  bind_exit_callback(*unpause_cmd,
                     [&opts = store.unpause]() { return cmd_unpause(opts); });

  auto *inspect = app.add_subcommand("inspect", "Inspect DAG run details");
  inspect->footer(
      "\nExamples:\n"
      "  dagforge inspect -c system_config.toml my_dag --latest\n"
      "  dagforge inspect -c system_config.toml my_dag --run latest\n"
      "  dagforge inspect -c system_config.toml my_dag --run <run_id> --xcom "
      "--details");
  bind_config_option(*inspect, store.inspect, env_config);
  inspect->add_option("dag_id", store.inspect.dag_id, "DAG ID")->required();
  inspect->add_option("--run", store.inspect.run_id, "Run ID or 'latest'");
  inspect->add_flag("--latest", store.inspect.latest,
                    "Use the most recent run (shortcut for --run latest)");
  inspect->add_flag("--xcom", store.inspect.xcom, "Show XCom variables");
  inspect->add_flag("--details", store.inspect.details,
                    "Show execution time histogram");
  bind_json_flag(*inspect, store.inspect);
  bind_exit_callback(*inspect,
                     [&opts = store.inspect]() { return cmd_inspect(opts); });

  auto *logs_cmd = app.add_subcommand("logs", "View task execution logs");
  logs_cmd->footer(
      "\nExamples:\n"
      "  dagforge logs -c system_config.toml my_dag --latest\n"
      "  dagforge logs -c system_config.toml my_dag --run latest\n"
      "  dagforge logs -c system_config.toml my_dag --latest --task task_a -f\n"
      "  dagforge logs -c system_config.toml my_dag --run <run_id> "
      "--short-time");
  bind_config_option(*logs_cmd, store.logs, env_config);
  logs_cmd->add_option("dag_id", store.logs.dag_id, "DAG ID")->required();
  logs_cmd->add_option("--run", store.logs.run_id, "Run ID or 'latest'");
  logs_cmd->add_option("--task", store.logs.task_id, "Filter by task ID");
  logs_cmd
      ->add_option("--attempt", store.logs.attempt,
                   "Attempt number (default: 1)")
      ->check(CLI::PositiveNumber);
  logs_cmd->add_flag("--latest", store.logs.latest, "Use the most recent run");
  logs_cmd->add_flag("--follow,-f", store.logs.follow,
                     "Poll for new log lines until run completes");
  logs_cmd->add_flag("--short-time", store.logs.short_time,
                     "Use compact timestamp display");
  bind_json_flag(*logs_cmd, store.logs);
  bind_exit_callback(*logs_cmd,
                     [&opts = store.logs]() { return cmd_logs(opts); });
}

auto register_validate_commands(CLI::App &app, const std::string &env_config,
                                CliOptionStore &store) -> void {
  auto *validate = app.add_subcommand("validate", "Validate DAG files");
  validate->footer("\nExamples:\n"
                   "  dagforge validate -c system_config.toml\n"
                   "  dagforge validate -f dags/my_dag.toml");
  bind_config_option(*validate, store.validate, env_config, false);
  validate
      ->add_option("-f,--file", store.validate.file,
                   "Specific TOML file to validate")
      ->check(CLI::ExistingFile);
  bind_json_flag(*validate, store.validate);
  validate->callback([&opts = store.validate]() {
    if (opts.config_file.empty() && !opts.file.has_value()) {
      std::fputs("Error: Either --config or --file is required\n", stderr);
      std::exit(1);
    }
    std::exit(cmd_validate(opts));
  });
}

auto register_db_commands(CLI::App &app, const std::string &env_config,
                          CliOptionStore &store) -> void {
  auto *db = app.add_subcommand("db", "Database management");
  db->require_subcommand(1);
  db->footer("\nExamples:\n"
             "  dagforge db init -c system_config.toml\n"
             "  dagforge db migrate -c system_config.toml\n"
             "  dagforge db prune-stale -c system_config.toml --dry-run");

  auto *db_init = db->add_subcommand("init", "Initialize database schema");
  bind_config_option(*db_init, store.db_init, env_config);
  bind_exit_callback(*db_init,
                     [&opts = store.db_init]() { return cmd_db_init(opts); });

  auto *db_migrate = db->add_subcommand("migrate", "Run database migrations");
  bind_config_option(*db_migrate, store.db_migrate, env_config);
  bind_exit_callback(*db_migrate,
                     [&opts = store.db_migrate]() { return cmd_db_migrate(opts); });

  auto *db_prune_stale = db->add_subcommand(
      "prune-stale",
      "Delete DB-only stale DAGs not present in DAG source directory");
  bind_config_option(*db_prune_stale, store.db_prune_stale, env_config);
  db_prune_stale->add_flag("--dry-run", store.db_prune_stale.dry_run,
                           "Show stale DAGs without deleting them");
  bind_exit_callback(*db_prune_stale, [&opts = store.db_prune_stale]() {
    return cmd_db_prune_stale(opts);
  });
}

} // namespace

int main(int argc, char *argv[]) {
  configure_terminate_handler();

  dagforge::log::set_output_stderr();
  dagforge::log::set_level(dagforge::log::Level::Warn);

  CLI::App app{"DAGForge", "A DAG-based Task Scheduler"};
  app.require_subcommand(1);
  app.footer("\nExamples:\n"
             "  dagforge serve start -c system_config.toml\n"
             "  dagforge trigger -c system_config.toml my_pipeline --wait\n"
             "\nTip: Set DAGFORGE_CONFIG=system_config.toml to skip -c on "
             "every command.");

  const std::string env_config = default_config();
  CliOptionStore store;

  register_serve_commands(app, env_config, store);
  register_run_commands(app, env_config, store);
  register_list_commands(app, env_config, store);
  register_state_commands(app, env_config, store);
  register_validate_commands(app, env_config, store);
  register_db_commands(app, env_config, store);

  CLI11_PARSE(app, argc, argv);
  return 0;
}

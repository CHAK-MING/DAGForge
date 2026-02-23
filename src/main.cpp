#include "dagforge/cli/commands.hpp"
#include "dagforge/util/log.hpp"

#include <CLI/CLI.hpp>

#include <cstdio>
#include <cstdlib>
#include <cstdlib> // getenv
#include <print>
#include <string>

namespace {
auto default_config() -> std::string {
  if (const char *env = std::getenv("DAGFORGE_CONFIG"); env && *env) {
    return env;
  }
  return {};
}
} // namespace

int main(int argc, char *argv[]) {
  // Keep non-serve CLI output clean by default.
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

  auto *serve = app.add_subcommand("serve", "Service lifecycle operations");
  serve->require_subcommand(1);
  serve->footer("\nExamples:\n"
                "  dagforge serve start -c system_config.toml\n"
                "  dagforge serve start -c system_config.toml --daemon "
                "--log-file dagforge.log\n"
                "  dagforge serve status -c system_config.toml\n"
                "  dagforge serve stop -c system_config.toml");

  dagforge::cli::ServeStartOptions serve_start_opts;
  auto *serve_start = serve->add_subcommand("start", "Start DAGForge service");
  serve_start_opts.config_file = env_config;
  auto *serve_start_cfg =
      serve_start
          ->add_option("-c,--config", serve_start_opts.config_file,
                       "System config file")
          ->check(CLI::ExistingFile);
  if (env_config.empty())
    serve_start_cfg->required();
  serve_start->add_flag("--no-api", serve_start_opts.no_api,
                        "Disable REST API");
  serve_start->add_option("--log-file", serve_start_opts.log_file,
                          "Log file path (required for --daemon)");
  serve_start->add_option("--log-level", serve_start_opts.log_level,
                          "Log level override: trace|debug|info|warn|error");
  serve_start->add_flag("-d,--daemon", serve_start_opts.daemon,
                        "Run as daemon");
  serve_start->add_option("--shards", serve_start_opts.shards,
                          "Number of shards (default: auto-detect CPU cores)");
  serve_start->callback([&serve_start_opts]() {
    std::exit(dagforge::cli::cmd_serve_start(serve_start_opts));
  });

  dagforge::cli::ServeStatusOptions serve_status_opts;
  auto *serve_status =
      serve->add_subcommand("status", "Show DAGForge service status");
  serve_status_opts.config_file = env_config;
  auto *serve_status_cfg =
      serve_status
          ->add_option("-c,--config", serve_status_opts.config_file,
                       "System config file")
          ->check(CLI::ExistingFile);
  if (env_config.empty())
    serve_status_cfg->required();
  serve_status->add_flag("--json", serve_status_opts.json, "Output JSON");
  serve_status->callback([&serve_status_opts]() {
    std::exit(dagforge::cli::cmd_serve_status(serve_status_opts));
  });

  dagforge::cli::ServeStopOptions serve_stop_opts;
  auto *serve_stop = serve->add_subcommand("stop", "Stop DAGForge service");
  serve_stop_opts.config_file = env_config;
  auto *serve_stop_cfg =
      serve_stop
          ->add_option("-c,--config", serve_stop_opts.config_file,
                       "System config file")
          ->check(CLI::ExistingFile);
  if (env_config.empty())
    serve_stop_cfg->required();
  serve_stop->add_option("--timeout", serve_stop_opts.timeout_sec,
                         "Seconds to wait before failing or forcing stop");
  serve_stop->add_flag("--force", serve_stop_opts.force,
                       "Send SIGKILL if graceful stop times out");
  serve_stop->callback([&serve_stop_opts]() {
    std::exit(dagforge::cli::cmd_serve_stop(serve_stop_opts));
  });

  dagforge::cli::TriggerOptions trigger_opts;
  auto *trigger = app.add_subcommand("trigger", "Trigger a DAG run");
  trigger->footer(
      "\nExamples:\n"
      "  dagforge trigger -c system_config.toml my_pipeline\n"
      "  dagforge trigger -c system_config.toml my_pipeline --wait\n"
      "  dagforge trigger -c system_config.toml my_pipeline --json");
  trigger_opts.config_file = env_config;
  auto *trigger_cfg = trigger
                          ->add_option("-c,--config", trigger_opts.config_file,
                                       "System config file")
                          ->check(CLI::ExistingFile);
  if (env_config.empty())
    trigger_cfg->required();
  trigger->add_option("dag_id", trigger_opts.dag_id, "DAG ID")->required();
  trigger->add_option("-e,--execution-date", trigger_opts.execution_date,
                      "Logical execution time (ISO8601 or 'now')");
  trigger->add_flag("--no-api", trigger_opts.no_api,
                    "Skip API trigger path and force local one-shot execution");
  trigger->add_flag("--wait", trigger_opts.wait,
                    "Block until run completes and return final status");
  trigger->add_flag("--json", trigger_opts.json, "Output JSON");
  trigger->callback([&trigger_opts]() {
    std::exit(dagforge::cli::cmd_trigger(trigger_opts));
  });

  dagforge::cli::TestTaskOptions test_task_opts;
  auto *test_task = app.add_subcommand(
      "test", "Run one task in isolation (Airflow-style test)");
  test_task->footer(
      "\nExamples:\n"
      "  dagforge test -c system_config.toml my_pipeline task_a\n"
      "  dagforge test -c system_config.toml my_pipeline task_a --json");
  test_task_opts.config_file = env_config;
  auto *test_task_cfg =
      test_task
          ->add_option("-c,--config", test_task_opts.config_file,
                       "System config file")
          ->check(CLI::ExistingFile);
  if (env_config.empty())
    test_task_cfg->required();
  test_task->add_option("dag_id", test_task_opts.dag_id, "DAG ID")->required();
  test_task->add_option("task_id", test_task_opts.task_id, "Task ID")
      ->required();
  test_task->add_flag("--json", test_task_opts.json, "Output JSON");
  test_task->callback([&test_task_opts]() {
    std::exit(dagforge::cli::cmd_test_task(test_task_opts));
  });

  auto *list = app.add_subcommand("list", "List resources");
  list->require_subcommand(1);
  list->footer(
      "\nExamples:\n"
      "  dagforge list dags -c system_config.toml\n"
      "  dagforge list dags -c system_config.toml --output json\n"
      "  dagforge list runs -c system_config.toml my_pipeline --state failed\n"
      "  dagforge list tasks -c system_config.toml my_pipeline");

  // list dags
  dagforge::cli::ListDagsOptions list_dags_opts;
  auto *list_dags = list->add_subcommand("dags", "List all DAGs");
  list_dags_opts.config_file = env_config;
  auto *list_dags_cfg =
      list_dags
          ->add_option("-c,--config", list_dags_opts.config_file,
                       "System config file")
          ->check(CLI::ExistingFile);
  if (env_config.empty())
    list_dags_cfg->required();
  list_dags->add_flag("--json", list_dags_opts.json, "Output JSON");
  list_dags
      ->add_option("--output", list_dags_opts.output,
                   "Output format: table|json")
      ->check(CLI::IsMember({"table", "json"}, CLI::ignore_case));
  list_dags->add_flag("--include-stale", list_dags_opts.include_stale,
                      "Include DB-only stale DAGs in output");
  list_dags->add_option("--limit", list_dags_opts.limit,
                        "Max records to display (default: 50)");
  list_dags->callback([&list_dags_opts]() {
    std::exit(dagforge::cli::cmd_list_dags(list_dags_opts));
  });

  // list runs
  dagforge::cli::ListRunsOptions list_runs_opts;
  auto *list_runs = list->add_subcommand("runs", "List DAG runs");
  list_runs_opts.config_file = env_config;
  auto *list_runs_cfg =
      list_runs
          ->add_option("-c,--config", list_runs_opts.config_file,
                       "System config file")
          ->check(CLI::ExistingFile);
  if (env_config.empty())
    list_runs_cfg->required();
  list_runs->add_option("dag_id", list_runs_opts.dag_id,
                        "Filter by DAG ID (optional)");
  list_runs->add_option("--state", list_runs_opts.state,
                        "Filter by state (success, failed, running)");
  list_runs->add_flag("--json", list_runs_opts.json, "Output JSON");
  list_runs
      ->add_option("--output", list_runs_opts.output,
                   "Output format: table|json")
      ->check(CLI::IsMember({"table", "json"}, CLI::ignore_case));
  list_runs->add_option("--limit", list_runs_opts.limit,
                        "Max records to display (default: 50)");
  list_runs->callback([&list_runs_opts]() {
    std::exit(dagforge::cli::cmd_list_runs(list_runs_opts));
  });

  // list tasks
  dagforge::cli::ListTasksOptions list_tasks_opts;
  auto *list_tasks =
      list->add_subcommand("tasks", "List task definitions in a DAG");
  list_tasks_opts.config_file = env_config;
  auto *list_tasks_cfg =
      list_tasks
          ->add_option("-c,--config", list_tasks_opts.config_file,
                       "System config file")
          ->check(CLI::ExistingFile);
  if (env_config.empty())
    list_tasks_cfg->required();
  list_tasks->add_option(
      "dag_id", list_tasks_opts.dag_id,
      "DAG ID (optional; lists tasks for all DAGs if omitted)");
  list_tasks->add_flag("--json", list_tasks_opts.json, "Output JSON");
  list_tasks
      ->add_option("--output", list_tasks_opts.output,
                   "Output format: table|json")
      ->check(CLI::IsMember({"table", "json"}, CLI::ignore_case));
  list_tasks->callback([&list_tasks_opts]() {
    std::exit(dagforge::cli::cmd_list_tasks(list_tasks_opts));
  });

  dagforge::cli::ClearOptions clear_opts;
  auto *clear =
      app.add_subcommand("clear", "Clear task states for re-execution");
  clear->footer(
      "\nExamples:\n"
      "  dagforge clear -c system_config.toml my_dag --run <run_id> --failed\n"
      "  dagforge clear -c system_config.toml my_dag --run <run_id> --failed "
      "--dry-run\n"
      "  dagforge clear -c system_config.toml my_dag --run <run_id> --task "
      "task_a");
  clear_opts.config_file = env_config;
  auto *clear_cfg = clear
                        ->add_option("-c,--config", clear_opts.config_file,
                                     "System config file")
                        ->check(CLI::ExistingFile);
  if (env_config.empty())
    clear_cfg->required();
  clear->add_option("dag_id", clear_opts.dag_id, "DAG ID")->required();
  clear->add_option("--run", clear_opts.run_id, "Run ID")->required();
  clear->add_option("--task", clear_opts.task,
                    "Clear specific task (by task_id)");
  clear->add_flag("--failed", clear_opts.failed_only,
                  "Only clear failed tasks");
  clear->add_flag("--all", clear_opts.all_tasks, "Clear all tasks in the run");
  clear->add_flag("--downstream", clear_opts.downstream,
                  "Also clear downstream tasks");
  clear->add_flag("--dry-run", clear_opts.dry_run,
                  "Show what would be cleared without making changes");
  clear->add_flag("--json", clear_opts.json, "Output JSON");
  clear->callback(
      [&clear_opts]() { std::exit(dagforge::cli::cmd_clear(clear_opts)); });

  dagforge::cli::PauseOptions pause_opts;
  auto *pause_cmd = app.add_subcommand("pause", "Pause DAG scheduling");
  pause_cmd->footer("\nExamples:\n"
                    "  dagforge pause -c system_config.toml my_dag");
  pause_opts.config_file = env_config;
  auto *pause_cfg = pause_cmd
                        ->add_option("-c,--config", pause_opts.config_file,
                                     "System config file")
                        ->check(CLI::ExistingFile);
  if (env_config.empty())
    pause_cfg->required();
  pause_cmd->add_option("dag_id", pause_opts.dag_id, "DAG ID")->required();
  pause_cmd->add_flag("--json", pause_opts.json, "Output JSON");
  pause_cmd->callback(
      [&pause_opts]() { std::exit(dagforge::cli::cmd_pause(pause_opts)); });

  dagforge::cli::UnpauseOptions unpause_opts;
  auto *unpause_cmd = app.add_subcommand("unpause", "Resume DAG scheduling");
  unpause_cmd->footer("\nExamples:\n"
                      "  dagforge unpause -c system_config.toml my_dag");
  unpause_opts.config_file = env_config;
  auto *unpause_cfg = unpause_cmd
                          ->add_option("-c,--config", unpause_opts.config_file,
                                       "System config file")
                          ->check(CLI::ExistingFile);
  if (env_config.empty())
    unpause_cfg->required();
  unpause_cmd->add_option("dag_id", unpause_opts.dag_id, "DAG ID")->required();
  unpause_cmd->add_flag("--json", unpause_opts.json, "Output JSON");
  unpause_cmd->callback([&unpause_opts]() {
    std::exit(dagforge::cli::cmd_unpause(unpause_opts));
  });

  dagforge::cli::InspectOptions inspect_opts;
  auto *inspect = app.add_subcommand("inspect", "Inspect DAG run details");
  inspect->footer(
      "\nExamples:\n"
      "  dagforge inspect -c system_config.toml my_dag --latest\n"
      "  dagforge inspect -c system_config.toml my_dag --run latest\n"
      "  dagforge inspect -c system_config.toml my_dag --run <run_id> --xcom "
      "--details");
  inspect_opts.config_file = env_config;
  auto *inspect_cfg = inspect
                          ->add_option("-c,--config", inspect_opts.config_file,
                                       "System config file")
                          ->check(CLI::ExistingFile);
  if (env_config.empty())
    inspect_cfg->required();
  inspect->add_option("dag_id", inspect_opts.dag_id, "DAG ID")->required();
  inspect->add_option("--run", inspect_opts.run_id, "Run ID or 'latest'");
  inspect->add_flag("--latest", inspect_opts.latest,
                    "Use the most recent run (shortcut for --run latest)");
  inspect->add_flag("--xcom", inspect_opts.xcom, "Show XCom variables");
  inspect->add_flag("--details", inspect_opts.details,
                    "Show execution time histogram");
  inspect->add_flag("--json", inspect_opts.json, "Output JSON");
  inspect->callback([&inspect_opts]() {
    std::exit(dagforge::cli::cmd_inspect(inspect_opts));
  });

  dagforge::cli::LogsOptions logs_opts;
  auto *logs_cmd = app.add_subcommand("logs", "View task execution logs");
  logs_cmd->footer(
      "\nExamples:\n"
      "  dagforge logs -c system_config.toml my_dag --latest\n"
      "  dagforge logs -c system_config.toml my_dag --run latest\n"
      "  dagforge logs -c system_config.toml my_dag --latest --task task_a -f\n"
      "  dagforge logs -c system_config.toml my_dag --run <run_id> "
      "--short-time");
  logs_opts.config_file = env_config;
  auto *logs_cfg = logs_cmd
                       ->add_option("-c,--config", logs_opts.config_file,
                                    "System config file")
                       ->check(CLI::ExistingFile);
  if (env_config.empty())
    logs_cfg->required();
  logs_cmd->add_option("dag_id", logs_opts.dag_id, "DAG ID")->required();
  logs_cmd->add_option("--run", logs_opts.run_id, "Run ID or 'latest'");
  logs_cmd->add_option("--task", logs_opts.task_id, "Filter by task ID");
  logs_cmd->add_option("--attempt", logs_opts.attempt,
                       "Attempt number (default: 1)");
  logs_cmd->add_flag("--latest", logs_opts.latest, "Use the most recent run");
  logs_cmd->add_flag("--follow,-f", logs_opts.follow,
                     "Poll for new log lines until run completes");
  logs_cmd->add_flag("--short-time", logs_opts.short_time,
                     "Use compact timestamp display");
  logs_cmd->add_flag("--json", logs_opts.json, "Output JSON");
  logs_cmd->callback(
      [&logs_opts]() { std::exit(dagforge::cli::cmd_logs(logs_opts)); });

  dagforge::cli::ValidateOptions validate_opts;
  auto *validate = app.add_subcommand("validate", "Validate DAG files");
  validate->footer("\nExamples:\n"
                   "  dagforge validate -c system_config.toml\n"
                   "  dagforge validate -f dags/my_dag.toml");
  validate_opts.config_file = env_config;
  validate
      ->add_option("-c,--config", validate_opts.config_file,
                   "System config file")
      ->check(CLI::ExistingFile);
  validate
      ->add_option("-f,--file", validate_opts.file,
                   "Specific TOML file to validate")
      ->check(CLI::ExistingFile);
  validate->add_flag("--json", validate_opts.json, "Output JSON");
  validate->callback([&validate_opts]() {
    // Require either --config or --file
    if (validate_opts.config_file.empty() && !validate_opts.file.has_value()) {
      std::fputs("Error: Either --config or --file is required\n", stderr);
      std::exit(1);
    }
    std::exit(dagforge::cli::cmd_validate(validate_opts));
  });

  auto *db = app.add_subcommand("db", "Database management");
  db->require_subcommand(1);
  db->footer("\nExamples:\n"
             "  dagforge db init -c system_config.toml\n"
             "  dagforge db migrate -c system_config.toml\n"
             "  dagforge db prune-stale -c system_config.toml --dry-run");

  dagforge::cli::DbOptions db_init_opts;
  auto *db_init = db->add_subcommand("init", "Initialize database schema");
  db_init_opts.config_file = env_config;
  auto *db_init_cfg = db_init
                          ->add_option("-c,--config", db_init_opts.config_file,
                                       "System config file")
                          ->check(CLI::ExistingFile);
  if (env_config.empty())
    db_init_cfg->required();
  db_init->callback([&db_init_opts]() {
    std::exit(dagforge::cli::cmd_db_init(db_init_opts));
  });

  dagforge::cli::DbOptions db_migrate_opts;
  auto *db_migrate = db->add_subcommand("migrate", "Run database migrations");
  db_migrate_opts.config_file = env_config;
  auto *db_migrate_cfg =
      db_migrate
          ->add_option("-c,--config", db_migrate_opts.config_file,
                       "System config file")
          ->check(CLI::ExistingFile);
  if (env_config.empty())
    db_migrate_cfg->required();
  db_migrate->callback([&db_migrate_opts]() {
    std::exit(dagforge::cli::cmd_db_migrate(db_migrate_opts));
  });

  dagforge::cli::DbOptions db_prune_stale_opts;
  auto *db_prune_stale = db->add_subcommand(
      "prune-stale",
      "Delete DB-only stale DAGs not present in DAG source directory");
  db_prune_stale_opts.config_file = env_config;
  auto *db_prune_stale_cfg =
      db_prune_stale
          ->add_option("-c,--config", db_prune_stale_opts.config_file,
                       "System config file")
          ->check(CLI::ExistingFile);
  if (env_config.empty())
    db_prune_stale_cfg->required();
  db_prune_stale->add_flag("--dry-run", db_prune_stale_opts.dry_run,
                           "Show stale DAGs without deleting them");
  db_prune_stale->callback([&db_prune_stale_opts]() {
    std::exit(dagforge::cli::cmd_db_prune_stale(db_prune_stale_opts));
  });

  CLI11_PARSE(app, argc, argv);
  return 0;
}

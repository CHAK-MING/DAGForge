#include "dagforge/config/dag_definition.hpp"
#include "dagforge/config/toml_util.hpp"

#include "dagforge/dag/dag_validator.hpp"
#include "dagforge/scheduler/cron.hpp"
#include "dagforge/util/log.hpp"

#include <glaze/toml.hpp>

#include <charconv>
#include <chrono>
#include <format>
#include <string>
#include <string_view>
#include <unordered_set>
#include <variant>

namespace dagforge {
namespace detail {

struct TaskDependencyToml {
  std::string task;
  std::string label;
};

struct XComPushToml {
  std::string key;
  std::string source{"stdout"};
  std::string json_path;
  std::string regex;
  int regex_group{0};
};

struct XComPullToml {
  std::string key;
  std::string from;
  std::string env;
  bool required{false};
};

struct TaskDefaultsToml {
  int timeout{0};
  int retry_interval{0};
  int max_retries{-1};
  std::string trigger_rule{"all_success"};
  std::string executor{"shell"};
  std::string working_dir;
  bool depends_on_past{false};
};

struct TaskToml {
  std::string id;
  std::string name;
  std::string command;
  std::string working_dir;
  std::string executor;
  int timeout{0};
  int retry_interval{0};
  int max_retries{-1};
  std::string trigger_rule;
  bool is_branch{false};
  std::string branch_xcom_key;
  bool depends_on_past{false};

  std::vector<std::variant<std::string, TaskDependencyToml>> dependencies;
  std::vector<XComPushToml> xcom_push;
  std::vector<XComPullToml> xcom_pull;

  std::string docker_image;
  std::string docker_socket;
  std::string pull_policy;

  std::string sensor_type;
  int sensor_interval{30};
  bool soft_fail{false};
  int sensor_expected_status{200};
  std::string sensor_http_method{"GET"};
  std::string sensor_target;
};

struct DAGToml {
  std::string id;
  std::string name;
  std::string description;
  std::string cron;
  std::string start_date;
  std::string end_date;
  int max_active_runs{16};
  bool catchup{false};
  TaskDefaultsToml default_args{};
  std::vector<TaskToml> tasks;
};

} // namespace detail
} // namespace dagforge

namespace glz {
template <> struct meta<dagforge::detail::TaskDependencyToml> {
  using T = dagforge::detail::TaskDependencyToml;
  static constexpr auto value = object("task", &T::task, "label", &T::label);
};

template <> struct meta<dagforge::detail::XComPushToml> {
  using T = dagforge::detail::XComPushToml;
  static constexpr auto value =
      object("key", &T::key, "source", &T::source, "json_path", &T::json_path,
             "regex", &T::regex, "regex_group", &T::regex_group);
};

template <> struct meta<dagforge::detail::XComPullToml> {
  using T = dagforge::detail::XComPullToml;
  static constexpr auto value = object("key", &T::key, "from", &T::from, "env",
                                       &T::env, "required", &T::required);
};

template <> struct meta<dagforge::detail::TaskDefaultsToml> {
  using T = dagforge::detail::TaskDefaultsToml;
  static constexpr auto value =
      object("timeout", &T::timeout, "retry_interval", &T::retry_interval,
             "max_retries", &T::max_retries, "trigger_rule", &T::trigger_rule,
             "executor", &T::executor, "working_dir", &T::working_dir,
             "depends_on_past", &T::depends_on_past);
};

template <> struct meta<dagforge::detail::TaskToml> {
  using T = dagforge::detail::TaskToml;
  static constexpr auto value = object(
      "id", &T::id, "name", &T::name, "command", &T::command, "working_dir",
      &T::working_dir, "executor", &T::executor, "timeout", &T::timeout,
      "retry_interval", &T::retry_interval, "max_retries", &T::max_retries,
      "trigger_rule", &T::trigger_rule, "is_branch", &T::is_branch,
      "branch_xcom_key", &T::branch_xcom_key, "depends_on_past",
      &T::depends_on_past, "dependencies", &T::dependencies, "xcom_push",
      &T::xcom_push, "xcom_pull", &T::xcom_pull, "docker_image",
      &T::docker_image, "docker_socket", &T::docker_socket, "pull_policy",
      &T::pull_policy, "sensor_type", &T::sensor_type, "sensor_interval",
      &T::sensor_interval, "soft_fail", &T::soft_fail, "sensor_expected_status",
      &T::sensor_expected_status, "sensor_http_method", &T::sensor_http_method,
      "sensor_target", &T::sensor_target);
};

template <> struct meta<dagforge::detail::DAGToml> {
  using T = dagforge::detail::DAGToml;
  static constexpr auto value =
      object("id", &T::id, "name", &T::name, "description", &T::description,
             "cron", &T::cron, "start_date", &T::start_date, "end_date",
             &T::end_date, "max_active_runs", &T::max_active_runs, "catchup",
             &T::catchup, "default_args", &T::default_args, "tasks", &T::tasks);
};
} // namespace glz

namespace dagforge {
namespace {

[[nodiscard]] auto parse_days_timepoint(std::string_view s)
    -> std::optional<std::chrono::system_clock::time_point> {
  if (s.empty()) {
    return std::nullopt;
  }
  using namespace std::chrono;
  int y_val = 0;
  unsigned m_val = 0;
  unsigned d_val = 0;

  const char *p = s.data();
  const char *end = p + s.size();

  auto [p1, ec1] = std::from_chars(p, end, y_val);
  if (ec1 != std::errc{} || p1 >= end || *p1 != '-')
    return std::nullopt;

  auto [p2, ec2] = std::from_chars(p1 + 1, end, m_val);
  if (ec2 != std::errc{} || p2 >= end || *p2 != '-')
    return std::nullopt;

  auto [p3, ec3] = std::from_chars(p2 + 1, end, d_val);
  if (ec3 != std::errc{})
    return std::nullopt;

  const year_month_day ymd{year{y_val}, month{m_val}, day{d_val}};
  if (!ymd.ok()) {
    return std::nullopt;
  }
  return sys_days{ymd};
}

[[nodiscard]] auto parse_dependencies(
    const std::vector<std::variant<std::string, detail::TaskDependencyToml>>
        &deps) -> std::vector<TaskDependency> {
  std::vector<TaskDependency> out;
  out.reserve(deps.size());

  for (const auto &dep : deps) {
    if (const auto *id = std::get_if<std::string>(&dep)) {
      if (!id->empty()) {
        out.emplace_back(TaskDependency{.task_id = TaskId(*id), .label = ""});
      }
      continue;
    }
    const auto &d = std::get<detail::TaskDependencyToml>(dep);
    if (!d.task.empty()) {
      out.emplace_back(
          TaskDependency{.task_id = TaskId(d.task), .label = d.label});
    }
  }

  return out;
}

[[nodiscard]] auto
parse_xcom_push(const std::vector<detail::XComPushToml> &push)
    -> std::vector<XComPushConfig> {
  std::vector<XComPushConfig> out;
  out.reserve(push.size());
  for (const auto &item : push) {
    out.emplace_back(XComPushConfig{.key = item.key,
                                    .source = parse<XComSource>(item.source),
                                    .json_path = item.json_path,
                                    .regex_pattern = item.regex,
                                    .regex_group = item.regex_group});
  }
  return out;
}

[[nodiscard]] auto
parse_xcom_pull(const std::vector<detail::XComPullToml> &pull)
    -> std::vector<XComPullConfig> {
  std::vector<XComPullConfig> out;
  out.reserve(pull.size());
  for (const auto &item : pull) {
    out.emplace_back(XComPullConfig{
        .ref = XComRef{.task_id = TaskId(item.from), .key = item.key},
        .env_var = item.env,
        .required = item.required,
        .default_value = std::nullopt});
  }
  return out;
}

[[nodiscard]] auto parse_defaults(const detail::TaskDefaultsToml &raw)
    -> TaskDefaults {
  TaskDefaults defaults{};
  const int timeout_sec =
      raw.timeout > 0
          ? raw.timeout
          : static_cast<int>(task_defaults::kExecutionTimeout.count());
  const int retry_interval_sec =
      raw.retry_interval > 0
          ? raw.retry_interval
          : static_cast<int>(task_defaults::kRetryInterval.count());
  const int max_retries =
      raw.max_retries >= 0 ? raw.max_retries : task_defaults::kMaxRetries;
  defaults.execution_timeout = std::chrono::seconds(timeout_sec);
  defaults.retry_interval = std::chrono::seconds(retry_interval_sec);
  defaults.max_retries = max_retries;
  defaults.depends_on_past = raw.depends_on_past;
  defaults.trigger_rule = parse<TriggerRule>(raw.trigger_rule);
  defaults.executor = parse<ExecutorType>(raw.executor);
  defaults.working_dir = raw.working_dir;
  return defaults;
}

[[nodiscard]] auto parse_executor_config(const detail::TaskToml &raw,
                                         ExecutorType executor,
                                         std::string_view command_value)
    -> ExecutorTaskConfig {
  switch (executor) {
  case ExecutorType::Docker: {
    DockerTaskConfig cfg{};
    cfg.image = raw.docker_image;
    if (!raw.docker_socket.empty()) {
      cfg.socket = raw.docker_socket;
    }
    if (!raw.pull_policy.empty()) {
      cfg.pull_policy = parse<ImagePullPolicy>(raw.pull_policy);
    }
    return cfg;
  }
  case ExecutorType::Sensor: {
    SensorTaskConfig cfg{};
    if (!raw.sensor_type.empty()) {
      cfg.type = parse<SensorType>(raw.sensor_type);
    }
    cfg.poke_interval = std::chrono::seconds(raw.sensor_interval);
    cfg.soft_fail = raw.soft_fail;
    cfg.expected_status = raw.sensor_expected_status;
    if (!raw.sensor_http_method.empty()) {
      cfg.http_method = raw.sensor_http_method;
    }

    cfg.target = raw.sensor_target;
    if (cfg.target.empty()) {
      cfg.target = std::string(command_value);
    }
    return cfg;
  }
  case ExecutorType::Shell:
  default:
    return ShellTaskConfig{};
  }
}

[[nodiscard]] auto parse_task(const detail::TaskToml &raw,
                              const TaskDefaults &defaults)
    -> Result<TaskConfig> {
  TaskConfig task{};
  task.task_id = TaskId(raw.id);
  task.name = raw.name.empty() ? raw.id : raw.name;
  task.command = raw.command;

  task.execution_timeout = defaults.execution_timeout;
  task.retry_interval = defaults.retry_interval;
  task.max_retries = defaults.max_retries;
  task.trigger_rule = defaults.trigger_rule;
  task.executor = defaults.executor;
  task.depends_on_past = defaults.depends_on_past;
  task.working_dir = defaults.working_dir;

  if (!raw.executor.empty()) {
    task.executor = parse<ExecutorType>(raw.executor);
  }
  if (!raw.working_dir.empty()) {
    task.working_dir = raw.working_dir;
  }
  if (raw.timeout > 0) {
    const int timeout_sec = raw.timeout;
    task.execution_timeout = std::chrono::seconds(timeout_sec);
  }
  if (raw.retry_interval > 0) {
    const int retry_interval_sec = raw.retry_interval;
    task.retry_interval = std::chrono::seconds(retry_interval_sec);
  }
  if (raw.max_retries >= 0) {
    task.max_retries = raw.max_retries;
  }
  if (!raw.trigger_rule.empty()) {
    task.trigger_rule = parse<TriggerRule>(raw.trigger_rule);
  }

  task.is_branch = raw.is_branch;
  if (!raw.branch_xcom_key.empty()) {
    task.branch_xcom_key = raw.branch_xcom_key;
  }
  if (raw.depends_on_past) {
    task.depends_on_past = true;
  }

  task.dependencies = parse_dependencies(raw.dependencies);
  task.xcom_push = parse_xcom_push(raw.xcom_push);
  task.xcom_pull = parse_xcom_pull(raw.xcom_pull);
  task.executor_config =
      parse_executor_config(raw, task.executor, task.command);

  return ok(std::move(task));
}

[[nodiscard]] auto validate_definition(const DAGDefinition &def)
    -> std::vector<std::string> {
  std::vector<std::string> errors;

  if (def.name.empty()) {
    errors.emplace_back("DAG name cannot be empty");
  }

  if (!def.cron.empty()) {
    if (auto res = CronExpr::parse(def.cron); !res) {
      errors.emplace_back(std::format("Invalid cron expression '{}': {}",
                                      def.cron, res.error().message()));
    }
  }

  if (def.tasks.empty()) {
    errors.emplace_back("DAG must have at least one task");
    return errors;
  }

  std::unordered_set<TaskId> task_ids;
  for (const auto &task : def.tasks) {
    if (task.task_id.value().empty()) {
      errors.emplace_back("Task ID cannot be empty");
      continue;
    }
    if (!is_valid_id_text(task.task_id.value())) {
      errors.emplace_back(std::format(
          "Task ID '{}' contains control characters", task.task_id));
      continue;
    }

    if (!task_ids.insert(task.task_id).second) {
      errors.emplace_back(std::format("Duplicate task ID: '{}'", task.task_id));
    }

    if (task.command.empty()) {
      errors.emplace_back(
          std::format("Task '{}': command cannot be empty", task.task_id));
    }

    if (task.execution_timeout.count() < 0) {
      errors.emplace_back(
          std::format("Task '{}': negative timeout not allowed", task.task_id));
    }
    if (task.retry_interval.count() < 0) {
      errors.emplace_back(std::format(
          "Task '{}': negative retry interval not allowed", task.task_id));
    }
    if (task.max_retries < 0) {
      errors.emplace_back(std::format(
          "Task '{}': negative max retries not allowed", task.task_id));
    }

    if (task.executor == ExecutorType::Docker) {
      if (const auto *docker =
              std::get_if<DockerTaskConfig>(&task.executor_config)) {
        if (docker->image.empty()) {
          errors.emplace_back(std::format(
              "Task '{}': docker image cannot be empty", task.task_id));
        }
      }
    }

    if (task.executor == ExecutorType::Sensor) {
      if (const auto *sensor =
              std::get_if<SensorTaskConfig>(&task.executor_config)) {
        if (sensor->target.empty()) {
          errors.emplace_back(std::format(
              "Task '{}': sensor target cannot be empty", task.task_id));
        }
      }
    }
  }

  for (const auto &task : def.tasks) {
    if (task.task_id.value().empty()) {
      continue;
    }
    for (const auto &dep : task.dependencies) {
      if (!is_valid_id_text(dep.task_id.value())) {
        errors.emplace_back(std::format(
            "Task '{}': dependency ID '{}' contains control characters",
            task.task_id, dep.task_id));
        continue;
      }
      if (dep.task_id == task.task_id) {
        errors.emplace_back(std::format(
            "Task '{}': self-dependency not allowed", task.task_id));
      } else if (!task_ids.contains(dep.task_id)) {
        errors.emplace_back(std::format("Task '{}': dependency '{}' not found",
                                        task.task_id, dep.task_id));
      }
    }
  }

  if (errors.empty()) {
    DAGInfo info;
    info.dag_id = def.dag_id;
    info.tasks = def.tasks;
    if (auto res = DAGValidator::validate(info); !res) {
      errors.emplace_back("Circular dependency detected");
    }
  }

  return errors;
}

[[nodiscard]] auto parse_definition_from_text(std::string_view text,
                                              std::string *diagnostic)
    -> Result<DAGInfo> {
  auto raw_result = toml_util::parse_toml<detail::DAGToml>(text, diagnostic);
  if (!raw_result)
    return fail(raw_result.error());
  auto &raw = *raw_result;

  DAGInfo dag{};
  if (raw.id.empty()) {
    constexpr auto kErr =
        "DAG parse error: missing required top-level field 'id'\n"
        "Hint: add `id = \"your_dag_id\"` before any [[tasks]] section.";
    log::error("{}", kErr);
    if (diagnostic) {
      *diagnostic = kErr;
    }
    return fail(Error::InvalidArgument);
  }
  dag.dag_id = DAGId(raw.id);
  dag.name = raw.name.empty() ? raw.id : raw.name;
  dag.description = raw.description;
  dag.cron = raw.cron;
  dag.catchup = raw.catchup;
  dag.max_concurrent_runs = raw.max_active_runs;
  dag.start_date = parse_days_timepoint(raw.start_date);
  dag.end_date = parse_days_timepoint(raw.end_date);

  auto defaults = parse_defaults(raw.default_args);

  dag.tasks.reserve(raw.tasks.size());
  for (std::size_t i = 0; i < raw.tasks.size(); ++i) {
    const auto &task_raw = raw.tasks[i];
    if (task_raw.id.empty()) {
      if (diagnostic) {
        *diagnostic = std::format(
            "DAG parse error: task #{} is missing required field 'id'\n"
            "Hint: add `id = \"task_name\"` under [[tasks]].",
            i + 1);
      }
      return fail(Error::InvalidArgument);
    }

    auto parsed = parse_task(task_raw, defaults);
    if (!parsed) {
      if (diagnostic) {
        *diagnostic = parsed.error().message();
      }
      return fail(parsed.error());
    }
    dag.tasks.emplace_back(std::move(*parsed));
  }

  auto now = std::chrono::system_clock::now();
  dag.created_at = now;
  dag.updated_at = now;
  dag.rebuild_task_index();

  return ok(std::move(dag));
}

} // namespace

auto DAGDefinitionLoader::load_from_file(std::string_view path,
                                         std::string *diagnostic)
    -> Result<DAGInfo> {
  auto text = toml_util::read_file(path);
  if (!text) {
    if (diagnostic) {
      *diagnostic = text.error().message();
    }
    return fail(text.error());
  }

  return load_from_string(*text, diagnostic);
}

auto DAGDefinitionLoader::load_from_string(std::string_view toml_str,
                                           std::string *diagnostic)
    -> Result<DAGInfo> {
  try {
    auto result = parse_definition_from_text(toml_str, diagnostic);
    if (!result) {
      return fail(result.error());
    }

    auto errors = validate_definition(*result);
    if (!errors.empty()) {
      if (diagnostic) {
        std::string joined;
        for (std::size_t i = 0; i < errors.size(); ++i) {
          if (i > 0) {
            joined += "; ";
          }
          joined += errors[i];
        }
        *diagnostic = std::move(joined);
      }
      for (const auto &err : errors) {
        log::error("DAG validation error: {}", err);
      }
      return fail(Error::InvalidArgument);
    }

    return result;
  } catch (const std::exception &e) {
    log::error("TOML parse error: {}", e.what());
    if (diagnostic) {
      *diagnostic = e.what();
    }
    return fail(Error::ParseError);
  } catch (...) {
    if (diagnostic) {
      *diagnostic = "unknown parse error";
    }
    return fail(Error::ParseError);
  }
}

auto DAGDefinitionLoader::to_string(const DAGInfo &dag) -> std::string {
  detail::DAGToml raw{};
  raw.id = dag.dag_id.str();
  raw.name = dag.name;
  raw.description = dag.description;
  raw.cron = dag.cron;
  raw.catchup = dag.catchup;
  raw.max_active_runs = dag.max_concurrent_runs;

  // default_args are not stored in DAGInfo — use task-level values
  raw.default_args = detail::TaskDefaultsToml{};

  raw.tasks.reserve(dag.tasks.size());
  for (const auto &t : dag.tasks) {
    detail::TaskToml task_raw{};
    task_raw.id = t.task_id.str();
    task_raw.name = t.name;
    task_raw.command = t.command;
    task_raw.executor = enum_to_string(t.executor);
    task_raw.timeout = static_cast<int>(t.execution_timeout.count());
    task_raw.retry_interval = static_cast<int>(t.retry_interval.count());
    task_raw.max_retries = t.max_retries;
    task_raw.trigger_rule = enum_to_string(t.trigger_rule);
    task_raw.is_branch = t.is_branch;
    task_raw.depends_on_past = t.depends_on_past;
    task_raw.working_dir = t.working_dir;

    for (const auto &dep : t.dependencies) {
      task_raw.dependencies.emplace_back(dep.task_id.str());
    }

    raw.tasks.emplace_back(std::move(task_raw));
  }

  auto result = glz::write_toml(raw);
  if (result) {
    return std::move(*result);
  }
  return std::format("id = \"{}\"\nname = \"{}\"\ncatchup = {}\n", dag.dag_id,
                     dag.name, dag.catchup ? "true" : "false");
}

} // namespace dagforge

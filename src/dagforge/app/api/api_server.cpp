#include "dagforge/app/api/api_server.hpp"
#include "dagforge/dag/dag_run.hpp"
#include <cstdint>

#include "dagforge/app/application.hpp"
#include "dagforge/app/http/http_server.hpp"
#include "dagforge/app/http/router.hpp"
#include "dagforge/app/http/websocket.hpp"
#include "dagforge/app/services/persistence_service.hpp"
#include "dagforge/core/coroutine.hpp"
#include "dagforge/core/error.hpp"
#include "dagforge/dag/dag_manager.hpp"
#include "dagforge/io/result.hpp"
#include "dagforge/storage/orm_models.hpp"
#include "dagforge/util/json.hpp"
#include "dagforge/util/log.hpp"
#include "dagforge/util/util.hpp"

#include <chrono>
#include <optional>
#include <sstream>
#include <string>
#include <thread>
#include <type_traits>
#include <unistd.h>
#include <unordered_map>
#include <utility>
#include <vector>

namespace dagforge {

using json = JsonValue;
using namespace http;

namespace api_dto {

struct DagInfoDto {
  std::string dag_id;
  std::string name;
  std::string description;
  std::string cron;
  int max_concurrent_runs{1};
  bool is_paused{false};
  std::vector<std::string> tasks;
};

struct DagsResponseDto {
  std::vector<DagInfoDto> dags;
};

struct RunHistoryEntryDto {
  std::string dag_run_id;
  std::string dag_id;
  std::string state;
  std::string trigger_type;
  std::string started_at;
  std::string finished_at;
  std::string execution_date;
};

struct HistoryResponseDto {
  std::vector<RunHistoryEntryDto> runs;
};

struct TaskIdsResponseDto {
  std::vector<std::string> tasks;
};

struct TaskDependencyDto {
  std::string task;
  std::string label;
};

struct TaskDetailDto {
  std::string task_id;
  std::string name;
  std::string command;
  std::vector<TaskDependencyDto> dependencies;
};

struct DagRunSummaryDto {
  std::string dag_run_id;
  std::string state;
  std::string started_at;
  std::string finished_at;
};

struct DagRunHistoryResponseDto {
  std::vector<DagRunSummaryDto> runs;
};

struct TaskInstanceDto {
  std::string task_id;
  std::string state;
  int attempt{0};
  int exit_code{0};
  std::string started_at;
  std::string finished_at;
  std::string error;
};

struct RunTasksResponseDto {
  std::string dag_run_id;
  std::vector<TaskInstanceDto> tasks;
};

struct TaskLogEntryDto {
  std::string task_id;
  int attempt{1};
  std::string stream;
  std::string logged_at;
  std::string content;
};

struct RunLogsResponseDto {
  std::string dag_run_id;
  std::vector<TaskLogEntryDto> logs;
};

struct TaskLogsResponseDto {
  std::string dag_run_id;
  std::string task_id;
  std::vector<TaskLogEntryDto> logs;
};

} // namespace api_dto

} // namespace dagforge

namespace glz {

template <> struct meta<dagforge::api_dto::DagInfoDto> {
  using T = dagforge::api_dto::DagInfoDto;
  static constexpr auto value = object(
      "dag_id", &T::dag_id, "name", &T::name, "description", &T::description,
      "cron", &T::cron, "max_concurrent_runs", &T::max_concurrent_runs,
      "is_paused", &T::is_paused, "tasks", &T::tasks);
};

template <> struct meta<dagforge::api_dto::DagsResponseDto> {
  using T = dagforge::api_dto::DagsResponseDto;
  static constexpr auto value = object("dags", &T::dags);
};

template <> struct meta<dagforge::api_dto::RunHistoryEntryDto> {
  using T = dagforge::api_dto::RunHistoryEntryDto;
  static constexpr auto value = object(
      "dag_run_id", &T::dag_run_id, "dag_id", &T::dag_id, "state", &T::state,
      "trigger_type", &T::trigger_type, "started_at", &T::started_at,
      "finished_at", &T::finished_at, "execution_date", &T::execution_date);
};

template <> struct meta<dagforge::api_dto::HistoryResponseDto> {
  using T = dagforge::api_dto::HistoryResponseDto;
  static constexpr auto value = object("runs", &T::runs);
};

template <> struct meta<dagforge::api_dto::TaskIdsResponseDto> {
  using T = dagforge::api_dto::TaskIdsResponseDto;
  static constexpr auto value = object("tasks", &T::tasks);
};

template <> struct meta<dagforge::api_dto::TaskDependencyDto> {
  using T = dagforge::api_dto::TaskDependencyDto;
  static constexpr auto value = object("task", &T::task, "label", &T::label);
};

template <> struct meta<dagforge::api_dto::TaskDetailDto> {
  using T = dagforge::api_dto::TaskDetailDto;
  static constexpr auto value =
      object("task_id", &T::task_id, "name", &T::name, "command", &T::command,
             "dependencies", &T::dependencies);
};

template <> struct meta<dagforge::api_dto::DagRunSummaryDto> {
  using T = dagforge::api_dto::DagRunSummaryDto;
  static constexpr auto value =
      object("dag_run_id", &T::dag_run_id, "state", &T::state, "started_at",
             &T::started_at, "finished_at", &T::finished_at);
};

template <> struct meta<dagforge::api_dto::DagRunHistoryResponseDto> {
  using T = dagforge::api_dto::DagRunHistoryResponseDto;
  static constexpr auto value = object("runs", &T::runs);
};

template <> struct meta<dagforge::api_dto::TaskLogEntryDto> {
  using T = dagforge::api_dto::TaskLogEntryDto;
  static constexpr auto value =
      object("task_id", &T::task_id, "attempt", &T::attempt, "stream",
             &T::stream, "logged_at", &T::logged_at, "content", &T::content);
};

template <> struct meta<dagforge::api_dto::RunLogsResponseDto> {
  using T = dagforge::api_dto::RunLogsResponseDto;
  static constexpr auto value =
      object("dag_run_id", &T::dag_run_id, "logs", &T::logs);
};

template <> struct meta<dagforge::api_dto::TaskLogsResponseDto> {
  using T = dagforge::api_dto::TaskLogsResponseDto;
  static constexpr auto value = object("dag_run_id", &T::dag_run_id, "task_id",
                                       &T::task_id, "logs", &T::logs);
};

template <> struct meta<dagforge::api_dto::TaskInstanceDto> {
  using T = dagforge::api_dto::TaskInstanceDto;
  static constexpr auto value =
      object("task_id", &T::task_id, "state", &T::state, "attempt", &T::attempt,
             "exit_code", &T::exit_code, "started_at", &T::started_at,
             "finished_at", &T::finished_at, "error", &T::error);
};

template <> struct meta<dagforge::api_dto::RunTasksResponseDto> {
  using T = dagforge::api_dto::RunTasksResponseDto;
  static constexpr auto value =
      object("dag_run_id", &T::dag_run_id, "tasks", &T::tasks);
};

} // namespace glz

namespace dagforge {

namespace {

auto status_from_error(const std::error_code &ec) -> HttpStatus {
  if (ec.category() == io::io_error_category()) {
    switch (static_cast<io::IoError>(ec.value())) {
    case io::IoError::TimedOut:
    case io::IoError::Cancelled:
      return HttpStatus::ServiceUnavailable;
    case io::IoError::InvalidArgument:
      return HttpStatus::BadRequest;
    case io::IoError::EndOfFile:
      return HttpStatus::NotFound;
    case io::IoError::ConnectionReset:
    case io::IoError::BrokenPipe:
      return HttpStatus::ServiceUnavailable;
    default:
      break;
    }
  }

  if (ec.category() == error_category()) {
    switch (static_cast<Error>(ec.value())) {
    case Error::NotFound:
    case Error::FileNotFound:
      return HttpStatus::NotFound;
    case Error::InvalidArgument:
    case Error::ParseError:
    case Error::InvalidUrl:
      return HttpStatus::BadRequest;
    case Error::Timeout:
    case Error::Cancelled:
      return HttpStatus::ServiceUnavailable;
    case Error::AlreadyExists:
      return HttpStatus::Conflict;
    case Error::ReadOnly:
      return HttpStatus::Forbidden;
    case Error::ResourceExhausted:
    case Error::QueueFull:
      return HttpStatus::ServiceUnavailable;
    case Error::SystemNotRunning:
    case Error::DatabaseError:
      return HttpStatus::InternalServerError;
    default:
      break;
    }
  }

  if (ec == std::make_error_code(std::errc::resource_unavailable_try_again)) {
    return HttpStatus::ServiceUnavailable;
  }
  return HttpStatus::InternalServerError;
}

auto text_response(std::string body, HttpStatus status,
                   std::string_view content_type) -> HttpResponse {
  HttpResponse resp;
  resp.status = status;
  resp.set_header("Content-Type", std::string(content_type));
  resp.body.assign(body.begin(), body.end());
  return resp;
}

auto to_dto(const DAGInfo &dag_info) -> api_dto::DagInfoDto {
  api_dto::DagInfoDto dto{
      .dag_id = dag_info.dag_id.str(),
      .name = dag_info.name,
      .description = dag_info.description,
      .cron = dag_info.cron,
      .max_concurrent_runs = dag_info.max_concurrent_runs,
      .is_paused = dag_info.is_paused,
      .tasks = {},
  };
  dto.tasks.reserve(dag_info.tasks.size());
  for (const auto &t : dag_info.tasks) {
    dto.tasks.emplace_back(t.task_id.str());
  }
  return dto;
}

auto to_dto(const DatabaseService::RunHistoryEntry &run,
            const Result<DAGRunState> &override_state)
    -> api_dto::RunHistoryEntryDto {
  auto state = override_state.value_or(run.state);
  return api_dto::RunHistoryEntryDto{
      .dag_run_id = run.dag_run_id.str(),
      .dag_id = run.dag_id.str(),
      .state = enum_to_string(state),
      .trigger_type = enum_to_string(run.trigger_type),
      .started_at = util::format_iso8601(run.started_at),
      .finished_at = util::format_iso8601(run.finished_at),
      .execution_date = util::format_iso8601(run.execution_date),
  };
}

auto to_dto(const TaskConfig &task) -> api_dto::TaskDetailDto {
  api_dto::TaskDetailDto dto{
      .task_id = task.task_id.str(),
      .name = task.name,
      .command = task.command,
      .dependencies = {},
  };
  dto.dependencies.reserve(task.dependencies.size());
  for (const auto &dep : task.dependencies) {
    dto.dependencies.emplace_back(api_dto::TaskDependencyDto{
        .task = dep.task_id.str(), .label = dep.label});
  }
  return dto;
}

auto xcom_entries_to_json_object(const std::vector<XComEntry> &xcoms) -> json {
  json xcom = json::object_t{};
  for (const auto &entry : xcoms) {
    xcom[entry.key] = entry.value;
  }
  return xcom;
}

auto error_response(int code, std::string_view message) -> HttpResponse {
  HttpResponse resp{
      .status = static_cast<HttpStatus>(code), .headers = {}, .body = {}};
  resp.set_header("Content-Type", "application/json");
  resp.set_body(dump_json(json{{"error", std::string(message)}}));
  return resp;
}

auto json_response(const json &j, HttpStatus status = HttpStatus::Ok)
    -> HttpResponse {
  HttpResponse resp;
  resp.status = status;
  resp.set_header("Content-Type", "application/json");
  auto s = dump_json(j);
  resp.body.assign(s.begin(), s.end());
  return resp;
}

template <typename T>
auto json_response_glz(const T &value, HttpStatus status = HttpStatus::Ok)
    -> HttpResponse {
  HttpResponse resp;
  resp.status = status;
  resp.set_header("Content-Type", "application/json");

  std::string buffer;
  if (auto ec = glz::write_json(value, buffer); !ec) {
    resp.body.assign(buffer.begin(), buffer.end());
  } else {
    log::error("JSON serialization failed for API response");
    resp.status = HttpStatus::InternalServerError;
    resp.body.clear();
    constexpr std::string_view fallback =
        R"({"error":"JSON serialization failed"})";
    resp.body.assign(fallback.begin(), fallback.end());
  }
  return resp;
}

auto to_result_response(const std::error_code &ec) -> Result<HttpResponse> {
  return error_response(static_cast<int>(status_from_error(ec)), ec.message());
}

auto parse_execution_date_arg(std::string_view value)
    -> std::optional<std::chrono::system_clock::time_point> {
  if (value.empty() || value == "now") {
    return std::chrono::system_clock::now();
  }

  std::chrono::system_clock::time_point tp;
  std::istringstream ss{std::string(value)};

  if (value.size() == 10) {
    std::chrono::year_month_day ymd;
    ss >> std::chrono::parse("%Y-%m-%d", ymd);
    if (ss.fail()) {
      return std::nullopt;
    }
    tp = std::chrono::sys_days{ymd};
  } else {
    ss >> std::chrono::parse("%Y-%m-%dT%H:%M:%SZ", tp);
    if (ss.fail()) {
      return std::nullopt;
    }
  }
  return tp;
}

} // namespace

struct ApiServer::Impl : std::enable_shared_from_this<Impl> {
  Application &app_;
  std::shared_ptr<http::HttpServer> server_;
  std::unique_ptr<http::WebSocketHub> ws_hub_;

  explicit Impl(Application &app) : app_(app) {
    server_ = std::make_shared<HttpServer>(app_.runtime());
    ws_hub_ = std::make_unique<WebSocketHub>(app_.runtime());

    if (!server_) {
      log::error("ApiServer: HttpServer allocation failed (server_ is null)");
    } else {
      log::info("ApiServer constructed. Impl: {}, server_: {}",
                static_cast<void *>(this), static_cast<void *>(server_.get()));
    }
  }

  void init() {
    setup_routes();
    setup_websocket();
  }

  auto get_run_history_async(const DAGRunId &dag_run_id) const
      -> task<Result<DatabaseService::RunHistoryEntry>> {
    auto *persistence = app_.persistence_service();
    if (!persistence) {
      co_return fail(Error::DatabaseError);
    }
    co_return co_await persistence->get_run_history(dag_run_id);
  }

  auto list_dag_run_history_async(const DAGId &dag_id, std::size_t limit) const
      -> task<Result<std::vector<DatabaseService::RunHistoryEntry>>> {
    auto *persistence = app_.persistence_service();
    if (!persistence) {
      co_return fail(Error::DatabaseError);
    }
    co_return co_await persistence->list_dag_run_history(dag_id, limit);
  }

  auto get_task_instances_async(const DAGRunId &dag_run_id) const
      -> task<Result<std::vector<TaskInstanceInfo>>> {
    auto *persistence = app_.persistence_service();
    if (!persistence) {
      co_return fail(Error::DatabaseError);
    }
    co_return co_await persistence->get_task_instances(dag_run_id);
  }

  auto get_dag_snapshot_async(const DAGId &dag_id) const
      -> task<Result<DAGInfo>> {
    auto *persistence = app_.persistence_service();
    if (!persistence) {
      co_return fail(Error::DatabaseError);
    }
    co_return co_await persistence->get_dag(dag_id);
  }

  auto get_task_xcoms_async(const DAGRunId &dag_run_id,
                            const TaskId &task_id) const
      -> task<Result<std::vector<XComEntry>>> {
    auto *persistence = app_.persistence_service();
    if (!persistence) {
      co_return fail(Error::DatabaseError);
    }
    co_return co_await persistence->get_task_xcoms(dag_run_id, task_id);
  }

  auto list_run_history_async(std::size_t limit) const
      -> task<Result<std::vector<DatabaseService::RunHistoryEntry>>> {
    auto *persistence = app_.persistence_service();
    if (!persistence) {
      co_return fail(Error::DatabaseError);
    }
    co_return co_await persistence->list_run_history(limit);
  }

  auto get_run_logs_async(const DAGRunId &run_id, std::size_t limit) const
      -> task<Result<std::vector<orm::TaskLogEntry>>> {
    auto *persistence = app_.persistence_service();
    if (!persistence) {
      co_return fail(Error::DatabaseError);
    }
    co_return co_await persistence->get_run_logs(run_id, limit);
  }

  auto get_task_logs_async(const DAGRunId &run_id, const TaskId &task_id,
                           int attempt, std::size_t limit) const
      -> task<Result<std::vector<orm::TaskLogEntry>>> {
    auto *persistence = app_.persistence_service();
    if (!persistence) {
      co_return fail(Error::DatabaseError);
    }
    co_return co_await persistence->get_task_logs(run_id, task_id, attempt,
                                                  limit);
  }

  void setup_websocket() {
    std::weak_ptr<Impl> weak_self = shared_from_this();
    server_->set_websocket_handler(
        [weak_self](
            std::shared_ptr<http::IWebSocketConnection> conn) -> spawn_task {
          auto self = weak_self.lock();
          if (!self) {
            co_return;
          }
          const int fd_num = conn->fd();
          self->ws_hub_->add_connection(conn);

          dagforge::log::debug("WebSocket client connected: fd={}", fd_num);

          co_await conn->handle_frames(
              [weak_self,
               fd_num](http::WebSocketOpCode opcode,
                       [[maybe_unused]] std::span<const std::byte> data) {
                auto self_locked = weak_self.lock();
                if (!self_locked)
                  return;

                if (opcode == http::WebSocketOpCode::Close) {
                  self_locked->ws_hub_->remove_connection(fd_num);
                  dagforge::log::info("WebSocket client disconnected: fd={}",
                                      fd_num);
                }
              });

          self->ws_hub_->remove_connection(fd_num);
        });
  }

  void setup_routes() {
    std::weak_ptr<Impl> weak_self = shared_from_this();
    auto &router = server_->router();

    router.get("/api/health", [](HttpRequest) -> task<HttpResponse> {
      co_return json_response({{"status", "healthy"}});
    });

    router.get("/api/status", [weak_self](HttpRequest) -> task<HttpResponse> {
      auto self = weak_self.lock();
      if (!self)
        co_return error_response(503, "Service Unavailable");
      co_return json_response({
          {"dag_count", self->app_.dag_manager().dag_count()},
          {"active_runs", self->app_.has_active_runs()},
          {"timestamp", util::format_timestamp()},
      });
    });

    router.get("/metrics", [weak_self](HttpRequest) -> task<HttpResponse> {
      auto self = weak_self.lock();
      if (!self)
        co_return error_response(503, "Service Unavailable");

      std::ostringstream out;
      const auto shard_count = self->app_.runtime().shard_count();

      out << "# HELP dagforge_active_coroutines_total Current number of "
             "alive coroutines\n";
      out << "# TYPE dagforge_active_coroutines_total gauge\n";
      for (unsigned sid = 0; sid < shard_count; ++sid) {
        const auto value = (sid == 0) ? self->app_.active_coroutines() : 0;
        out << "dagforge_active_coroutines_total{shard=\"" << sid << "\"} "
            << value << "\n";
      }

      out << "# HELP dagforge_mysql_batch_write_ops Batch writes committed\n";
      out << "# TYPE dagforge_mysql_batch_write_ops counter\n";
      out << "dagforge_mysql_batch_write_ops "
          << self->app_.mysql_batch_write_ops() << "\n";

      out << "# HELP dagforge_event_bus_queue_length Tasks queued for "
             "cross-shard execution\n";
      out << "# TYPE dagforge_event_bus_queue_length gauge\n";
      out << "dagforge_event_bus_queue_length "
          << self->app_.event_bus_queue_length() << "\n";

      out << "# HELP dagforge_dropped_persistence_events_total Persistence "
             "events dropped due to failures\n";
      out << "# TYPE dagforge_dropped_persistence_events_total counter\n";
      out << "dagforge_dropped_persistence_events_total "
          << self->app_.dropped_persistence_events() << "\n";

      out << "# HELP dagforge_shard_stall_age_ms Milliseconds since shard "
             "heartbeat was last observed\n";
      out << "# TYPE dagforge_shard_stall_age_ms gauge\n";
      for (unsigned sid = 0; sid < shard_count; ++sid) {
        out << "dagforge_shard_stall_age_ms{shard=\"" << sid << "\"} "
            << self->app_.shard_stall_age_ms(sid) << "\n";
      }

      co_return text_response(out.str(), HttpStatus::Ok,
                              "text/plain; version=0.0.4; charset=utf-8");
    });

    router.get("/api/dags", [weak_self](HttpRequest) -> task<HttpResponse> {
      auto self = weak_self.lock();
      if (!self)
        co_return error_response(503, "Service Unavailable");
      api_dto::DagsResponseDto resp_dto;
      for (const auto &dag_info : self->app_.dag_manager().list_dags()) {
        resp_dto.dags.emplace_back(to_dto(dag_info));
      }
      co_return json_response_glz(resp_dto);
    });

    router.get("/api/dags/{dag_id}",
               [weak_self](HttpRequest req) -> task<HttpResponse> {
                 auto self = weak_self.lock();
                 if (!self)
                   co_return error_response(503, "Service Unavailable");

                 auto dag_id = req.path_param("dag_id");
                 if (!dag_id)
                   co_return error_response(400, "Missing dag_id");

                 co_return self->app_.dag_manager()
                     .get_dag(DAGId{*dag_id})
                     .transform([](const auto &dag) {
                       return json_response_glz(to_dto(dag));
                     })
                     .or_else(to_result_response)
                     .value();
               });

    router.get("/api/dags/{dag_id}/tasks",
               [weak_self](HttpRequest req) -> task<HttpResponse> {
                 auto self = weak_self.lock();
                 if (!self)
                   co_return error_response(503, "Service Unavailable");

                 auto dag_id = req.path_param("dag_id");
                 if (!dag_id)
                   co_return error_response(400, "Missing dag_id");

                 co_return self->app_.dag_manager()
                     .get_dag(DAGId{*dag_id})
                     .transform([](const auto &dag) {
                       api_dto::TaskIdsResponseDto dto;
                       dto.tasks.reserve(dag.tasks.size());
                       for (const auto &task : dag.tasks) {
                         dto.tasks.emplace_back(task.task_id.str());
                       }
                       return json_response_glz(dto);
                     })
                     .or_else(to_result_response)
                     .value();
               });

    router.get("/api/dags/{dag_id}/tasks/{task_id}",
               [weak_self](HttpRequest req) -> task<HttpResponse> {
                 auto self = weak_self.lock();
                 if (!self)
                   co_return error_response(503, "Service Unavailable");

                 auto dag_id = req.path_param("dag_id");
                 auto task_id = req.path_param("task_id");
                 if (!dag_id || !task_id) {
                   co_return error_response(400, "Missing dag_id or task_id");
                 }

                 co_return self->app_.dag_manager()
                     .get_dag(DAGId{*dag_id})
                     .and_then([&](const auto &dag) -> Result<HttpResponse> {
                       auto *task = dag.find_task(TaskId{*task_id});
                       if (!task)
                         return fail(Error::NotFound);

                       return ok(json_response_glz(to_dto(*task)));
                     })
                     .or_else(to_result_response)
                     .value();
               });

    router.post(
        "/api/dags/{dag_id}/trigger",
        [weak_self](HttpRequest req) -> task<HttpResponse> {
          auto self = weak_self.lock();
          if (!self)
            co_return error_response(503, "Service Unavailable");

          auto dag_id = req.path_param("dag_id");
          if (!dag_id)
            co_return error_response(400, "Missing dag_id");

          std::optional<std::chrono::system_clock::time_point> execution_date;
          auto body = req.body_as_string();
          if (!body.empty()) {
            auto parsed = parse_json(body);
            if (!parsed) {
              co_return error_response(400, "Invalid JSON body");
            }
            if (!parsed->is_object()) {
              co_return error_response(400,
                                       "Request body must be a JSON object");
            }

            const auto &obj = parsed->get_object();
            if (auto it = obj.find("execution_date"); it != obj.end()) {
              if (!it->second.is_string()) {
                co_return error_response(
                    400, "execution_date must be a string (now | YYYY-MM-DD | "
                         "YYYY-MM-DDTHH:MM:SSZ)");
              }
              auto parsed_execution_date =
                  parse_execution_date_arg(it->second.as<std::string>());
              if (!parsed_execution_date) {
                co_return error_response(400,
                                         "Invalid execution_date, expected now "
                                         "| YYYY-MM-DD | YYYY-MM-DDTHH:MM:SSZ");
              }
              execution_date = *parsed_execution_date;
            }
          }

          auto res = co_await self->app_.trigger_run(
              DAGId{*dag_id}, TriggerType::Manual, execution_date);
          co_return res
              .transform([](const auto &run_id) {
                return json_response(
                    {{"dag_run_id", run_id.str()}, {"status", "triggered"}},
                    HttpStatus::Created);
              })
              .or_else(to_result_response)
              .value();
        });

    router.post("/api/dags/{dag_id}/pause",
                [weak_self](HttpRequest req) -> task<HttpResponse> {
                  auto self = weak_self.lock();
                  if (!self)
                    co_return error_response(503, "Service Unavailable");

                  auto dag_id = req.path_param("dag_id");
                  if (!dag_id)
                    co_return error_response(400, "Missing dag_id");

                  auto res =
                      co_await self->app_.set_dag_paused(DAGId{*dag_id}, true);
                  co_return res
                      .transform([&]() {
                        return json_response(
                            {{"dag_id", *dag_id}, {"is_paused", true}});
                      })
                      .or_else(to_result_response)
                      .value();
                });

    router.post("/api/dags/{dag_id}/unpause",
                [weak_self](HttpRequest req) -> task<HttpResponse> {
                  auto self = weak_self.lock();
                  if (!self)
                    co_return error_response(503, "Service Unavailable");

                  auto dag_id = req.path_param("dag_id");
                  if (!dag_id)
                    co_return error_response(400, "Missing dag_id");

                  auto res =
                      co_await self->app_.set_dag_paused(DAGId{*dag_id}, false);
                  co_return res
                      .transform([&]() {
                        return json_response(
                            {{"dag_id", *dag_id}, {"is_paused", false}});
                      })
                      .or_else(to_result_response)
                      .value();
                });

    router.get("/api/history", [weak_self](HttpRequest) -> task<HttpResponse> {
      auto self = weak_self.lock();
      if (!self)
        co_return error_response(503, "Service Unavailable");

      auto res = co_await self->list_run_history_async(50);
      if (!res)
        co_return to_result_response(res.error()).value();

      api_dto::HistoryResponseDto dto;
      dto.runs.reserve(res->size());
      for (const auto &run : *res) {
        auto override_state =
            co_await self->app_.get_run_state_async(run.dag_run_id);
        dto.runs.emplace_back(to_dto(run, override_state));
      }
      co_return json_response_glz(dto);
    });

    router.get("/api/history/{dag_run_id}",
               [weak_self](HttpRequest req) -> task<HttpResponse> {
                 auto self = weak_self.lock();
                 if (!self)
                   co_return error_response(503, "Service Unavailable");

                 auto dag_run_id = req.path_param("dag_run_id");
                 if (!dag_run_id)
                   co_return error_response(400, "Missing dag_run_id");

                 auto res = co_await self->get_run_history_async(
                     DAGRunId{*dag_run_id});
                 if (!res)
                   co_return to_result_response(res.error()).value();
                 auto override_state =
                     co_await self->app_.get_run_state_async(res->dag_run_id);
                 co_return json_response_glz(to_dto(*res, override_state));
               });

    router.get("/api/dags/{dag_id}/history",
               [weak_self](HttpRequest req) -> task<HttpResponse> {
                 auto self = weak_self.lock();
                 if (!self)
                   co_return error_response(503, "Service Unavailable");

                 auto dag_id = req.path_param("dag_id");
                 if (!dag_id)
                   co_return error_response(400, "Missing dag_id");

                 auto res = co_await self->list_dag_run_history_async(
                     DAGId{*dag_id}, 50);
                 if (!res)
                   co_return to_result_response(res.error()).value();

                 api_dto::DagRunHistoryResponseDto dto;
                 dto.runs.reserve(res->size());
                 for (const auto &run : *res) {
                   auto override_state =
                       co_await self->app_.get_run_state_async(run.dag_run_id);
                   auto state = override_state.value_or(run.state);
                   dto.runs.emplace_back(api_dto::DagRunSummaryDto{
                       .dag_run_id = run.dag_run_id.str(),
                       .state = enum_to_string(state),
                       .started_at = util::format_iso8601(run.started_at),
                       .finished_at = util::format_iso8601(run.finished_at),
                   });
                 }
                 co_return json_response_glz(dto);
               });

    router.get("/api/runs/{dag_run_id}/tasks/{task_id}/xcom",
               [weak_self](HttpRequest req) -> task<HttpResponse> {
                 auto self = weak_self.lock();
                 if (!self)
                   co_return error_response(503, "Service Unavailable");

                 auto dag_run_id = req.path_param("dag_run_id");
                 auto task_id = req.path_param("task_id");
                 if (!dag_run_id || !task_id) {
                   co_return error_response(400,
                                            "Missing dag_run_id or task_id");
                 }

                 auto res = co_await self->get_task_xcoms_async(
                     DAGRunId{*dag_run_id}, TaskId{*task_id});
                 co_return res
                     .transform([&](const auto &xcoms) {
                       return json_response(
                           {{"task_id", *task_id},
                            {"xcom", xcom_entries_to_json_object(xcoms)}});
                     })
                     .or_else(to_result_response)
                     .value();
               });

    router.get("/api/runs/{dag_run_id}/xcom",
               [weak_self](HttpRequest req) -> task<HttpResponse> {
                 auto self = weak_self.lock();
                 if (!self)
                   co_return error_response(503, "Service Unavailable");

                 auto dag_run_id = req.path_param("dag_run_id");
                 if (!dag_run_id)
                   co_return error_response(400, "Missing dag_run_id");

                 auto run_id = DAGRunId{*dag_run_id};
                 auto run_res = co_await self->get_run_history_async(run_id);
                 if (!run_res)
                   co_return to_result_response(run_res.error()).value();

                 auto dag_res =
                     co_await self->get_dag_snapshot_async(run_res->dag_id);
                 if (!dag_res)
                   co_return to_result_response(dag_res.error()).value();

                 json all_xcoms = json::object_t{};
                 for (const auto &task : dag_res->tasks) {
                   auto xcoms_res = co_await self->get_task_xcoms_async(
                       run_id, task.task_id);
                   if (xcoms_res && !xcoms_res->empty()) {
                     all_xcoms[task.task_id.str()] =
                         xcom_entries_to_json_object(*xcoms_res);
                   }
                 }
                 co_return json_response(
                     {{"dag_run_id", *dag_run_id}, {"xcom", all_xcoms}});
               });

    router.get(
        "/api/runs/{dag_run_id}/tasks",
        [weak_self](HttpRequest req) -> task<HttpResponse> {
          auto self = weak_self.lock();
          if (!self)
            co_return error_response(503, "Service Unavailable");

          auto dag_run_id = req.path_param("dag_run_id");
          if (!dag_run_id)
            co_return error_response(400, "Missing dag_run_id");

          auto run_id = DAGRunId{*dag_run_id};
          auto run_res = co_await self->get_run_history_async(run_id);
          if (!run_res)
            co_return to_result_response(run_res.error()).value();

          auto dag_res = co_await self->get_dag_snapshot_async(run_res->dag_id);
          if (!dag_res)
            co_return to_result_response(dag_res.error()).value();

          auto tasks_res = co_await self->get_task_instances_async(run_id);

          co_return tasks_res
              .transform([&](const auto &tasks) {
                api_dto::RunTasksResponseDto dto{
                    .dag_run_id = *dag_run_id,
                    .tasks = {}, // Initialize tasks vector
                };
                // Return one record per task (latest attempt only).
                std::unordered_map<int64_t, TaskInstanceInfo> latest_by_rowid;
                latest_by_rowid.reserve(tasks.size());
                for (const auto &t : tasks) {
                  auto it = latest_by_rowid.find(t.task_rowid);
                  if (it == latest_by_rowid.end() ||
                      t.attempt > it->second.attempt) {
                    latest_by_rowid[t.task_rowid] = t;
                  }
                }

                dto.tasks.reserve(dag_res->tasks.size());
                for (const auto &tc : dag_res->tasks) {
                  auto it = latest_by_rowid.find(tc.task_rowid);
                  if (it == latest_by_rowid.end()) {
                    dto.tasks.emplace_back(api_dto::TaskInstanceDto{
                        .task_id = tc.task_id.str(),
                        .state = enum_to_string(TaskState::Pending),
                        .attempt = 0,
                        .exit_code = 0,
                        .started_at = "",
                        .finished_at = "",
                        .error = "",
                    });
                    continue;
                  }
                  const auto &t = it->second;
                  dto.tasks.emplace_back(api_dto::TaskInstanceDto{
                      .task_id = tc.task_id.str(),
                      .state = enum_to_string(t.state),
                      .attempt = t.attempt,
                      .exit_code = t.exit_code,
                      .started_at = util::format_iso8601(t.started_at),
                      .finished_at = util::format_iso8601(t.finished_at),
                      .error = t.error_message,
                  });
                }
                return json_response_glz(dto);
              })
              .or_else(to_result_response)
              .value();
        });

    router.get("/api/runs/{dag_run_id}/logs",
               [weak_self](HttpRequest req) -> task<HttpResponse> {
                 auto self = weak_self.lock();
                 if (!self)
                   co_return error_response(503, "Service Unavailable");

                 auto dag_run_id = req.path_param("dag_run_id");
                 if (!dag_run_id)
                   co_return error_response(400, "Missing dag_run_id");

                 QueryParams qp(req.query_string);
                 std::size_t limit = 10000;
                 if (auto lim = qp.get("limit"); lim) {
                   try {
                     limit = static_cast<std::size_t>(std::stoul(*lim));
                   } catch (...) {
                   }
                 }

                 auto res = co_await self->get_run_logs_async(
                     DAGRunId{*dag_run_id}, limit);
                 if (!res)
                   co_return to_result_response(res.error()).value();

                 api_dto::RunLogsResponseDto dto;
                 dto.dag_run_id = *dag_run_id;
                 dto.logs.reserve(res->size());
                 for (const auto &e : *res) {
                   dto.logs.emplace_back(api_dto::TaskLogEntryDto{
                       .task_id = e.task_id.str(),
                       .attempt = e.attempt,
                       .stream = e.stream,
                       .logged_at = util::format_iso8601(e.logged_at),
                       .content = e.content,
                   });
                 }
                 co_return json_response_glz(dto);
               });

    router.get("/api/runs/{dag_run_id}/tasks/{task_id}/logs",
               [weak_self](HttpRequest req) -> task<HttpResponse> {
                 auto self = weak_self.lock();
                 if (!self)
                   co_return error_response(503, "Service Unavailable");

                 auto dag_run_id = req.path_param("dag_run_id");
                 auto task_id = req.path_param("task_id");
                 if (!dag_run_id || !task_id)
                   co_return error_response(400,
                                            "Missing dag_run_id or task_id");

                 QueryParams qp(req.query_string);
                 int attempt = 1;
                 if (auto att = qp.get("attempt"); att) {
                   try {
                     attempt = std::stoi(*att);
                   } catch (...) {
                   }
                 }
                 std::size_t limit = 5000;
                 if (auto lim = qp.get("limit"); lim) {
                   try {
                     limit = static_cast<std::size_t>(std::stoul(*lim));
                   } catch (...) {
                   }
                 }

                 auto res = co_await self->get_task_logs_async(
                     DAGRunId{*dag_run_id}, TaskId{*task_id}, attempt, limit);
                 if (!res)
                   co_return to_result_response(res.error()).value();

                 api_dto::TaskLogsResponseDto dto;
                 dto.dag_run_id = *dag_run_id;
                 dto.task_id = *task_id;
                 dto.logs.reserve(res->size());
                 for (const auto &e : *res) {
                   dto.logs.emplace_back(api_dto::TaskLogEntryDto{
                       .task_id = e.task_id.str(),
                       .attempt = e.attempt,
                       .stream = e.stream,
                       .logged_at = util::format_iso8601(e.logged_at),
                       .content = e.content,
                   });
                 }
                 co_return json_response_glz(dto);
               });
  }

  void start() {
    const auto &api_cfg = app_.config().api;
    if (api_cfg.tls_enabled) {
      auto tls_res = server_->set_tls_credentials(api_cfg.tls_cert_file,
                                                  api_cfg.tls_key_file);
      if (!tls_res) {
        log::error("API TLS setup failed: {}", tls_res.error().message());
        return;
      }
    }
    app_.runtime().spawn_external(
        server_->start(api_cfg.host, api_cfg.port, api_cfg.reuse_port));
  }

  void stop() {
    if (ws_hub_) {
      ws_hub_->close_all();
      const auto deadline =
          std::chrono::steady_clock::now() + std::chrono::milliseconds(300);
      while (ws_hub_->connection_count() > 0 &&
             std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
      }
    }
    if (server_) {
      server_->stop();
    }
  }

  [[nodiscard]] auto is_running() const -> bool {
    return server_ && server_->is_running();
  }

  [[nodiscard]] auto websocket_hub() const -> http::WebSocketHub & {
    return *ws_hub_;
  }
};

ApiServer::ApiServer(Application &app) : impl_(std::make_shared<Impl>(app)) {
  impl_->init();
}
ApiServer::~ApiServer() = default;

void ApiServer::start() {
  const auto impl = impl_;
  impl->start();
}

void ApiServer::stop() {
  const auto impl = impl_;
  impl->stop();
}

auto ApiServer::is_running() const -> bool {
  auto impl = impl_;
  return impl->is_running();
}

auto ApiServer::websocket_hub() -> http::WebSocketHub & {
  auto impl = impl_;
  return impl->websocket_hub();
}

} // namespace dagforge

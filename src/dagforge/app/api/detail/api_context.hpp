#pragma once

#include "dagforge/app/application.hpp"
#include "dagforge/app/http/http_server.hpp"
#include "dagforge/app/http/router.hpp"
#include "dagforge/app/http/websocket.hpp"
#include "dagforge/app/metrics_registry.hpp"
#include "dagforge/app/services/execution_service.hpp"
#include "dagforge/app/services/persistence_service.hpp"

#include <array>
#include <atomic>
#include <chrono>
#include <experimental/scope>
#include <span>
#include <string>
#include <utility>

namespace dagforge::api_detail {

inline constexpr std::array<std::uint64_t, 12> kHttpDurationBucketsNs{
    1'000'000,     5'000'000,     10'000'000,    25'000'000,
    50'000'000,    100'000'000,   250'000'000,   500'000'000,
    1'000'000'000, 2'500'000'000, 5'000'000'000, 10'000'000'000};

struct ApiContext {
  struct RunHistoryView {
    DatabaseService::RunHistoryEntry entry;
    DAGRunState state{DAGRunState::Queued};
  };

  Application &app;
  http::HttpServer &server;
  http::WebSocketHub &ws_hub;
  std::atomic<std::uint64_t> &http_active_requests;
  detail::HttpMetricsRegistry &http_metrics;

  [[nodiscard]] auto router() -> http::Router & { return server.router(); }

  template <typename Handler>
  auto make_instrumented_route(http::HttpMethod method, std::string endpoint,
                               Handler handler) -> http::RouteHandler {
    auto route_metrics = http_metrics.register_route(
        method, endpoint,
        std::span<const std::uint64_t>{kHttpDurationBucketsNs.data(),
                                       kHttpDurationBucketsNs.size()});
    return [this, route_metrics, handler = std::move(handler)](
               http::HttpRequest req) mutable -> task<http::HttpResponse> {
      http_active_requests.fetch_add(1, std::memory_order_relaxed);
      const auto active_request_guard = std::experimental::scope_exit([this] {
        http_active_requests.fetch_sub(1, std::memory_order_relaxed);
      });

      const auto started_at = std::chrono::steady_clock::now();
      auto resp = co_await handler(std::move(req));
      const auto elapsed_ns =
          std::chrono::duration_cast<std::chrono::nanoseconds>(
              std::chrono::steady_clock::now() - started_at)
              .count();
      route_metrics.record(resp.status, static_cast<std::uint64_t>(
                                            elapsed_ns > 0 ? elapsed_ns : 0));
      co_return resp;
    };
  }

  auto get_run_history_async(const DAGRunId &dag_run_id) const
      -> task<Result<DatabaseService::RunHistoryEntry>>;
  auto get_run_view_async(const DAGRunId &dag_run_id) const
      -> task<Result<RunHistoryView>>;
  auto list_dag_run_history_async(const DAGId &dag_id, std::size_t limit) const
      -> task<Result<std::vector<DatabaseService::RunHistoryEntry>>>;
  auto list_dag_run_views_async(const DAGId &dag_id, std::size_t limit) const
      -> task<Result<std::vector<RunHistoryView>>>;
  auto get_task_instances_async(const DAGRunId &dag_run_id) const
      -> task<Result<std::vector<TaskInstanceInfo>>>;
  auto get_dag_snapshot_async(const DAGId &dag_id) const
      -> task<Result<DAGInfo>>;
  auto get_task_xcoms_async(const DAGRunId &dag_run_id,
                            const TaskId &task_id) const
      -> task<Result<std::vector<XComEntry>>>;
  auto list_run_history_async(std::size_t limit) const
      -> task<Result<std::vector<DatabaseService::RunHistoryEntry>>>;
  auto list_run_views_async(std::size_t limit) const
      -> task<Result<std::vector<RunHistoryView>>>;
  auto get_run_logs_async(const DAGRunId &run_id, std::size_t limit) const
      -> task<Result<std::vector<orm::TaskLogEntry>>>;
  auto get_task_logs_async(const DAGRunId &run_id, const TaskId &task_id,
                           int attempt, std::size_t limit) const
      -> task<Result<std::vector<orm::TaskLogEntry>>>;
};

inline auto ApiContext::get_run_history_async(const DAGRunId &dag_run_id) const
    -> task<Result<DatabaseService::RunHistoryEntry>> {
  if (auto *execution = app.execution_service()) {
    auto snapshot_res = co_await execution->get_run_snapshot(dag_run_id);
    if (snapshot_res) {
      if (auto dag_id = dag_id_from_run_id(dag_run_id)) {
        co_return ok(DatabaseService::RunHistoryEntry{
            .dag_run_id = (*snapshot_res)->id().clone(),
            .dag_id = dag_id->clone(),
            .dag_rowid = (*snapshot_res)->dag_rowid(),
            .run_rowid = (*snapshot_res)->run_rowid(),
            .dag_version = (*snapshot_res)->dag_version(),
            .state = (*snapshot_res)->state(),
            .trigger_type = (*snapshot_res)->trigger_type(),
            .scheduled_at = (*snapshot_res)->scheduled_at(),
            .started_at = (*snapshot_res)->started_at(),
            .finished_at = (*snapshot_res)->finished_at(),
            .execution_date = (*snapshot_res)->execution_date(),
        });
      }
    }
  }

  auto *persistence = app.persistence_service();
  if (!persistence) {
    co_return fail(Error::DatabaseError);
  }
  co_return co_await persistence->get_run_history(dag_run_id);
}

inline auto ApiContext::get_run_view_async(const DAGRunId &dag_run_id) const
    -> task<Result<RunHistoryView>> {
  auto entry_res = co_await get_run_history_async(dag_run_id);
  if (!entry_res) {
    co_return fail(entry_res.error());
  }

  auto state_res = co_await app.get_run_state_async(entry_res->dag_run_id);
  const auto resolved_state = state_res.value_or(entry_res->state);
  co_return ok(RunHistoryView{
      .entry = std::move(*entry_res),
      .state = resolved_state,
  });
}

inline auto ApiContext::list_dag_run_history_async(const DAGId &dag_id,
                                                   std::size_t limit) const
    -> task<Result<std::vector<DatabaseService::RunHistoryEntry>>> {
  auto *persistence = app.persistence_service();
  if (!persistence) {
    co_return fail(Error::DatabaseError);
  }
  co_return co_await persistence->list_dag_run_history(dag_id, limit);
}

inline auto ApiContext::list_dag_run_views_async(const DAGId &dag_id,
                                                 std::size_t limit) const
    -> task<Result<std::vector<RunHistoryView>>> {
  auto entries_res = co_await list_dag_run_history_async(dag_id, limit);
  if (!entries_res) {
    co_return fail(entries_res.error());
  }

  std::vector<RunHistoryView> views;
  views.reserve(entries_res->size());
  for (auto &entry : *entries_res) {
    auto state_res = co_await app.get_run_state_async(entry.dag_run_id);
    const auto resolved_state = state_res.value_or(entry.state);
    views.emplace_back(RunHistoryView{
        .entry = std::move(entry),
        .state = resolved_state,
    });
  }
  co_return ok(std::move(views));
}

inline auto
ApiContext::get_task_instances_async(const DAGRunId &dag_run_id) const
    -> task<Result<std::vector<TaskInstanceInfo>>> {
  auto *persistence = app.persistence_service();
  if (!persistence) {
    co_return fail(Error::DatabaseError);
  }
  co_return co_await persistence->get_task_instances(dag_run_id);
}

inline auto ApiContext::get_dag_snapshot_async(const DAGId &dag_id) const
    -> task<Result<DAGInfo>> {
  auto *persistence = app.persistence_service();
  if (!persistence) {
    co_return fail(Error::DatabaseError);
  }
  co_return co_await persistence->get_dag(dag_id);
}

inline auto ApiContext::get_task_xcoms_async(const DAGRunId &dag_run_id,
                                             const TaskId &task_id) const
    -> task<Result<std::vector<XComEntry>>> {
  auto *persistence = app.persistence_service();
  if (!persistence) {
    co_return fail(Error::DatabaseError);
  }
  co_return co_await persistence->get_task_xcoms(dag_run_id, task_id);
}

inline auto ApiContext::list_run_history_async(std::size_t limit) const
    -> task<Result<std::vector<DatabaseService::RunHistoryEntry>>> {
  auto *persistence = app.persistence_service();
  if (!persistence) {
    co_return fail(Error::DatabaseError);
  }
  co_return co_await persistence->list_run_history(limit);
}

inline auto ApiContext::list_run_views_async(std::size_t limit) const
    -> task<Result<std::vector<RunHistoryView>>> {
  auto entries_res = co_await list_run_history_async(limit);
  if (!entries_res) {
    co_return fail(entries_res.error());
  }

  std::vector<RunHistoryView> views;
  views.reserve(entries_res->size());
  for (auto &entry : *entries_res) {
    auto state_res = co_await app.get_run_state_async(entry.dag_run_id);
    const auto resolved_state = state_res.value_or(entry.state);
    views.emplace_back(RunHistoryView{
        .entry = std::move(entry),
        .state = resolved_state,
    });
  }
  co_return ok(std::move(views));
}

inline auto ApiContext::get_run_logs_async(const DAGRunId &run_id,
                                           std::size_t limit) const
    -> task<Result<std::vector<orm::TaskLogEntry>>> {
  auto *persistence = app.persistence_service();
  if (!persistence) {
    co_return fail(Error::DatabaseError);
  }
  co_return co_await persistence->get_run_logs(run_id, limit);
}

inline auto ApiContext::get_task_logs_async(const DAGRunId &run_id,
                                            const TaskId &task_id, int attempt,
                                            std::size_t limit) const
    -> task<Result<std::vector<orm::TaskLogEntry>>> {
  auto *persistence = app.persistence_service();
  if (!persistence) {
    co_return fail(Error::DatabaseError);
  }
  co_return co_await persistence->get_task_logs(run_id, task_id, attempt,
                                                limit);
}

} // namespace dagforge::api_detail

#pragma once

#include "../api_context.hpp"
#include "../dto_mapper.hpp"

namespace dagforge::api_detail {

inline auto register_log_routes(ApiContext &ctx) -> void {
  auto &router = ctx.router();

  router.get(
      "/api/runs/{dag_run_id}/logs",
      ctx.make_instrumented_route(
          http::HttpMethod::GET, "/api/runs/{dag_run_id}/logs",
          [&ctx](http::HttpRequest req) -> task<http::HttpResponse> {
            auto dag_run_id = req.path_param("dag_run_id");
            if (!dag_run_id) {
              co_return error_response(400, "Missing dag_run_id");
            }

            http::QueryParams qp(req.query_string);
            std::size_t limit = 10000;
            if (auto lim = qp.get("limit"); lim) {
              try {
                limit = static_cast<std::size_t>(std::stoul(*lim));
              } catch (const std::exception &e) {
                log::warn("Ignoring invalid /api/runs/{{dag_run_id}}/logs "
                          "limit '{}': {}",
                          *lim, e.what());
              }
            }

            auto res = co_await ctx.get_run_logs_async(DAGRunId{*dag_run_id}, limit);
            if (!res) {
              co_return to_result_response(res.error()).value();
            }

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
          }));

  router.get(
      "/api/runs/{dag_run_id}/tasks/{task_id}/logs",
      ctx.make_instrumented_route(
          http::HttpMethod::GET, "/api/runs/{dag_run_id}/tasks/{task_id}/logs",
          [&ctx](http::HttpRequest req) -> task<http::HttpResponse> {
            auto dag_run_id = req.path_param("dag_run_id");
            auto task_id = req.path_param("task_id");
            if (!dag_run_id || !task_id) {
              co_return error_response(400, "Missing dag_run_id or task_id");
            }

            http::QueryParams qp(req.query_string);
            int attempt = 1;
            if (auto att = qp.get("attempt"); att) {
              try {
                attempt = std::stoi(*att);
              } catch (const std::exception &e) {
                log::warn("Ignoring invalid /api/runs/{{dag_run_id}}/"
                          "tasks/{{task_id}}/logs attempt '{}': {}",
                          *att, e.what());
              }
            }
            std::size_t limit = 5000;
            if (auto lim = qp.get("limit"); lim) {
              try {
                limit = static_cast<std::size_t>(std::stoul(*lim));
              } catch (const std::exception &e) {
                log::warn("Ignoring invalid /api/runs/{{dag_run_id}}/"
                          "tasks/{{task_id}}/logs limit '{}': {}",
                          *lim, e.what());
              }
            }

            auto res = co_await ctx.get_task_logs_async(DAGRunId{*dag_run_id},
                                                        TaskId{*task_id},
                                                        attempt, limit);
            if (!res) {
              co_return to_result_response(res.error()).value();
            }

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
          }));
}

} // namespace dagforge::api_detail

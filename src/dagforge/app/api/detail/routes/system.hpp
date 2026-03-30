#pragma once

#include "../api_context.hpp"
#include "../dto_mapper.hpp"
#include "dagforge/app/metrics_exporter.hpp"

namespace dagforge::api_detail {

inline auto register_system_routes(ApiContext &ctx) -> void {
  auto &router = ctx.router();

  router.get("/api/health",
             ctx.make_instrumented_route(
                 http::HttpMethod::GET, "/api/health",
                 [](http::HttpRequest) -> task<http::HttpResponse> {
                   co_return json_response({{"status", "healthy"}});
                 }));

  router.get("/api/status",
             ctx.make_instrumented_route(
                 http::HttpMethod::GET, "/api/status",
                 [&ctx](http::HttpRequest) -> task<http::HttpResponse> {
                   co_return json_response({
                       {"dag_count", ctx.app.dag_manager().dag_count()},
                       {"active_runs", ctx.app.has_active_runs()},
                       {"timestamp", util::format_timestamp()},
                   });
                 }));

  router.get(
      "/metrics",
      ctx.make_instrumented_route(
          http::HttpMethod::GET, "/metrics",
          [&ctx](http::HttpRequest) -> task<http::HttpResponse> {
            co_return text_response(render_prometheus_metrics(ctx.app),
                                    http::HttpStatus::Ok,
                                    "text/plain; version=0.0.4; charset=utf-8");
          }));
}

} // namespace dagforge::api_detail

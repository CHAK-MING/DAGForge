#pragma once

#include "api_context.hpp"

namespace dagforge::api_detail {

inline auto setup_websocket(ApiContext &ctx) -> void {
  ctx.server.set_websocket_handler(
      [&ctx](std::shared_ptr<http::IWebSocketConnection> conn) -> spawn_task {
        const int fd_num = conn->fd();
        ctx.ws_hub.add_connection(conn);

        dagforge::log::debug("WebSocket client connected: fd={}", fd_num);

        co_await conn->handle_frames(
            [&ctx, fd_num](http::WebSocketOpCode opcode,
                           [[maybe_unused]] std::span<const std::byte> data) {
              if (opcode == http::WebSocketOpCode::Close) {
                ctx.ws_hub.remove_connection(fd_num);
                dagforge::log::debug("WebSocket client disconnected: fd={}",
                                     fd_num);
              }
            });

        ctx.ws_hub.remove_connection(fd_num);
      });
}

} // namespace dagforge::api_detail

#pragma once

#include "dagforge/app/http/websocket.hpp"
#include "dagforge/client/http/http_types.hpp"
#include "dagforge/core/coroutine.hpp"
#include "dagforge/core/error.hpp"

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <string_view>

namespace dagforge {
class Runtime;
}

namespace dagforge::http {

class Router;

using WebSocketHandler = std::move_only_function<spawn_task(
    std::shared_ptr<IWebSocketConnection> connection)>;

class HttpServer {
public:
  explicit HttpServer(Runtime &runtime);
  ~HttpServer();

  HttpServer(const HttpServer &) = delete;
  auto operator=(const HttpServer &) -> HttpServer & = delete;

  auto router() -> Router &;
  auto set_websocket_handler(WebSocketHandler handler) -> void;
  [[nodiscard]] auto set_tls_credentials(std::string cert_chain_file,
                                         std::string private_key_file)
      -> Result<void>;

  auto start(std::string_view host, uint16_t port) -> task<Result<void>>;
  auto start(std::string_view host, uint16_t port, bool reuse_port)
      -> task<Result<void>>;
  auto stop() -> void;

  [[nodiscard]] auto is_running() const -> bool;

private:
  struct Impl;
  std::shared_ptr<Impl> impl_;
};

} // namespace dagforge::http

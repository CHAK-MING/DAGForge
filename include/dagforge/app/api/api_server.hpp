#pragma once

#include "dagforge/app/http/websocket.hpp"
#include "dagforge/io/result.hpp"
#include "dagforge/util/id.hpp"

#include <memory>

namespace dagforge {

class Application;

namespace http {
class HttpServer;
class WebSocketHub;
} // namespace http

class ApiServer {
public:
  explicit ApiServer(Application &app);
  ~ApiServer();

  [[nodiscard]] auto start() -> Result<void>;
  void stop();
  [[nodiscard]] bool is_running() const;
  http::WebSocketHub &websocket_hub();

private:
  struct Impl;
  std::shared_ptr<Impl> impl_;
};

} // namespace dagforge

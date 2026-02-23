#pragma once

#include "dagforge/client/http/http_types.hpp"
#include "dagforge/core/coroutine.hpp"

#include <boost/asio/generic/stream_protocol.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/ssl/stream.hpp>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <vector>

namespace dagforge {
class Runtime;
} // namespace dagforge

namespace dagforge::http {

enum class WebSocketOpCode : std::uint8_t {
  Continuation = 0x0,
  Text = 0x1,
  Binary = 0x2,
  Close = 0x8,
  Ping = 0x9,
  Pong = 0xA
};

class IWebSocketConnection {
public:
  virtual ~IWebSocketConnection() = default;

  virtual auto send_text(std::string text) -> spawn_task = 0;
  virtual auto send_binary(std::vector<std::byte> data) -> spawn_task = 0;
  virtual auto send_close() -> spawn_task = 0;
  virtual auto send_pong(std::vector<std::byte> data) -> spawn_task = 0;
  virtual auto handle_frames(
      std::move_only_function<void(WebSocketOpCode, std::span<const std::byte>)>
          on_message) -> spawn_task = 0;
  [[nodiscard]] virtual auto is_closed() const -> bool = 0;
  [[nodiscard]] virtual auto fd() const -> int = 0;
  virtual auto force_close() -> void = 0;
};

class WebSocketConnection
    : public IWebSocketConnection,
      public std::enable_shared_from_this<WebSocketConnection> {
public:
  WebSocketConnection(boost::asio::generic::stream_protocol::socket socket,
                      HttpRequest upgrade_request);
  explicit WebSocketConnection(
      boost::asio::generic::stream_protocol::socket socket);
  ~WebSocketConnection();

  WebSocketConnection(const WebSocketConnection &) = delete;
  auto operator=(const WebSocketConnection &) -> WebSocketConnection & = delete;

  auto send_text(std::string text) -> spawn_task override;
  auto send_binary(std::vector<std::byte> data) -> spawn_task override;
  auto send_close() -> spawn_task override;
  auto send_pong(std::vector<std::byte> data) -> spawn_task override;

  auto handle_frames(
      std::move_only_function<void(WebSocketOpCode, std::span<const std::byte>)>
          on_message) -> spawn_task override;

  [[nodiscard]] auto is_closed() const -> bool override;
  [[nodiscard]] auto fd() const -> int override;
  auto force_close() -> void override;

private:
  struct Impl;
  std::shared_ptr<Impl> impl_;
};

class TlsWebSocketConnection
    : public IWebSocketConnection,
      public std::enable_shared_from_this<TlsWebSocketConnection> {
public:
  TlsWebSocketConnection(
      boost::asio::ssl::stream<boost::asio::ip::tcp::socket> stream,
      HttpRequest upgrade_request);
  ~TlsWebSocketConnection();

  TlsWebSocketConnection(const TlsWebSocketConnection &) = delete;
  auto operator=(const TlsWebSocketConnection &)
      -> TlsWebSocketConnection & = delete;

  auto send_text(std::string text) -> spawn_task override;
  auto send_binary(std::vector<std::byte> data) -> spawn_task override;
  auto send_close() -> spawn_task override;
  auto send_pong(std::vector<std::byte> data) -> spawn_task override;
  auto handle_frames(
      std::move_only_function<void(WebSocketOpCode, std::span<const std::byte>)>
          on_message) -> spawn_task override;

  [[nodiscard]] auto is_closed() const -> bool override;
  [[nodiscard]] auto fd() const -> int override;
  auto force_close() -> void override;

private:
  struct Impl;
  std::shared_ptr<Impl> impl_;
};

class WebSocketHub {
public:
  explicit WebSocketHub(Runtime &runtime);
  ~WebSocketHub();

  WebSocketHub(const WebSocketHub &) = delete;
  auto operator=(const WebSocketHub &) -> WebSocketHub & = delete;

  struct LogMessage {
    std::string timestamp;
    std::string dag_run_id;
    std::string task_id;
    std::string stream;
    std::string content;
  };

  struct EventMessage {
    std::string timestamp;
    std::string event;
    std::string dag_run_id;
    std::string task_id;
    std::string data;
  };

  auto add_connection(std::shared_ptr<IWebSocketConnection> conn,
                      std::optional<std::string> run_filter = std::nullopt)
      -> void;
  auto remove_connection(int fd) -> void;

  auto broadcast_log(const LogMessage &msg) -> void;
  auto broadcast_event(const EventMessage &event) -> void;

  [[nodiscard]] auto connection_count() const -> size_t;
  auto close_all() -> void;

private:
  struct Impl;
  std::shared_ptr<Impl> impl_;
};

} // namespace dagforge::http

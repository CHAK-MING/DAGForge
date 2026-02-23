#include "dagforge/client/http/http_client.hpp"

#include "dagforge/core/asio_awaitable.hpp"
#include "dagforge/util/log.hpp"

#include <boost/asio/cancel_after.hpp>
#include <boost/asio/connect.hpp>
#include <boost/asio/write.hpp>
#include <boost/beast/core/flat_buffer.hpp>
#include <boost/beast/http.hpp>

#include <string>

namespace dagforge::http {

namespace {

namespace beast = boost::beast;
namespace beast_http = beast::http;

auto to_response(
    const beast_http::response<beast_http::vector_body<uint8_t>> &msg)
    -> HttpResponse {
  HttpResponse out;
  out.status = static_cast<HttpStatus>(msg.result_int());
  out.headers.reserve(static_cast<std::size_t>(
      std::distance(msg.base().begin(), msg.base().end())));
  for (const auto &field : msg.base()) {
    out.headers.emplace(field.name_string(), field.value());
  }
  out.body = msg.body();
  return out;
}

template <typename Stream>
auto request_over_stream(Stream &stream, HttpRequest req,
                         HttpClientConfig config, const std::string &host)
    -> task<HttpResponse> {
  if (!req.headers.contains("Host")) {
    req.headers["Host"] = host;
  }
  if (config.keep_alive && !req.headers.contains("Connection")) {
    req.headers["Connection"] = "keep-alive";
  }

  auto request_data = req.serialize();
  auto [write_ec, written] = co_await boost::asio::async_write(
      stream, boost::asio::buffer(request_data),
      boost::asio::cancel_after(config.read_timeout, use_nothrow));
  (void)written;
  if (write_ec) {
    log::error("Failed to write request: {}", write_ec.message());
    co_return HttpResponse{
        .status = HttpStatus::InternalServerError, .headers = {}, .body = {}};
  }

  beast::flat_buffer read_buffer;
  beast_http::response_parser<beast_http::vector_body<uint8_t>> parser;
  parser.header_limit(256 * 1024);
  parser.body_limit(config.max_response_size);

  auto [read_ec, read_n] = co_await beast_http::async_read(
      stream, read_buffer, parser,
      boost::asio::cancel_after(config.read_timeout, use_nothrow));
  (void)read_n;
  if (read_ec) {
    log::error("Failed to read response: {}", read_ec.message());
    co_return HttpResponse{
        .status = HttpStatus::InternalServerError, .headers = {}, .body = {}};
  }

  co_return to_response(parser.release());
}

} // namespace

struct HttpClient::Impl {
  SocketVariant socket;
  HttpClientConfig config;
  std::string host;

  Impl(SocketVariant socket_in, HttpClientConfig cfg)
      : socket(std::move(socket_in)), config(cfg) {}
};

HttpClient::HttpClient(SocketVariant socket, HttpClientConfig config)
    : impl_(std::make_unique<Impl>(std::move(socket), config)) {}

HttpClient::~HttpClient() = default;

HttpClient::HttpClient(HttpClient &&) noexcept = default;
auto HttpClient::operator=(HttpClient &&) noexcept -> HttpClient & = default;

auto HttpClient::connect_tcp(io::IoContext &ctx, std::string_view host,
                             uint16_t port, HttpClientConfig config)
    -> task<Result<std::unique_ptr<HttpClient>>> {
  boost::system::error_code ec;
  boost::asio::ip::tcp::resolver resolver(ctx);
  std::string host_str(host);
  std::string port_str = std::to_string(port);
  auto endpoints = resolver.resolve(host_str, port_str, ec);
  if (ec) {
    log::debug("Failed to resolve {}:{} - {}", host, port, ec.message());
    co_return fail(Error::InvalidUrl);
  }

  boost::asio::ip::tcp::socket socket(ctx);
  auto [connect_ec, endpoint] = co_await boost::asio::async_connect(
      socket, endpoints,
      boost::asio::cancel_after(config.connect_timeout, use_nothrow));
  (void)endpoint;
  if (connect_ec) {
    log::debug("Failed to connect to {}:{} - {}", host, port,
               connect_ec.message());
    co_return fail(std::error_code(connect_ec.value(), std::system_category()));
  }

  auto client = std::make_unique<HttpClient>(std::move(socket), config);
  client->impl_->host = std::string(host);
  co_return ok(std::move(client));
}

auto HttpClient::connect_unix(io::IoContext &ctx, std::string_view socket_path,
                              HttpClientConfig config)
    -> task<Result<std::unique_ptr<HttpClient>>> {
  boost::system::error_code ec;
  boost::asio::local::stream_protocol::socket socket(ctx);
  socket.open(boost::asio::local::stream_protocol(), ec);
  if (ec) {
    log::debug("Failed to open unix socket {} - {}", socket_path, ec.message());
    co_return fail(std::error_code(ec.value(), std::system_category()));
  }
  socket.non_blocking(true, ec);
  if (ec) {
    log::debug("Failed to set non-blocking for {} - {}", socket_path,
               ec.message());
    co_return fail(std::error_code(ec.value(), std::system_category()));
  }
  auto [connect_ec] = co_await socket.async_connect(
      boost::asio::local::stream_protocol::endpoint(std::string(socket_path)),
      boost::asio::cancel_after(config.connect_timeout, use_nothrow));
  if (connect_ec) {
    log::debug("Failed to connect to {} - {}", socket_path,
               connect_ec.message());
    co_return fail(std::error_code(connect_ec.value(), std::system_category()));
  }

  auto client = std::make_unique<HttpClient>(std::move(socket), config);
  client->impl_->host = "localhost";
  co_return ok(std::move(client));
}

auto HttpClient::request(HttpRequest req) -> task<HttpResponse> {
  if (!is_connected()) {
    co_return HttpResponse{
        .status = HttpStatus::InternalServerError, .headers = {}, .body = {}};
  }

  if (auto *tcp = std::get_if<boost::asio::ip::tcp::socket>(&impl_->socket)) {
    co_return co_await request_over_stream(*tcp, std::move(req), impl_->config,
                                           impl_->host);
  }
  auto *unix_socket =
      std::get_if<boost::asio::local::stream_protocol::socket>(&impl_->socket);
  co_return co_await request_over_stream(*unix_socket, std::move(req),
                                         impl_->config, impl_->host);
}

auto HttpClient::get(std::string_view path, const HttpHeaders &headers)
    -> task<HttpResponse> {
  HttpRequest req;
  req.method = HttpMethod::GET;
  req.path = std::string(path);
  req.headers = headers;
  co_return co_await request(std::move(req));
}

auto HttpClient::post(std::string_view path, std::vector<uint8_t> body,
                      const HttpHeaders &headers) -> task<HttpResponse> {
  HttpRequest req;
  req.method = HttpMethod::POST;
  req.path = std::string(path);
  req.body = std::move(body);
  req.headers = headers;
  co_return co_await request(std::move(req));
}

auto HttpClient::post_json(std::string_view path, std::string_view json,
                           const HttpHeaders &headers) -> task<HttpResponse> {
  HttpRequest req;
  req.method = HttpMethod::POST;
  req.path = std::string(path);
  req.body = std::vector<uint8_t>(json.begin(), json.end());
  req.headers = headers;
  req.headers["Content-Type"] = "application/json";
  co_return co_await request(std::move(req));
}

auto HttpClient::delete_(std::string_view path, const HttpHeaders &headers)
    -> task<HttpResponse> {
  HttpRequest req;
  req.method = HttpMethod::DELETE;
  req.path = std::string(path);
  req.headers = headers;
  co_return co_await request(std::move(req));
}

auto HttpClient::put(std::string_view path, std::vector<uint8_t> body,
                     const HttpHeaders &headers) -> task<HttpResponse> {
  HttpRequest req;
  req.method = HttpMethod::PUT;
  req.path = std::string(path);
  req.body = std::move(body);
  req.headers = headers;
  co_return co_await request(std::move(req));
}

auto HttpClient::is_connected() const noexcept -> bool {
  if (auto *tcp = std::get_if<boost::asio::ip::tcp::socket>(&impl_->socket)) {
    return tcp->is_open();
  }
  if (auto *unix_socket =
          std::get_if<boost::asio::local::stream_protocol::socket>(
              &impl_->socket)) {
    return unix_socket->is_open();
  }
  return false;
}

auto HttpClient::close() -> void {
  boost::system::error_code ec;
  if (auto *tcp = std::get_if<boost::asio::ip::tcp::socket>(&impl_->socket)) {
    tcp->close(ec);
    return;
  }
  if (auto *unix_socket =
          std::get_if<boost::asio::local::stream_protocol::socket>(
              &impl_->socket)) {
    unix_socket->close(ec);
  }
}

} // namespace dagforge::http

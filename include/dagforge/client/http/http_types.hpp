#pragma once

#include "dagforge/core/error.hpp"
#include "dagforge/util/string_hash.hpp"

#include <cstdint>
#include <format>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace dagforge::http {

enum class HttpMethod : std::uint8_t {
  GET,
  POST,
  PUT,
  DELETE,
  PATCH,
  OPTIONS,
  HEAD
};

enum class HttpStatus : std::uint16_t {
  Ok = 200,
  Created = 201,
  Accepted = 202,
  NoContent = 204,

  MovedPermanently = 301,
  Found = 302,
  NotModified = 304,

  BadRequest = 400,
  Unauthorized = 401,
  Forbidden = 403,
  NotFound = 404,
  MethodNotAllowed = 405,
  Conflict = 409,

  InternalServerError = 500,
  NotImplemented = 501,
  BadGateway = 502,
  ServiceUnavailable = 503
};

using HttpHeaders =
    std::unordered_map<std::string, std::string, dagforge::StringHash,
                       dagforge::StringEqual>;

class QueryParams {
public:
  QueryParams() = default;
  explicit QueryParams(std::string_view query_string);

  [[nodiscard]] auto get(std::string_view key) const -> Result<std::string>;
  [[nodiscard]] auto has(std::string_view key) const -> bool;
  [[nodiscard]] auto size() const -> std::size_t { return params_.size(); }

private:
  std::unordered_map<std::string, std::string, dagforge::StringHash,
                     dagforge::StringEqual>
      params_;
};

struct HttpRequest {
  HttpMethod method{HttpMethod::GET};
  std::string path;
  std::string query_string;
  int version_major{1};
  int version_minor{1};
  HttpHeaders headers;
  std::vector<uint8_t> body;
  // Mutable: populated by Router during const route() method
  mutable std::unordered_map<std::string, std::string, dagforge::StringHash,
                             dagforge::StringEqual>
      path_params;

  [[nodiscard]] auto header(std::string_view key) const -> Result<std::string>;
  [[nodiscard]] auto is_websocket_upgrade() const -> bool;
  [[nodiscard]] auto body_as_string() const -> std::string_view;
  [[nodiscard]] auto path_param(std::string_view key) const
      -> Result<std::string>;

  [[nodiscard]] auto serialize() const -> std::vector<uint8_t>;
};

struct HttpResponse {
  HttpStatus status{HttpStatus::Ok};
  HttpHeaders headers;
  std::vector<uint8_t> body;

  [[nodiscard]] static auto ok() -> HttpResponse;
  [[nodiscard]] static auto json(std::string_view json_str) -> HttpResponse;
  [[nodiscard]] static auto not_found() -> HttpResponse;
  [[nodiscard]] static auto bad_request() -> HttpResponse;
  [[nodiscard]] static auto internal_error() -> HttpResponse;

  auto set_header(std::string key, std::string value) -> HttpResponse &;
  auto set_body(std::string body_str) -> HttpResponse &;
  auto set_body(std::vector<uint8_t> body_data) -> HttpResponse &;

  [[nodiscard]] auto serialize() const -> std::vector<uint8_t>;
};

[[nodiscard]] auto status_reason_phrase(HttpStatus status) -> std::string_view;

} // namespace dagforge::http

template <>
struct std::formatter<dagforge::http::HttpMethod>
    : std::formatter<std::string_view> {
  auto format(dagforge::http::HttpMethod method, auto &ctx) const {
    using enum dagforge::http::HttpMethod;
    std::string_view name = [method] {
      switch (method) {
      case GET:
        return "GET";
      case POST:
        return "POST";
      case PUT:
        return "PUT";
      case DELETE:
        return "DELETE";
      case PATCH:
        return "PATCH";
      case OPTIONS:
        return "OPTIONS";
      case HEAD:
        return "HEAD";
      }
      return "UNKNOWN";
    }();
    return std::formatter<std::string_view>::format(name, ctx);
  }
};

template <>
struct std::formatter<dagforge::http::HttpStatus>
    : std::formatter<std::uint16_t> {
  auto format(dagforge::http::HttpStatus status, auto &ctx) const {
    return std::formatter<std::uint16_t>::format(
        static_cast<std::uint16_t>(status), ctx);
  }
};

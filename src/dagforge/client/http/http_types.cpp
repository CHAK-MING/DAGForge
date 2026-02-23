#include "dagforge/client/http/http_types.hpp"

#include <boost/algorithm/string/predicate.hpp>
#include <boost/beast/http/status.hpp>
#include <boost/url/params_view.hpp>

#include <algorithm>
#include <format>
#include <utility>

namespace dagforge::http {

auto HttpRequest::header(std::string_view key) const -> Result<std::string> {
  auto it = headers.find(std::string(key));
  if (it != headers.end()) {
    return ok(it->second);
  }
  return fail(Error::NotFound);
}

auto HttpRequest::is_websocket_upgrade() const -> bool {
  auto upgrade = header("Upgrade");
  auto connection = header("Connection");

  if (!upgrade || !connection) {
    return false;
  }

  return boost::algorithm::iequals(*upgrade, "websocket") &&
         boost::algorithm::icontains(*connection, "upgrade");
}

auto HttpRequest::body_as_string() const -> std::string_view {
  return {reinterpret_cast<const char *>(body.data()), body.size()};
}

auto HttpRequest::path_param(std::string_view key) const
    -> Result<std::string> {
  auto it = path_params.find(std::string(key));
  if (it != path_params.end()) {
    return ok(it->second);
  }
  return fail(Error::NotFound);
}

auto HttpRequest::serialize() const -> std::vector<uint8_t> {
  std::vector<uint8_t> result;

  // Pre-allocate: request line (~40) + headers (~40 each) + body
  result.reserve(256 + body.size());

  // Write request line directly to vector (zero-copy)
  std::format_to(std::back_inserter(result), "{} {} HTTP/1.1\r\n", method,
                 path);

  // Write headers directly (zero-copy)
  bool has_content_length = false;
  bool has_host = false;

  for (const auto &[key, value] : headers) {
    std::format_to(std::back_inserter(result), "{}: {}\r\n", key, value);
    if (key == "Content-Length") {
      has_content_length = true;
    }
    if (key == "Host") {
      has_host = true;
    }
  }

  // Add missing Host header
  if (!has_host) {
    constexpr std::string_view host_header = "Host: localhost\r\n";
    result.insert(result.end(), host_header.begin(), host_header.end());
  }

  // Add Content-Length if missing
  if (!has_content_length && !body.empty()) {
    std::format_to(std::back_inserter(result), "Content-Length: {}\r\n",
                   body.size());
  }

  // End of headers
  result.emplace_back('\r');
  result.emplace_back('\n');

  // Append body (single copy, unavoidable)
  result.insert(result.end(), body.begin(), body.end());

  return result;
}

QueryParams::QueryParams(std::string_view query_string) {
  if (query_string.empty()) {
    return;
  }
  try {
    boost::urls::params_view parsed(query_string);
    for (const auto &param : parsed) {
      params_[std::string(param.key)] = std::string(param.value);
    }
  } catch (...) {
    return;
  }
}

auto QueryParams::get(std::string_view key) const -> Result<std::string> {
  auto it = params_.find(std::string(key));
  if (it != params_.end())
    return ok(it->second);
  return fail(Error::NotFound);
}

auto QueryParams::has(std::string_view key) const -> bool {
  return params_.contains(std::string(key));
}

auto HttpResponse::ok() -> HttpResponse {
  return {.status = HttpStatus::Ok, .headers = {}, .body = {}};
}

auto HttpResponse::json(std::string_view json_str) -> HttpResponse {
  HttpResponse resp{.status = HttpStatus::Ok, .headers = {}, .body = {}};
  resp.headers["Content-Type"] = "application/json";
  resp.body.assign(json_str.begin(), json_str.end());
  return resp;
}

auto HttpResponse::not_found() -> HttpResponse {
  return {.status = HttpStatus::NotFound, .headers = {}, .body = {}};
}

auto HttpResponse::bad_request() -> HttpResponse {
  return {.status = HttpStatus::BadRequest, .headers = {}, .body = {}};
}

auto HttpResponse::internal_error() -> HttpResponse {
  return {.status = HttpStatus::InternalServerError, .headers = {}, .body = {}};
}

auto HttpResponse::set_header(std::string key, std::string value)
    -> HttpResponse & {
  headers[std::move(key)] = std::move(value);
  return *this;
}

auto HttpResponse::set_body(std::string body_str) -> HttpResponse & {
  body.assign(body_str.begin(), body_str.end());
  return *this;
}

auto HttpResponse::set_body(std::vector<uint8_t> body_data) -> HttpResponse & {
  body = std::move(body_data);
  return *this;
}

auto HttpResponse::serialize() const -> std::vector<uint8_t> {
  std::vector<uint8_t> result;

  // Pre-allocate: status line (~30) + headers (~40 each) + body
  result.reserve(256 + body.size());

  // Write status line directly to vector (zero-copy)
  std::format_to(std::back_inserter(result), "HTTP/1.1 {} {}\r\n", status,
                 status_reason_phrase(status));

  // Write headers directly (zero-copy)
  bool has_content_length = false;
  for (const auto &[key, value] : headers) {
    std::format_to(std::back_inserter(result), "{}: {}\r\n", key, value);
    if (key == "Content-Length") {
      has_content_length = true;
    }
  }

  // Add Content-Length if missing
  if (!has_content_length && !body.empty()) {
    std::format_to(std::back_inserter(result), "Content-Length: {}\r\n",
                   body.size());
  }

  // Security headers
  constexpr std::string_view kSecurityHeaders =
      "X-Frame-Options: DENY\r\n"
      "X-Content-Type-Options: nosniff\r\n"
      "Cache-Control: no-store\r\n";
  result.insert(result.end(), kSecurityHeaders.begin(), kSecurityHeaders.end());

  // End of headers
  result.emplace_back('\r');
  result.emplace_back('\n');

  // Append body (single copy, unavoidable)
  result.insert(result.end(), body.begin(), body.end());

  return result;
}

auto status_reason_phrase(HttpStatus status) -> std::string_view {
  namespace beast_http = boost::beast::http;
  return beast_http::obsolete_reason(static_cast<beast_http::status>(status));
}

} // namespace dagforge::http

#include "dagforge/client/http/http_parser.hpp"

#include "gtest/gtest.h"

#include <string>
#include <vector>

using namespace dagforge;

namespace {

auto to_bytes(std::string_view text) -> std::vector<std::uint8_t> {
  return std::vector<std::uint8_t>(text.begin(), text.end());
}

} // namespace

TEST(HttpParserTest, RequestParserExtractsPathQueryHeadersAndBody) {
  http::HttpRequestParser parser;
  const auto raw = to_bytes(
      "POST /api/dags?limit=10&dag_id=demo HTTP/1.1\r\n"
      "Host: localhost\r\n"
      "Content-Type: application/json\r\n"
      "Content-Length: 11\r\n"
      "\r\n"
      "{\"ok\":true}");

  auto parsed = parser.parse(raw);
  ASSERT_TRUE(parsed.has_value()) << parsed.error().message();
  EXPECT_EQ(parsed->method, http::HttpMethod::POST);
  EXPECT_EQ(parsed->path, "/api/dags");
  EXPECT_EQ(parsed->query_string, "limit=10&dag_id=demo");
  EXPECT_EQ(parsed->version_major, 1);
  EXPECT_EQ(parsed->version_minor, 1);
  ASSERT_TRUE(parsed->header("Host").has_value());
  EXPECT_EQ(*parsed->header("Host"), "localhost");
  EXPECT_EQ(parsed->body_as_string(), "{\"ok\":true}");
}

TEST(HttpParserTest, RequestParserReturnsIncompleteForPartialMessage) {
  http::HttpRequestParser parser;
  const auto raw = to_bytes("GET /api/dags HTTP/1.1\r\nHost: localhost\r\n");

  auto parsed = parser.parse(raw);
  ASSERT_FALSE(parsed.has_value());
  EXPECT_EQ(parsed.error(), make_error_code(Error::Incomplete));
}

TEST(HttpParserTest, RequestParserReturnsProtocolErrorForMalformedMessage) {
  http::HttpRequestParser parser;
  const auto raw = to_bytes("GET /api/dags\r\nHost: localhost\r\n\r\n");

  auto parsed = parser.parse(raw);
  ASSERT_FALSE(parsed.has_value());
  EXPECT_EQ(parsed.error(), make_error_code(Error::ProtocolError));
}

TEST(HttpParserTest, RequestParserFallsBackToGetForUnknownVerb) {
  http::HttpRequestParser parser;
  const auto raw = to_bytes(
      "BREW /tea HTTP/1.1\r\n"
      "Host: localhost\r\n"
      "\r\n");

  auto parsed = parser.parse(raw);
  ASSERT_TRUE(parsed.has_value()) << parsed.error().message();
  EXPECT_EQ(parsed->method, http::HttpMethod::GET);
  EXPECT_EQ(parsed->path, "/tea");
}

TEST(HttpParserTest, ResponseParserExtractsStatusHeadersAndBody) {
  http::HttpResponseParser parser;
  const auto raw = to_bytes(
      "HTTP/1.1 404 Not Found\r\n"
      "Content-Type: application/json\r\n"
      "Content-Length: 19\r\n"
      "\r\n"
      "{\"error\":\"missing\"}");

  auto parsed = parser.parse(raw);
  ASSERT_TRUE(parsed.has_value()) << parsed.error().message();
  EXPECT_EQ(parsed->status, http::HttpStatus::NotFound);
  ASSERT_TRUE(parsed->headers.contains("Content-Type"));
  EXPECT_EQ(parsed->headers.at("Content-Type"), "application/json");
  EXPECT_EQ(std::string(parsed->body.begin(), parsed->body.end()),
            "{\"error\":\"missing\"}");
}

TEST(HttpParserTest, ResponseParserReturnsIncompleteForPartialMessage) {
  http::HttpResponseParser parser;
  const auto raw = to_bytes("HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\n");

  auto parsed = parser.parse(raw);
  ASSERT_FALSE(parsed.has_value());
  EXPECT_EQ(parsed.error(), make_error_code(Error::Incomplete));
}

TEST(HttpParserTest, ResponseParserReturnsProtocolErrorForMalformedMessage) {
  http::HttpResponseParser parser;
  const auto raw = to_bytes("HTTP/1.1 ???\r\n\r\n");

  auto parsed = parser.parse(raw);
  ASSERT_FALSE(parsed.has_value());
  EXPECT_EQ(parsed.error(), make_error_code(Error::ProtocolError));
}

TEST(HttpParserTest, QueryParamsIgnoresInvalidEncoding) {
  http::QueryParams params{"dag_id=%zz&limit=10"};
  EXPECT_FALSE(params.has("dag_id"));
  EXPECT_EQ(params.size(), 0U);
}

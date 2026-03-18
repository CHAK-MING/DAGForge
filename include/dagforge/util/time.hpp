#pragma once

#include <chrono>
#include <cstdint>
#include <ctime>
#include <format>
#include <string>

namespace dagforge::util {

namespace detail {

template <typename FormatFn>
[[nodiscard]] inline auto format_time_or(
    std::chrono::system_clock::time_point tp, std::string_view empty_value,
    FormatFn &&format_fn) -> std::string {
  if (tp == std::chrono::system_clock::time_point{}) {
    return std::string{empty_value};
  }
  return std::forward<FormatFn>(format_fn)(tp);
}

} // namespace detail

// Formats time point to ISO 8601 (YYYY-MM-DDTHH:MM:SSZ)
[[nodiscard]] inline auto
format_iso8601(std::chrono::system_clock::time_point tp) -> std::string {
  return detail::format_time_or(
      tp, "", [](std::chrono::system_clock::time_point value) {
        auto const sec_tp = std::chrono::floor<std::chrono::seconds>(value);
        return std::format("{:%Y-%m-%dT%H:%M:%SZ}", sec_tp);
      });
}

// Formats current time to ISO 8601
[[nodiscard]] inline auto format_timestamp() -> std::string {
  return format_iso8601(std::chrono::system_clock::now());
}

// Formats time point to local timestamp (YYYY-MM-DD HH:MM:SS)
[[nodiscard]] inline auto
format_local_timestamp(std::chrono::system_clock::time_point tp)
    -> std::string {
  return detail::format_time_or(
      tp, "-", [](std::chrono::system_clock::time_point value) {
        return std::format("{:%Y-%m-%d %H:%M:%S}", value);
      });
}

// Formats time point to short local timestamp (YYYY-MM-DD HH:MM)
[[nodiscard]] inline auto
format_local_timestamp_short(std::chrono::system_clock::time_point tp)
    -> std::string {
  return detail::format_time_or(
      tp, "-", [](std::chrono::system_clock::time_point value) {
        return std::format("{:%Y-%m-%d %H:%M}", value);
      });
}

// Formats epoch milliseconds to ISO 8601
[[nodiscard]] inline auto format_iso8601(std::int64_t millis) -> std::string {
  if (millis <= 0) {
    return {};
  }
  return format_iso8601(
      std::chrono::system_clock::time_point{std::chrono::milliseconds{millis}});
}

// Converts time_point to UTC tm
[[nodiscard]] inline auto to_utc(std::chrono::system_clock::time_point tp)
    -> std::tm {
  auto t = std::chrono::system_clock::to_time_t(tp);
  std::tm tm{};
  gmtime_r(&t, &tm);
  return tm;
}

// Converts time_point to Unix epoch milliseconds.
[[nodiscard]] inline auto
to_unix_millis(std::chrono::system_clock::time_point tp) -> std::int64_t {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             tp.time_since_epoch())
      .count();
}

} // namespace dagforge::util

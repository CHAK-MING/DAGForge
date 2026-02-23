#pragma once

#include <string>
#include <system_error>

#include "dagforge/core/error.hpp"

namespace dagforge::io {

// I/O Error Codes

enum class IoError : std::uint8_t {
  Success = 0,
  Cancelled,
  TimedOut,
  EndOfFile,
  ConnectionReset,
  ConnectionRefused,
  BrokenPipe,
  WouldBlock,
  InvalidArgument,
  BadDescriptor,
  NoBufferSpace,
  OperationInProgress,
  NotConnected,
  AlreadyConnected,
  Unknown,
};

class IoErrorCategory : public std::error_category {
public:
  [[nodiscard]] auto name() const noexcept -> const char * override {
    return "dagforge.io";
  }

  [[nodiscard]] auto message(int ev) const -> std::string override {
    switch (static_cast<IoError>(ev)) {
    case IoError::Success:
      return "success";
    case IoError::Cancelled:
      return "operation cancelled";
    case IoError::TimedOut:
      return "operation timed out";
    case IoError::EndOfFile:
      return "end of file";
    case IoError::ConnectionReset:
      return "connection reset";
    case IoError::ConnectionRefused:
      return "connection refused";
    case IoError::BrokenPipe:
      return "broken pipe";
    case IoError::WouldBlock:
      return "operation would block";
    case IoError::InvalidArgument:
      return "invalid argument";
    case IoError::BadDescriptor:
      return "bad file descriptor";
    case IoError::NoBufferSpace:
      return "no buffer space";
    case IoError::OperationInProgress:
      return "operation in progress";
    case IoError::NotConnected:
      return "not connected";
    case IoError::AlreadyConnected:
      return "already connected";
    case IoError::Unknown:
      return "unknown error";
    }
    std::unreachable();
  }

  // Bring base class overloads into scope to avoid hiding
  using std::error_category::equivalent;

  [[nodiscard]] auto equivalent(int code,
                                const std::error_condition &cond) const noexcept
      -> bool override {
    if (cond.category() == std::generic_category()) {
      switch (static_cast<IoError>(code)) {
      case IoError::Cancelled:
        return cond.value() == static_cast<int>(std::errc::operation_canceled);
      case IoError::TimedOut:
        return cond.value() == static_cast<int>(std::errc::timed_out);
      case IoError::WouldBlock:
        return cond.value() ==
               static_cast<int>(std::errc::operation_would_block);
      case IoError::InvalidArgument:
        return cond.value() == static_cast<int>(std::errc::invalid_argument);
      case IoError::BadDescriptor:
        return cond.value() == static_cast<int>(std::errc::bad_file_descriptor);
      default:
        break;
      }
    }
    return false;
  }
};

[[nodiscard]] inline auto io_error_category() noexcept
    -> const IoErrorCategory & {
  static const IoErrorCategory instance;
  return instance;
}

[[nodiscard]] inline auto make_error_code(IoError e) noexcept
    -> std::error_code {
  return {std::to_underlying(e), io_error_category()};
}

} // namespace dagforge::io

// Enable ADL for error_code
template <>
struct std::is_error_code_enum<dagforge::io::IoError> : std::true_type {};

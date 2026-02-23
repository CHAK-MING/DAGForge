#pragma once

#include <algorithm>
#include <cctype>
#include <concepts>
#include <format>
#include <functional>
#include <ostream>
#include <string>
#include <string_view>

namespace dagforge {

[[nodiscard]] inline auto has_control_chars(std::string_view value) noexcept
    -> bool {
  return std::any_of(value.begin(), value.end(),
                     [](unsigned char ch) { return std::iscntrl(ch) != 0; });
}

[[nodiscard]] inline auto is_valid_id_text(std::string_view value) noexcept
    -> bool {
  return !value.empty() && !has_control_chars(value);
}

// Phantom type tags for type-safe ID disambiguation
struct DAGTag {};
struct TaskTag {};
struct DAGTaskTag {};
struct DAGRunTag {};
struct InstanceTag {};

// Type-safe ID wrapper using phantom type pattern
// Prevents accidental mixing of different ID types at compile time
template <typename Tag> class TypedId {
public:
  explicit TypedId(std::string value) : value_(std::move(value)) {}
  explicit TypedId(std::string_view value) : value_(value) {}
  explicit TypedId(const char *value) : value_(value ? value : "") {}

  TypedId() = default;

  [[nodiscard]] auto value() const noexcept -> std::string_view {
    return value_;
  }
  [[nodiscard]] auto str() const noexcept -> const std::string & {
    return value_;
  }
  [[nodiscard]] auto c_str() const noexcept -> const char * {
    return value_.c_str();
  }

  // Explicit conversion to avoid unintended implicit string coercions
  // (e.g., pass-by-value to functions expecting std::string).
  [[nodiscard]] explicit operator const std::string &() const noexcept {
    return value_;
  }
  [[nodiscard]] explicit operator std::string_view() const noexcept {
    return value_;
  }

  [[nodiscard]] friend auto operator<=>(const TypedId &lhs,
                                        const TypedId &rhs) = default;
  [[nodiscard]] friend auto operator==(const TypedId &lhs, const TypedId &rhs)
      -> bool = default;

  [[nodiscard]] friend auto operator==(const TypedId &lhs,
                                       std::string_view rhs) noexcept -> bool {
    return lhs.value_ == rhs;
  }
  [[nodiscard]] friend auto operator==(std::string_view lhs,
                                       const TypedId &rhs) noexcept -> bool {
    return lhs == rhs.value_;
  }

  [[nodiscard]] friend auto operator<(const TypedId &lhs,
                                      std::string_view rhs) noexcept -> bool {
    return std::string_view{lhs.value_} < rhs;
  }
  [[nodiscard]] friend auto operator<(std::string_view lhs,
                                      const TypedId &rhs) noexcept -> bool {
    return lhs < std::string_view{rhs.value_};
  }
  [[nodiscard]] auto clone() const -> TypedId { return TypedId{value_}; }

  [[nodiscard]] auto empty() const noexcept -> bool { return value_.empty(); }
  [[nodiscard]] auto size() const noexcept -> std::size_t {
    return value_.size();
  }

private:
  std::string value_;
};

using DAGId = TypedId<DAGTag>;
using TaskId = TypedId<TaskTag>;
using DAGTaskId = TypedId<DAGTaskTag>;
using DAGRunId = TypedId<DAGRunTag>;
using InstanceId = TypedId<InstanceTag>;

template <typename T>
concept IsTypedId = requires(T id) {
  { id.value() } -> std::convertible_to<std::string_view>;
  { id.empty() } -> std::convertible_to<bool>;
};

template <typename Tag>
inline auto operator<<(std::ostream &os, const TypedId<Tag> &id)
    -> std::ostream & {
  return os << id.value();
}

} // namespace dagforge

// --- std::hash specializations ---
// `is_avalanching` tells ankerl::unordered_dense::hash to delegate to
// std::hash<TypedId<T>> instead of falling back to wyhash-over-raw-bytes
// (which would hash the std::string internal pointers — UB / corruption).
template <> struct std::hash<dagforge::TypedId<dagforge::DAGTag>> {
  using is_avalanching = void;
  auto operator()(const dagforge::TypedId<dagforge::DAGTag> &id) const noexcept
      -> std::size_t {
    return std::hash<std::string_view>{}(id.value());
  }
};

template <> struct std::hash<dagforge::TypedId<dagforge::TaskTag>> {
  using is_avalanching = void;
  auto operator()(const dagforge::TypedId<dagforge::TaskTag> &id) const noexcept
      -> std::size_t {
    return std::hash<std::string_view>{}(id.value());
  }
};

template <> struct std::hash<dagforge::TypedId<dagforge::DAGTaskTag>> {
  using is_avalanching = void;
  auto
  operator()(const dagforge::TypedId<dagforge::DAGTaskTag> &id) const noexcept
      -> std::size_t {
    return std::hash<std::string_view>{}(id.value());
  }
};

template <> struct std::hash<dagforge::TypedId<dagforge::DAGRunTag>> {
  using is_avalanching = void;
  auto
  operator()(const dagforge::TypedId<dagforge::DAGRunTag> &id) const noexcept
      -> std::size_t {
    return std::hash<std::string_view>{}(id.value());
  }
};

template <> struct std::hash<dagforge::TypedId<dagforge::InstanceTag>> {
  using is_avalanching = void;
  auto
  operator()(const dagforge::TypedId<dagforge::InstanceTag> &id) const noexcept
      -> std::size_t {
    return std::hash<std::string_view>{}(id.value());
  }
};

// --- std::formatter specializations (before any std::format usage) ---
template <>
struct std::formatter<dagforge::TypedId<dagforge::DAGTag>>
    : std::formatter<std::string_view> {
  auto format(const dagforge::TypedId<dagforge::DAGTag> &id, auto &ctx) const {
    return std::formatter<std::string_view>::format(id.value(), ctx);
  }
};

template <>
struct std::formatter<dagforge::TypedId<dagforge::TaskTag>>
    : std::formatter<std::string_view> {
  auto format(const dagforge::TypedId<dagforge::TaskTag> &id, auto &ctx) const {
    return std::formatter<std::string_view>::format(id.value(), ctx);
  }
};

template <>
struct std::formatter<dagforge::TypedId<dagforge::DAGTaskTag>>
    : std::formatter<std::string_view> {
  auto format(const dagforge::TypedId<dagforge::DAGTaskTag> &id,
              auto &ctx) const {
    return std::formatter<std::string_view>::format(id.value(), ctx);
  }
};

template <>
struct std::formatter<dagforge::TypedId<dagforge::DAGRunTag>>
    : std::formatter<std::string_view> {
  auto format(const dagforge::TypedId<dagforge::DAGRunTag> &id,
              auto &ctx) const {
    return std::formatter<std::string_view>::format(id.value(), ctx);
  }
};

template <>
struct std::formatter<dagforge::TypedId<dagforge::InstanceTag>>
    : std::formatter<std::string_view> {
  auto format(const dagforge::TypedId<dagforge::InstanceTag> &id,
              auto &ctx) const {
    return std::formatter<std::string_view>::format(id.value(), ctx);
  }
};

// Reopen namespace for functions that use std::format with TypedId
namespace dagforge {

namespace detail {
[[nodiscard]] auto generate_short_uuid() -> std::string;
[[nodiscard]] auto generate_uuid_v7_like() -> std::string;
} // namespace detail

inline auto generate_dag_task_id(const DAGId &dag_id, const TaskId &task_id)
    -> DAGTaskId {
  return DAGTaskId{std::format("{}_{}", dag_id, task_id)};
}

inline auto generate_dag_run_id([[maybe_unused]] const DAGId &dag_id)
    -> DAGRunId {
  return DAGRunId{detail::generate_uuid_v7_like()};
}

inline auto generate_instance_id(const DAGRunId &dag_run_id,
                                 const TaskId &task_id) -> InstanceId {
  return InstanceId{std::format("{}_{}", dag_run_id, task_id)};
}

} // namespace dagforge

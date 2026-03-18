#pragma once

#include <algorithm>
#include <cctype>
#include <concepts>
#include <format>
#include <functional>
#include <optional>
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

// --- std::hash specialization ---
// `is_avalanching` tells ankerl::unordered_dense::hash to delegate to
// std::hash<TypedId<T>> instead of falling back to wyhash-over-raw-bytes
// (which would hash the std::string internal pointers — UB / corruption).
template <typename Tag> struct std::hash<dagforge::TypedId<Tag>> {
  using is_avalanching = void;
  auto operator()(const dagforge::TypedId<Tag> &id) const noexcept
      -> std::size_t {
    return std::hash<std::string_view>{}(id.value());
  }
};

// --- std::formatter specialization (before any std::format usage) ---
template <typename Tag>
struct std::formatter<dagforge::TypedId<Tag>>
    : std::formatter<std::string_view> {
  auto format(const dagforge::TypedId<Tag> &id, auto &ctx) const {
    return std::formatter<std::string_view>::format(id.value(), ctx);
  }
};

// Reopen namespace for functions that use std::format with TypedId
namespace dagforge {

namespace detail {
[[nodiscard]] auto generate_short_uuid() -> std::string;
[[nodiscard]] auto generate_uuid_v7_like() -> std::string;
inline constexpr std::string_view kDagRunSeparator = "__";
} // namespace detail

inline auto generate_dag_task_id(const DAGId &dag_id, const TaskId &task_id)
    -> DAGTaskId {
  std::string out;
  out.reserve(dag_id.value().size() + 1 + task_id.value().size());
  out.append(dag_id.value());
  out.push_back('_');
  out.append(task_id.value());
  return DAGTaskId{std::move(out)};
}

inline auto generate_dag_run_id([[maybe_unused]] const DAGId &dag_id)
    -> DAGRunId {
  return DAGRunId{
      std::format("{}{}{}", dag_id, detail::kDagRunSeparator,
                  detail::generate_uuid_v7_like())};
}

[[nodiscard]] inline auto dag_id_from_run_id(const DAGRunId &dag_run_id)
    -> std::optional<DAGId> {
  auto value = dag_run_id.value();
  auto pos = value.find(detail::kDagRunSeparator);
  if (pos == std::string_view::npos || pos == 0) {
    return std::nullopt;
  }
  return DAGId{value.substr(0, pos)};
}

inline auto generate_instance_id(const DAGRunId &dag_run_id,
                                 const TaskId &task_id) -> InstanceId {
  std::string out;
  out.reserve(dag_run_id.value().size() + 1 + task_id.value().size());
  out.append(dag_run_id.value());
  out.push_back('_');
  out.append(task_id.value());
  return InstanceId{std::move(out)};
}

} // namespace dagforge

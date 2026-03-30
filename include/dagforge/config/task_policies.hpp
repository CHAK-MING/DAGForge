#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/util/enum.hpp"
#endif

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include <boost/describe/enum.hpp>

#include <chrono>
#include <cstdint>
#include <string_view>
#endif

namespace dagforge {

namespace task_defaults {
inline constexpr std::chrono::seconds kExecutionTimeout{3600};
inline constexpr std::chrono::seconds kRetryInterval{60};
inline constexpr int kMaxRetries{3};
} // namespace task_defaults

enum class TriggerRule : std::uint8_t {
  AllSuccess,
  AllFailed,
  AllDone,
  OneSuccess,
  OneFailed,
  NoneFailed,
  NoneSkipped,
  AllDoneMinOneSuccess,
  AllSkipped,
  OneDone,
  NoneFailedMinOneSuccess,
  Always,
};
BOOST_DESCRIBE_ENUM(TriggerRule, AllSuccess, AllFailed, AllDone, OneSuccess,
                    OneFailed, NoneFailed, NoneSkipped, AllDoneMinOneSuccess,
                    AllSkipped, OneDone, NoneFailedMinOneSuccess, Always)

[[nodiscard]] constexpr auto to_string_view(TriggerRule value) noexcept
    -> std::string_view {
  return ::dagforge::util::enum_to_snake_case_view(value);
}

template <>
[[nodiscard]] inline auto parse<TriggerRule>(std::string_view s) noexcept
    -> TriggerRule {
  return ::dagforge::util::parse_enum(s, TriggerRule::AllSuccess);
}

} // namespace dagforge

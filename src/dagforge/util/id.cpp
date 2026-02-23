#include "dagforge/util/id.hpp"

#include <chrono>
#include <format>
#include <random>

namespace dagforge::detail {

auto generate_short_uuid() -> std::string {
  thread_local std::mt19937_64 gen(std::random_device{}());
  thread_local std::uniform_int_distribution<std::uint32_t> dis;
  return std::format("{:08x}", dis(gen));
}

auto generate_uuid_v7_like() -> std::string {
  thread_local std::mt19937_64 gen(std::random_device{}());
  thread_local std::uniform_int_distribution<std::uint64_t> dis;
  const auto now_ms = static_cast<std::uint64_t>(
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::system_clock::now().time_since_epoch())
          .count());
  const auto rnd = dis(gen);
  return std::format("{:012x}{:016x}", now_ms, rnd);
}

} // namespace dagforge::detail

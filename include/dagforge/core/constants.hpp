#pragma once

#include <chrono>
#include <cstddef>

namespace dagforge {

namespace io {
constexpr std::size_t kEventBufferSize = 4096;
constexpr std::size_t kReadBufferSize = 4096;
constexpr std::size_t kInitialOutputReserve = 8192;
} // namespace io

namespace timing {
constexpr auto kConfigWatchInterval = std::chrono::milliseconds(100);
constexpr auto kShutdownPollInterval = std::chrono::milliseconds(50);
constexpr auto kDaemonPollInterval = std::chrono::milliseconds(100);
constexpr auto kRuntimeYieldInterval = std::chrono::milliseconds(1);
} // namespace timing

} // namespace dagforge

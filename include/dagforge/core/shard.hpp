#pragma once

#include "dagforge/io/context.hpp"

#include <array>
#include <memory_resource>

namespace dagforge {

using shard_id = unsigned;

class Shard {
public:
  static constexpr std::size_t kArenaSize = 64 * 1024UL; // 64KB per shard

  explicit Shard(shard_id id);
  ~Shard();

  Shard(const Shard &) = delete;
  Shard &operator=(const Shard &) = delete;

  [[nodiscard]] auto id() const noexcept -> shard_id { return id_; }
  [[nodiscard]] auto ctx() noexcept -> io::IoContext & { return ctx_; }
  [[nodiscard]] auto memory_resource() noexcept -> std::pmr::memory_resource * {
    return &pool_;
  }

private:
  shard_id id_;
  std::pmr::monotonic_buffer_resource upstream_{arena_.data(), arena_.size()};
  std::pmr::unsynchronized_pool_resource pool_{&upstream_};

  io::IoContext ctx_;
  alignas(64) std::array<std::byte, kArenaSize> arena_;
};

} // namespace dagforge

#include "dagforge/core/shard.hpp"

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory_resource>

namespace dagforge {

namespace {

[[nodiscard]] auto align_up(std::size_t value, std::size_t alignment)
    -> std::size_t {
  if (alignment <= 1) {
    return value;
  }
  const auto remainder = value % alignment;
  if (remainder == 0) {
    return value;
  }
  const auto delta = alignment - remainder;
  return value + delta;
}

} // namespace

struct Shard::TrackingArenaResource final : pmr::memory_resource {
  TrackingArenaResource(std::byte *arena, std::size_t capacity)
      : arena_(arena), capacity_(capacity) {}

  [[nodiscard]] auto used_bytes() const noexcept -> std::uint64_t {
    return used_bytes_.load(std::memory_order_relaxed);
  }

  [[nodiscard]] auto capacity_bytes() const noexcept -> std::uint64_t {
    return capacity_;
  }

  [[nodiscard]] auto allocations_total() const noexcept -> std::uint64_t {
    return allocations_total_.load(std::memory_order_relaxed);
  }

  [[nodiscard]] auto oom_fallbacks_total() const noexcept -> std::uint64_t {
    return oom_fallbacks_total_.load(std::memory_order_relaxed);
  }

private:
  auto do_allocate(std::size_t bytes, std::size_t alignment)
      -> void * override {
    allocations_total_.fetch_add(1, std::memory_order_relaxed);

    auto used = used_bytes_.load(std::memory_order_relaxed);
    const auto aligned = align_up(used, std::max<std::size_t>(alignment, 1));
    if (aligned <= capacity_ && bytes <= capacity_ - aligned) {
      const auto next_used = aligned + bytes;
      used_bytes_.store(next_used, std::memory_order_relaxed);
      return arena_ + aligned;
    }

    oom_fallbacks_total_.fetch_add(1, std::memory_order_relaxed);
    return upstream_->allocate(bytes, alignment);
  }

  void do_deallocate(void *ptr, std::size_t bytes,
                     std::size_t alignment) override {
    if (ptr == nullptr) {
      return;
    }
    const auto raw_addr = reinterpret_cast<std::uintptr_t>(ptr);
    const auto arena_addr = reinterpret_cast<std::uintptr_t>(arena_);
    if (raw_addr >= arena_addr && raw_addr < arena_addr + capacity_) {
      return;
    }
    upstream_->deallocate(ptr, bytes, alignment);
  }

  [[nodiscard]] auto
  do_is_equal(const pmr::memory_resource &other) const noexcept
      -> bool override {
    return this == &other;
  }

  std::byte *arena_;
  std::size_t capacity_;
  std::atomic<std::uint64_t> used_bytes_{0};
  std::atomic<std::uint64_t> allocations_total_{0};
  std::atomic<std::uint64_t> oom_fallbacks_total_{0};
  pmr::memory_resource *upstream_{pmr::get_default_resource()};
};

Shard::Shard(shard_id id) : id_(id) {
  tracking_resource_ =
      std::make_unique<TrackingArenaResource>(arena_.data(), arena_.size());
  pool_ = std::make_unique<pmr::unsynchronized_pool_resource>(
      tracking_resource_.get());
}

Shard::~Shard() = default;

auto Shard::memory_used_bytes() const noexcept -> std::uint64_t {
  return tracking_resource_ ? tracking_resource_->used_bytes() : 0;
}

auto Shard::memory_capacity_bytes() const noexcept -> std::uint64_t {
  return tracking_resource_ ? tracking_resource_->capacity_bytes()
                            : static_cast<std::uint64_t>(kArenaSize);
}

auto Shard::memory_allocations_total() const noexcept -> std::uint64_t {
  return tracking_resource_ ? tracking_resource_->allocations_total() : 0;
}

auto Shard::memory_oom_fallbacks_total() const noexcept -> std::uint64_t {
  return tracking_resource_ ? tracking_resource_->oom_fallbacks_total() : 0;
}

} // namespace dagforge

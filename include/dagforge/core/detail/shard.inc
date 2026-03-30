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
  [[nodiscard]] auto memory_resource() noexcept -> pmr::memory_resource * {
    return pool_ ? pool_.get() : pmr::get_default_resource();
  }
  [[nodiscard]] auto memory_used_bytes() const noexcept -> std::uint64_t;
  [[nodiscard]] auto memory_capacity_bytes() const noexcept -> std::uint64_t;
  [[nodiscard]] auto memory_allocations_total() const noexcept
      -> std::uint64_t;
  [[nodiscard]] auto memory_oom_fallbacks_total() const noexcept
      -> std::uint64_t;

private:
  shard_id id_;
  alignas(64) std::array<std::byte, kArenaSize> arena_;
  struct TrackingArenaResource;
  std::unique_ptr<TrackingArenaResource> tracking_resource_;
  std::unique_ptr<pmr::unsynchronized_pool_resource> pool_;
  io::IoContext ctx_;
};

} // namespace dagforge

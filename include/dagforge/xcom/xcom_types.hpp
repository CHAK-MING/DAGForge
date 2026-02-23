#pragma once

#include "dagforge/core/error.hpp"
#include "dagforge/util/hash.hpp"
#include "dagforge/util/id.hpp"
#include "dagforge/util/json.hpp"
#include "dagforge/util/string_hash.hpp"

#include <chrono>
#include <optional>
#include <string>
#include <unordered_map>

namespace dagforge {

struct XComEntry {
  std::string key;
  JsonValue value;
  std::size_t byte_size{0};
  std::chrono::system_clock::time_point created_at;
};

struct XComTaskEntry {
  TaskId task_id;
  std::string key;
  JsonValue value;
  std::size_t byte_size{0};
  std::chrono::system_clock::time_point created_at;
};

struct XComRef {
  TaskId task_id;
  std::string key;

  auto operator==(const XComRef &other) const -> bool = default;
};

struct XComPullConfig {
  XComRef ref;
  std::string env_var;
  bool required{false};
  std::optional<JsonValue> default_value;

  [[nodiscard]] auto source_task() const noexcept -> const TaskId & {
    return ref.task_id;
  }

  [[nodiscard]] auto key() const noexcept -> const std::string & {
    return ref.key;
  }
};

struct XComRefHash {
  auto operator()(const XComRef &ref) const -> std::size_t {
    return util::combine(ref.task_id, ref.key);
  }
};

using XComMap =
    std::unordered_map<std::string, XComEntry, StringHash, StringEqual>;

class XComCache {
public:
  struct CacheKey {
    DAGRunId run_id;
    TaskId task_id;
    std::string key;

    auto operator==(const CacheKey &other) const -> bool = default;
  };

  using CacheKeyView =
      std::tuple<const DAGRunId &, const TaskId &, const std::string &>;

  struct CacheKeyHash {
    using is_transparent = void;

    auto operator()(const CacheKey &k) const noexcept -> std::size_t {
      return util::combine(k.run_id, k.task_id, k.key);
    }
    auto operator()(const CacheKeyView &t) const noexcept -> std::size_t {
      return util::combine(std::get<0>(t), std::get<1>(t), std::get<2>(t));
    }
  };

  struct CacheKeyEqual {
    using is_transparent = void;

    auto operator()(const CacheKey &a, const CacheKey &b) const -> bool {
      return a == b;
    }
    auto operator()(const CacheKey &a, const CacheKeyView &t) const -> bool {
      return a.run_id == std::get<0>(t) && a.task_id == std::get<1>(t) &&
             a.key == std::get<2>(t);
    }
    auto operator()(const CacheKeyView &t, const CacheKey &a) const -> bool {
      return a.run_id == std::get<0>(t) && a.task_id == std::get<1>(t) &&
             a.key == std::get<2>(t);
    }
  };

  void set(const DAGRunId &run_id, const TaskId &task_id,
           const std::string &key, const JsonValue &value) {
    cache_[CacheKey{run_id.clone(), task_id.clone(), key}] = value;
  }

  [[nodiscard]] auto get(const DAGRunId &run_id, const TaskId &task_id,
                         const std::string &key) const
      -> Result<std::reference_wrapper<const JsonValue>> {
    auto it = cache_.find(CacheKeyView{run_id, task_id, key});
    if (it != cache_.end()) {
      return ok(std::cref(it->second));
    }
    return fail(Error::NotFound);
  }

  void reserve(std::size_t n) { cache_.reserve(n); }

  void clear_run(const DAGRunId &run_id) {
    std::erase_if(cache_, [&run_id](const auto &pair) {
      return pair.first.run_id == run_id;
    });
  }

  void clear() { cache_.clear(); }

private:
  std::unordered_map<CacheKey, JsonValue, CacheKeyHash, CacheKeyEqual> cache_;
};

} // namespace dagforge

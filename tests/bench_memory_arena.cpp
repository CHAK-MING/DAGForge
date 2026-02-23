// bench_memory_arena.cpp

#include "dagforge/core/arena.hpp"

#include <benchmark/benchmark.h>
#include <glaze/json.hpp>

#include <cstddef>
#include <cstdint>
#include <format>
#include <memory_resource>
#include <string>
#include <vector>

namespace dagforge {

struct NodeStd {
  std::string id;
  std::string cmd;
};

struct DocStd {
  std::vector<NodeStd> nodes;
};

struct NodeArena {
  using allocator_type = std::pmr::polymorphic_allocator<std::byte>;

  std::pmr::string id;
  std::pmr::string cmd;

  NodeArena()
      : id(std::pmr::get_default_resource()),
        cmd(std::pmr::get_default_resource()) {}

  explicit NodeArena(const allocator_type &alloc)
      : id(alloc.resource()), cmd(alloc.resource()) {}

  NodeArena(const NodeArena &other, const allocator_type &alloc)
      : id(other.id, alloc.resource()), cmd(other.cmd, alloc.resource()) {}

  NodeArena(NodeArena &&other, const allocator_type &alloc)
      : id(std::move(other.id), alloc.resource()),
        cmd(std::move(other.cmd), alloc.resource()) {}
};

struct DocArena {
  std::pmr::vector<NodeArena> nodes;
};

namespace {

[[nodiscard]] auto make_payloads(std::size_t count)
    -> std::vector<std::string> {
  std::vector<std::string> payloads;
  payloads.reserve(count);
  for (std::size_t i = 0; i < count; ++i) {
    payloads.emplace_back(std::format("payload_node_{}_for_arena", i));
  }
  return payloads;
}

[[nodiscard]] auto make_node_json(std::size_t count) -> std::string {
  std::string json;
  json.reserve(count * 96 + 32);
  json += "{\"nodes\":[";
  for (std::size_t i = 0; i < count; ++i) {
    if (i > 0) {
      json.push_back(',');
    }
    json += std::format("{{\"id\":\"task_{}\",\"cmd\":\"echo {}\"}}", i, i);
  }
  json += "]}";
  return json;
}

} // namespace
} // namespace dagforge

namespace glz {

template <> struct meta<dagforge::NodeStd> {
  using T = dagforge::NodeStd;
  static constexpr auto value = object("id", &T::id, "cmd", &T::cmd);
};

template <> struct meta<dagforge::DocStd> {
  using T = dagforge::DocStd;
  static constexpr auto value = object("nodes", &T::nodes);
};

template <> struct meta<dagforge::NodeArena> {
  using T = dagforge::NodeArena;
  static constexpr auto value = object("id", &T::id, "cmd", &T::cmd);
};

template <> struct meta<dagforge::DocArena> {
  using T = dagforge::DocArena;
  static constexpr auto value = object("nodes", &T::nodes);
};

} // namespace glz

namespace dagforge {
namespace {

void BM_Memory_PmrArena_VectorString(benchmark::State &state) {
  const auto count = static_cast<std::size_t>(state.range(0));
  const auto payloads = make_payloads(count);

  for (auto _ : state) {
    Arena<1 << 20> arena;
    auto values = arena.vector<std::pmr::string>(count);
    for (const auto &payload : payloads) {
      values.push_back(std::pmr::string(payload, arena.resource()));
    }
    benchmark::DoNotOptimize(values.size());
  }

  state.SetItemsProcessed(static_cast<int64_t>(count) * state.iterations());
}

void BM_Memory_StdMalloc_VectorString(benchmark::State &state) {
  const auto count = static_cast<std::size_t>(state.range(0));
  const auto payloads = make_payloads(count);

  for (auto _ : state) {
    std::vector<std::string> values;
    values.reserve(count);
    for (const auto &payload : payloads) {
      values.emplace_back(payload);
    }
    benchmark::DoNotOptimize(values.size());
  }

  state.SetItemsProcessed(static_cast<int64_t>(count) * state.iterations());
}

void BM_Glaze_JsonParse_ToStd(benchmark::State &state) {
  const auto count = static_cast<std::size_t>(state.range(0));
  const auto payload = make_node_json(count);
  constexpr auto kOpts = glz::opts{.null_terminated = false};

  for (auto _ : state) {
    DocStd doc;
    doc.nodes.reserve(count);
    if (auto ec = glz::read<kOpts>(doc, payload); ec) {
      state.SkipWithError("glaze std parse failed");
      return;
    }
    benchmark::DoNotOptimize(doc.nodes.size());
  }

  state.SetItemsProcessed(static_cast<int64_t>(count) * state.iterations());
}

void BM_Glaze_JsonParse_ToArena(benchmark::State &state) {
  const auto count = static_cast<std::size_t>(state.range(0));
  const auto payload = make_node_json(count);
  constexpr auto kOpts = glz::opts{.null_terminated = false};

  for (auto _ : state) {
    Arena<1 << 22> arena;
    DocArena doc;
    doc.nodes = std::pmr::vector<NodeArena>(arena.resource());
    doc.nodes.reserve(count);
    if (auto ec = glz::read<kOpts>(doc, payload); ec) {
      state.SkipWithError("glaze arena parse failed");
      return;
    }
    benchmark::DoNotOptimize(doc.nodes.size());
  }

  state.SetItemsProcessed(static_cast<int64_t>(count) * state.iterations());
}

BENCHMARK(BM_Memory_PmrArena_VectorString)
    ->Arg(1024)
    ->Arg(4096)
    ->Arg(16384)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK(BM_Memory_StdMalloc_VectorString)
    ->Arg(1024)
    ->Arg(4096)
    ->Arg(16384)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK(BM_Glaze_JsonParse_ToStd)
    ->Arg(256)
    ->Arg(1024)
    ->Arg(4096)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK(BM_Glaze_JsonParse_ToArena)
    ->Arg(256)
    ->Arg(1024)
    ->Arg(4096)
    ->Unit(benchmark::kMicrosecond);

} // namespace
} // namespace dagforge

// bench_config_parse.cpp
//
// Keep benchmark payload generation outside the hot loop and only measure
// TOML parse + hydration through DAGDefinitionLoader.

#include "dagforge/config/dag_definition.hpp"

#include <benchmark/benchmark.h>

#include <cstddef>
#include <cstdint>
#include <string>

namespace dagforge {
namespace {

[[nodiscard]] auto make_dag_toml(std::size_t task_count) -> std::string {
  std::string toml;
  toml.reserve(task_count * 104 + 160);

  toml += "id = \"bench_dag\"\n";
  toml += "name = \"bench_dag\"\n";
  toml += "catchup = false\n";
  toml += "max_concurrent_runs = 4\n\n";

  for (std::size_t i = 0; i < task_count; ++i) {
    toml += "[[tasks]]\n";
    toml += "id = \"t_" + std::to_string(i) + "\"\n";
    toml += "command = \"echo " + std::to_string(i) + "\"\n";
    toml += "executor = \"shell\"\n";
    if (i > 0) {
      toml += "dependencies = [\"t_" + std::to_string(i - 1) + "\"]\n";
    }
    toml += "\n";
  }

  return toml;
}

void BM_ConfigParseDagToml(benchmark::State &state) {
  const auto task_count = static_cast<std::size_t>(state.range(0));
  const auto toml = make_dag_toml(task_count);

  for (auto _ : state) {
    auto parsed = DAGDefinitionLoader::load_from_string(toml);
    if (!parsed) {
      state.SkipWithError(parsed.error().message().c_str());
      return;
    }
    benchmark::DoNotOptimize(parsed->dag_id);
    benchmark::DoNotOptimize(parsed->tasks.size());
  }

  state.SetItemsProcessed(static_cast<int64_t>(task_count) *
                          state.iterations());
}

BENCHMARK(BM_ConfigParseDagToml)
    ->Arg(100)
    ->Arg(1000)
    ->Arg(5000)
    ->Unit(benchmark::kMicrosecond);

} // namespace
} // namespace dagforge

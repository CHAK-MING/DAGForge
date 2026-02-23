// bench_dag_engine.cpp

#include "dagforge/dag/dag.hpp"
#include "dagforge/dag/dag_run.hpp"

#include "test_utils.hpp"

#include <benchmark/benchmark.h>

#include <cstdint>
#include <format>
#include <memory>
#include <vector>

namespace dagforge {
namespace {

class TaskIdPool {
public:
  explicit TaskIdPool(std::size_t count) {
    ids_.reserve(count);
    for (std::size_t i = 0; i < count; ++i) {
      ids_.emplace_back(std::format("t_{}", i));
    }
  }

  [[nodiscard]] auto at(std::size_t i) const -> const TaskId & {
    return ids_[i];
  }

private:
  std::vector<TaskId> ids_;
};

class InstanceIdPool {
public:
  explicit InstanceIdPool(std::size_t count) {
    ids_.reserve(count);
    for (std::size_t i = 0; i < count; ++i) {
      ids_.emplace_back(std::format("inst_{}", i));
    }
  }

  [[nodiscard]] auto at(std::size_t i) const -> const InstanceId & {
    return ids_[i];
  }

private:
  std::vector<InstanceId> ids_;
};

[[nodiscard]] auto make_linear_dag(const TaskIdPool &ids, std::size_t nodes)
    -> DAG {
  DAG dag;
  for (std::size_t i = 0; i < nodes; ++i) {
    if (!dag.add_node(ids.at(i).clone())) {
      return dag;
    }
    if (i > 0) {
      if (!dag.add_edge(static_cast<NodeIndex>(i - 1),
                        static_cast<NodeIndex>(i))) {
        return dag;
      }
    }
  }
  return dag;
}

[[nodiscard]] auto make_flat_dag(const TaskIdPool &ids, std::size_t nodes)
    -> DAG {
  DAG dag;
  for (std::size_t i = 0; i < nodes; ++i) {
    (void)dag.add_node(ids.at(i).clone());
  }
  return dag;
}

[[nodiscard]] auto make_layered_dag(const TaskIdPool &ids, std::size_t width,
                                    std::size_t depth) -> DAG {
  DAG dag;
  const auto total = width * depth;
  for (std::size_t i = 0; i < total; ++i) {
    (void)dag.add_node(ids.at(i).clone());
  }

  for (std::size_t layer = 1; layer < depth; ++layer) {
    const auto prev = (layer - 1) * width;
    const auto curr = layer * width;
    for (std::size_t i = 0; i < width; ++i) {
      (void)dag.add_edge(static_cast<NodeIndex>(prev + i),
                         static_cast<NodeIndex>(curr + i));
    }
  }
  return dag;
}

[[nodiscard]] auto make_fanin_dag(const TaskIdPool &ids, std::size_t deps)
    -> DAG {
  DAG dag;
  for (std::size_t i = 0; i < deps; ++i) {
    (void)dag.add_node(ids.at(i).clone());
  }
  auto sink = dag.add_node(TaskId{"sink"});
  if (!sink) {
    return dag;
  }
  for (std::size_t i = 0; i < deps; ++i) {
    (void)dag.add_edge(static_cast<NodeIndex>(i), *sink);
  }
  return dag;
}

[[nodiscard]] auto make_diamond_dag(const TaskIdPool &ids, std::size_t width)
    -> DAG {
  DAG dag;
  (void)dag.add_node(TaskId{"root"});
  for (std::size_t i = 0; i < width; ++i) {
    (void)dag.add_node(ids.at(i).clone());
    (void)dag.add_edge(0, static_cast<NodeIndex>(i + 1));
  }
  auto sink = dag.add_node(TaskId{"sink"});
  if (sink) {
    for (std::size_t i = 0; i < width; ++i) {
      (void)dag.add_edge(static_cast<NodeIndex>(i + 1), *sink);
    }
  }
  return dag;
}

[[nodiscard]] auto make_run(DAG dag, const char *run_name) -> Result<DAGRun> {
  auto shared = std::make_shared<DAG>(std::move(dag));
  return DAGRun::create(test::dag_run_id(run_name), std::move(shared));
}

// ===================================================================
// BM_DagBuildLinear
// Measures: sequential add_node + add_edge for a chain of N nodes.
// ===================================================================
void BM_DagBuildLinear(benchmark::State &state) {
  const auto nodes = static_cast<std::size_t>(state.range(0));
  TaskIdPool ids(nodes);

  for (auto _ : state) {
    auto dag = make_linear_dag(ids, nodes);
    benchmark::DoNotOptimize(dag.size());
  }
  state.SetItemsProcessed(static_cast<int64_t>(nodes) * state.iterations());
}

// ===================================================================
// BM_DagBuildFanIn
// Measures: N source nodes all fanning into a single sink.
// ===================================================================
void BM_DagBuildFanIn(benchmark::State &state) {
  const auto deps = static_cast<std::size_t>(state.range(0));
  TaskIdPool ids(deps);

  for (auto _ : state) {
    auto dag = make_fanin_dag(ids, deps);
    benchmark::DoNotOptimize(dag.size());
  }
  state.SetItemsProcessed(static_cast<int64_t>(deps) * state.iterations());
}

// ===================================================================
// BM_DagBuildDiamond
// Measures: root → N middle nodes → sink diamond topology.
// ===================================================================
void BM_DagBuildDiamond(benchmark::State &state) {
  const auto width = static_cast<std::size_t>(state.range(0));
  TaskIdPool ids(width);

  for (auto _ : state) {
    auto dag = make_diamond_dag(ids, width);
    benchmark::DoNotOptimize(dag.size());
  }
  state.SetItemsProcessed(static_cast<int64_t>(width + 2) * state.iterations());
}

// ===================================================================
// BM_DagTopologicalSort
// Measures: topological ordering on a pre-built linear DAG.
// ===================================================================
void BM_DagTopologicalSort(benchmark::State &state) {
  const auto nodes = static_cast<std::size_t>(state.range(0));
  TaskIdPool ids(nodes);
  auto dag = make_linear_dag(ids, nodes);

  for (auto _ : state) {
    auto order = dag.get_topological_order();
    benchmark::DoNotOptimize(order);
  }
  state.SetItemsProcessed(static_cast<int64_t>(nodes) * state.iterations());
}

// ===================================================================
// BM_DagTopologicalSortFanIn
// Measures: topo sort for fan-in topology (N deps + 1 sink).
// ===================================================================
void BM_DagTopologicalSortFanIn(benchmark::State &state) {
  const auto deps = static_cast<std::size_t>(state.range(0));
  TaskIdPool ids(deps);
  auto dag = make_fanin_dag(ids, deps);

  for (auto _ : state) {
    auto order = dag.get_topological_order();
    benchmark::DoNotOptimize(order);
  }
  state.SetItemsProcessed(static_cast<int64_t>(deps + 1) * state.iterations());
}

// ===================================================================
// BM_DagTriggerTaxFanIn
// Measures: trigger-rule evaluation cost when N predecessors complete
// and a single fan-in sink waits on AllSuccess.
// Setup (DAG + DAGRun creation) is excluded via PauseTiming.
// ===================================================================
void BM_DagTriggerTaxFanIn(benchmark::State &state) {
  const auto deps = static_cast<std::size_t>(state.range(0));
  TaskIdPool task_ids(deps);
  InstanceIdPool inst_ids(deps);

  for (auto _ : state) {
    state.PauseTiming();
    auto run_result = make_run(make_fanin_dag(task_ids, deps), "fanin_run");
    if (!run_result) {
      state.SkipWithError("failed to create DAGRun");
      return;
    }
    auto &run = *run_result;
    state.ResumeTiming();

    for (std::size_t i = 0; i < deps; ++i) {
      const auto idx = static_cast<NodeIndex>(i);
      (void)run.mark_task_started(idx, inst_ids.at(i).clone());
      (void)run.mark_task_completed(idx, 0);
    }
    benchmark::DoNotOptimize(run.ready_count());
  }
  state.SetItemsProcessed(static_cast<int64_t>(deps) * state.iterations());
}

// ===================================================================
// BM_DagTriggerTaxLinear
// Measures: sequential start → complete for a linear chain.
// ===================================================================
void BM_DagTriggerTaxLinear(benchmark::State &state) {
  const auto depth = static_cast<std::size_t>(state.range(0));
  TaskIdPool task_ids(depth);
  InstanceIdPool inst_ids(depth);

  for (auto _ : state) {
    state.PauseTiming();
    auto run_result = make_run(make_linear_dag(task_ids, depth), "linear_run");
    if (!run_result) {
      state.SkipWithError("failed to create DAGRun");
      return;
    }
    auto &run = *run_result;
    state.ResumeTiming();

    for (std::size_t i = 0; i < depth; ++i) {
      const auto idx = static_cast<NodeIndex>(i);
      (void)run.mark_task_started(idx, inst_ids.at(i).clone());
      (void)run.mark_task_completed(idx, 0);
    }
    benchmark::DoNotOptimize(run.state());
  }
  state.SetItemsProcessed(static_cast<int64_t>(depth) * state.iterations());
}

// ===================================================================
// BM_DagValidation
// Measures: cycle-detection pass on a linear DAG (should be O(V+E)).
// ===================================================================
void BM_DagValidation(benchmark::State &state) {
  const auto nodes = static_cast<std::size_t>(state.range(0));
  TaskIdPool ids(nodes);
  auto dag = make_linear_dag(ids, nodes);

  for (auto _ : state) {
    auto valid = dag.is_valid();
    benchmark::DoNotOptimize(valid);
  }
  state.SetItemsProcessed(static_cast<int64_t>(nodes) * state.iterations());
}

// ===================================================================
// BM_DagReadyTasks
// Measures: get_ready_tasks query after root completes in a diamond.
// All middle nodes become ready simultaneously.
// ===================================================================
void BM_DagReadyTasks(benchmark::State &state) {
  const auto width = static_cast<std::size_t>(state.range(0));
  TaskIdPool task_ids(width);
  auto run_result = make_run(make_diamond_dag(task_ids, width), "ready_run");
  if (!run_result) {
    state.SkipWithError("failed to create DAGRun");
    return;
  }
  auto &run = *run_result;
  (void)run.mark_task_started(0, InstanceId{"root_inst"});
  (void)run.mark_task_completed(0, 0);
  std::pmr::vector<NodeIndex> ready;
  ready.reserve(width);

  for (auto _ : state) {
    run.copy_ready_tasks(ready);
    benchmark::DoNotOptimize(ready);
  }
  state.counters["ready_tasks_per_call"] = benchmark::Counter(
      static_cast<double>(width), benchmark::Counter::kAvgThreads);
  state.SetItemsProcessed(state.iterations());
  state.SetBytesProcessed(static_cast<int64_t>(width * sizeof(NodeIndex)) *
                          state.iterations());
}

// ===================================================================
// BM_EndToEnd_Scheduling_Throughput
// Measures: full scheduling cycle on flat or layered DAGs.
//   mode 0 → flat (all tasks independent)
//   mode 1 → layered (width × depth grid)
// ===================================================================
void BM_EndToEnd_Scheduling_Throughput(benchmark::State &state) {
  const auto mode = state.range(0);
  const auto a = static_cast<std::size_t>(state.range(1));
  const auto b = static_cast<std::size_t>(state.range(2));
  const auto nodes = (mode == 0) ? a : (a * b);

  TaskIdPool task_ids(nodes);
  InstanceIdPool inst_ids(nodes);

  for (auto _ : state) {
    state.PauseTiming();
    auto run_result =
        (mode == 0)
            ? make_run(make_flat_dag(task_ids, a), "e2e_sched_run")
            : make_run(make_layered_dag(task_ids, a, b), "e2e_sched_run");
    if (!run_result) {
      state.SkipWithError("failed to create DAGRun");
      return;
    }
    auto &run = *run_result;
    std::size_t completed = 0;
    std::pmr::vector<NodeIndex> ready;
    ready.reserve(nodes);
    state.ResumeTiming();

    while (completed < nodes) {
      run.copy_ready_tasks(ready);
      if (ready.empty()) {
        state.SkipWithError("scheduler stalled with pending tasks");
        return;
      }
      for (NodeIndex idx : ready) {
        (void)run.mark_task_started(
            idx, inst_ids.at(static_cast<std::size_t>(idx)).clone());
      }
      for (NodeIndex idx : ready) {
        (void)run.mark_task_completed(idx, 0);
      }
      completed += ready.size();
    }

    benchmark::DoNotOptimize(run.state());
  }

  state.SetItemsProcessed(static_cast<int64_t>(nodes) * state.iterations());
}

BENCHMARK(BM_DagBuildLinear)
    ->RangeMultiplier(10)
    ->Range(1000, 100000)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK(BM_DagBuildFanIn)
    ->Arg(64)
    ->Arg(256)
    ->Arg(1024)
    ->Arg(4096)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK(BM_DagBuildDiamond)
    ->Arg(64)
    ->Arg(256)
    ->Arg(1024)
    ->Arg(4096)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK(BM_DagTopologicalSort)
    ->RangeMultiplier(10)
    ->Range(1000, 100000)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK(BM_DagTopologicalSortFanIn)
    ->Arg(64)
    ->Arg(256)
    ->Arg(1024)
    ->Arg(4096)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK(BM_DagTriggerTaxFanIn)
    ->Arg(64)
    ->Arg(256)
    ->Arg(1024)
    ->Arg(4096)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK(BM_DagTriggerTaxLinear)
    ->Arg(100)
    ->Arg(500)
    ->Arg(1000)
    ->Arg(5000)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK(BM_DagValidation)
    ->RangeMultiplier(10)
    ->Range(1000, 100000)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK(BM_DagReadyTasks)
    ->Arg(64)
    ->Arg(256)
    ->Arg(1024)
    ->Arg(4096)
    ->Unit(benchmark::kNanosecond);

BENCHMARK(BM_EndToEnd_Scheduling_Throughput)
    ->Args({0, 10000, 1})
    ->Args({1, 100, 100})
    ->Unit(benchmark::kMillisecond);

} // namespace
} // namespace dagforge

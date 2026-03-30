#include "dagforge/app/metrics_registry.hpp"

#include "gtest/gtest.h"

#include <array>
#include <cstdint>
#include <string_view>
#include <thread>
#include <tuple>

using namespace dagforge;
using namespace dagforge::http;

namespace {

constexpr std::array<std::uint64_t, 3> kBucketsNs{10, 100, 1000};

auto find_http_count(
    const std::vector<
        std::tuple<std::string, std::string, std::string, std::uint64_t>>
        &counts,
    std::string_view method, std::string_view endpoint, std::string_view status)
    -> std::uint64_t {
  for (const auto &[actual_method, actual_endpoint, actual_status, count] :
       counts) {
    if (actual_method == method && actual_endpoint == endpoint &&
        actual_status == status) {
      return count;
    }
  }
  return 0;
}

auto find_http_duration_snapshot(
    const std::vector<std::pair<std::string, metrics::Histogram::Snapshot>>
        &snapshots,
    std::string_view endpoint) -> const metrics::Histogram::Snapshot * {
  for (const auto &[actual_endpoint, snapshot] : snapshots) {
    if (actual_endpoint == endpoint) {
      return &snapshot;
    }
  }
  return nullptr;
}

auto find_dag_snapshot(
    const std::vector<detail::DagRunMetricsSnapshot> &snapshots,
    std::string_view dag_id) -> const detail::DagRunMetricsSnapshot * {
  for (const auto &snapshot : snapshots) {
    if (snapshot.dag_id.value() == dag_id) {
      return &snapshot;
    }
  }
  return nullptr;
}

} // namespace

TEST(MetricsRegistryTest, HttpRegistryRecordsRouteCountsAndDurations) {
  detail::HttpMetricsRegistry registry;
  const auto health = registry.register_route(HttpMethod::GET, "/api/health",
                                              kBucketsNs);
  const auto metrics =
      registry.register_route(HttpMethod::GET, "/metrics", kBucketsNs);

  health.record(HttpStatus::Ok, 40);
  health.record(HttpStatus::ServiceUnavailable, 400);
  metrics.record(HttpStatus::Ok, 4);

  const auto counts = registry.request_counts();
  EXPECT_EQ(find_http_count(counts, "GET", "/api/health", "200"), 1U);
  EXPECT_EQ(find_http_count(counts, "GET", "/api/health", "503"), 1U);
  EXPECT_EQ(find_http_count(counts, "GET", "/metrics", "200"), 1U);

  const auto durations = registry.request_duration_snapshots();
  const auto *health_snapshot =
      find_http_duration_snapshot(durations, "/api/health");
  ASSERT_NE(health_snapshot, nullptr);
  EXPECT_EQ(health_snapshot->count, 2U);
  EXPECT_EQ(health_snapshot->sum_ns, 440U);

  const auto *metrics_snapshot =
      find_http_duration_snapshot(durations, "/metrics");
  ASSERT_NE(metrics_snapshot, nullptr);
  EXPECT_EQ(metrics_snapshot->count, 1U);
  EXPECT_EQ(metrics_snapshot->sum_ns, 4U);
}

TEST(MetricsRegistryTest, HttpRegistrySharesEndpointHistogramAcrossMethods) {
  detail::HttpMetricsRegistry registry;
  const auto get_route =
      registry.register_route(HttpMethod::GET, "/api/dags", kBucketsNs);
  const auto post_route =
      registry.register_route(HttpMethod::POST, "/api/dags", kBucketsNs);

  get_route.record(HttpStatus::Ok, 25);
  post_route.record(HttpStatus::Created, 250);

  const auto durations = registry.request_duration_snapshots();
  const auto *snapshot = find_http_duration_snapshot(durations, "/api/dags");
  ASSERT_NE(snapshot, nullptr);
  EXPECT_EQ(snapshot->count, 2U);
  EXPECT_EQ(snapshot->sum_ns, 275U);

  const auto counts = registry.request_counts();
  EXPECT_EQ(find_http_count(counts, "GET", "/api/dags", "200"), 1U);
  EXPECT_EQ(find_http_count(counts, "POST", "/api/dags", "201"), 1U);
}

TEST(MetricsRegistryTest, DagRunRegistryAggregatesPerDagAndTerminalState) {
  detail::DagRunMetricsRegistry registry;

  registry.record(DAGId{"dag.alpha"}, 0, 15);
  registry.record(DAGId{"dag.alpha"}, 1, 150);
  registry.record(DAGId{"dag.beta"}, 2, 1500);

  const auto snapshots = registry.snapshot();
  ASSERT_EQ(snapshots.size(), 2U);

  const auto *alpha = find_dag_snapshot(snapshots, "dag.alpha");
  ASSERT_NE(alpha, nullptr);
  EXPECT_EQ(alpha->terminal_counts[0], 1U);
  EXPECT_EQ(alpha->terminal_counts[1], 1U);
  EXPECT_EQ(alpha->terminal_duration_histograms[0].count, 1U);
  EXPECT_EQ(alpha->terminal_duration_histograms[0].sum_ns, 15U);
  EXPECT_EQ(alpha->terminal_duration_histograms[1].count, 1U);
  EXPECT_EQ(alpha->terminal_duration_histograms[1].sum_ns, 150U);

  const auto *beta = find_dag_snapshot(snapshots, "dag.beta");
  ASSERT_NE(beta, nullptr);
  EXPECT_EQ(beta->terminal_counts[2], 1U);
  EXPECT_EQ(beta->terminal_duration_histograms[2].count, 1U);
  EXPECT_EQ(beta->terminal_duration_histograms[2].sum_ns, 1500U);
}

TEST(MetricsRegistryTest, DagRunRegistryHandlesConcurrentFirstInsert) {
  detail::DagRunMetricsRegistry registry;

  constexpr int kThreads = 8;
  constexpr int kPerThreadRecords = 200;
  {
    std::array<std::jthread, kThreads> threads;
    for (int i = 0; i < kThreads; ++i) {
      threads[static_cast<std::size_t>(i)] = std::jthread([&registry] {
        for (int n = 0; n < kPerThreadRecords; ++n) {
          registry.record(DAGId{"dag.concurrent"}, 0, 10);
        }
      });
    }
  }

  const auto snapshots = registry.snapshot();
  ASSERT_EQ(snapshots.size(), 1U);
  const auto *snapshot = find_dag_snapshot(snapshots, "dag.concurrent");
  ASSERT_NE(snapshot, nullptr);
  EXPECT_EQ(snapshot->terminal_counts[0],
            static_cast<std::uint64_t>(kThreads * kPerThreadRecords));
  EXPECT_EQ(snapshot->terminal_duration_histograms[0].count,
            static_cast<std::uint64_t>(kThreads * kPerThreadRecords));
  EXPECT_EQ(snapshot->terminal_duration_histograms[0].sum_ns,
            static_cast<std::uint64_t>(kThreads * kPerThreadRecords * 10));
}

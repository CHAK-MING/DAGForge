#pragma once

#include "dagforge/app/services/persistence_service.hpp"
#include "dagforge/config/system_config.hpp"
#include "dagforge/util/json.hpp"

#include "test_utils.hpp"
#include "gtest/gtest.h"

#include <chrono>
#include <cstdint>
#include <format>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>

namespace dagforge::test {

inline auto make_mysql_test_config(std::uint16_t api_port) -> SystemConfig {
  return make_test_config(api_port);
}

class MySqlIsolatedTest : public ::testing::Test {
protected:
  void SetUp() override {
    dagforge::test::reset_mysql_test_database(make_mysql_test_config(0).database);
  }

  void TearDown() override {
    dagforge::test::reset_mysql_test_database(make_mysql_test_config(0).database);
  }
};

inline auto wait_api_ready(std::uint16_t port) -> bool {
  return poll_until(
      [&]() {
        const auto [status, body] = http_get(port, "/api/health");
        if (status != 200) {
          return false;
        }
        auto parsed = parse_json(body);
        return parsed.has_value() &&
               (*parsed)["status"].as<std::string>() == "healthy";
      },
      std::chrono::seconds(10), std::chrono::milliseconds(100));
}

struct TaskSnapshot {
  std::string state;
  int attempt{0};
};

inline auto trigger_dag(std::uint16_t port, std::string_view dag_id)
    -> std::string {
  const auto [status, body] =
      http_post(port, std::format("/api/dags/{}/trigger", dag_id), "{}");
  EXPECT_EQ(status, 201) << body;
  auto parsed = parse_json(body);
  EXPECT_TRUE(parsed.has_value()) << body;
  EXPECT_TRUE(parsed->contains("dag_run_id")) << body;
  return (*parsed)["dag_run_id"].as<std::string>();
}

inline auto fetch_run_state(std::uint16_t port, std::string_view run_id)
    -> std::optional<std::string> {
  const auto [status, body] =
      http_get(port, std::format("/api/history/{}", run_id));
  if (status != 200) {
    return std::nullopt;
  }
  auto parsed = parse_json(body);
  if (!parsed || !parsed->contains("state")) {
    return std::nullopt;
  }
  return (*parsed)["state"].as<std::string>();
}

inline auto fetch_run_tasks(std::uint16_t port, std::string_view run_id)
    -> std::unordered_map<std::string, TaskSnapshot> {
  std::unordered_map<std::string, TaskSnapshot> out;
  const auto [status, body] =
      http_get(port, std::format("/api/runs/{}/tasks", run_id));
  if (status != 200) {
    return out;
  }
  auto parsed = parse_json(body);
  if (!parsed || !parsed->contains("tasks")) {
    return out;
  }
  const auto *arr = (*parsed)["tasks"].get_if<JsonValue::array_t>();
  if (arr == nullptr) {
    return out;
  }

  for (const auto &entry : *arr) {
    const auto *task_id = entry["task_id"].get_if<std::string>();
    const auto *state = entry["state"].get_if<std::string>();
    if (task_id == nullptr || state == nullptr) {
      continue;
    }
    TaskSnapshot snapshot{.state = *state};
    if (const auto *attempt = entry["attempt"].get_if<int64_t>()) {
      snapshot.attempt = static_cast<int>(*attempt);
    } else if (const auto *attempt = entry["attempt"].get_if<double>()) {
      snapshot.attempt = static_cast<int>(*attempt);
    }
    out.emplace(*task_id, std::move(snapshot));
  }
  return out;
}

} // namespace dagforge::test

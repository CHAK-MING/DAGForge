#include "dagforge/config/config.hpp"
#include "dagforge/config/task_config.hpp"

#include "gtest/gtest.h"

using namespace dagforge;

TEST(ConfigTest, DatabaseDefaults) {
  DatabaseConfig db;
  EXPECT_EQ(db.host, "127.0.0.1");
  EXPECT_EQ(db.port, 3306);
  EXPECT_EQ(db.username, "dagforge");
  EXPECT_EQ(db.database, "dagforge");
}

TEST(ConfigTest, SchedulerDefaults) {
  SchedulerConfig cfg;
  EXPECT_EQ(cfg.log_level, "info");
  EXPECT_EQ(cfg.tick_interval_ms, 1000);
  EXPECT_EQ(cfg.max_concurrency, 10);
}

TEST(ConfigTest, ApiDefaults) {
  ApiConfig cfg;
  EXPECT_FALSE(cfg.enabled);
  EXPECT_EQ(cfg.host, "127.0.0.1");
  EXPECT_EQ(cfg.port, 8080);
}

TEST(ConfigTest, LoadFromTomlString) {
  std::string toml = R"(
[database]
host = "127.0.0.1"
port = 3306
username = "dagforge"
password = "dagforge"
database = "dagforge_test"
pool_size = 4
connect_timeout = 5

[scheduler]
log_level = "debug"
max_concurrency = 8

[api]
enabled = true
port = 9999
host = "0.0.0.0"

[dag_source]
mode = "file"
directory = "./dags"
scan_interval_sec = 30
)";

  auto result = ConfigLoader::load_from_string(toml);
  ASSERT_TRUE(result.has_value()) << result.error().message();

  EXPECT_EQ(result->database.database, "dagforge_test");
  EXPECT_EQ(result->scheduler.log_level, "debug");
  EXPECT_TRUE(result->api.enabled);
  EXPECT_EQ(result->api.port, 9999);
}

TEST(ConfigTest, TaskConfigDefaults) {
  TaskConfig task;
  EXPECT_TRUE(task.task_id.empty());
  EXPECT_EQ(task.executor, ExecutorType::Shell);
  EXPECT_EQ(task.max_retries, 3);
}

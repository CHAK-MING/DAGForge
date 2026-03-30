#include "dagforge/config/task_config.hpp"
#include "dagforge/core/runtime.hpp"
#include "dagforge/executor/executor.hpp"

#include <atomic>
#include "gtest/gtest.h"

#include <filesystem>
#include <fstream>
#include <thread>
#include <unistd.h>
#include <vector>

using namespace dagforge;
using namespace std::chrono_literals;

TEST(SensorExecutorTest, BuildFileSensorTaskConfig) {
  SensorExecutorConfig sensor;
  sensor.type = SensorType::File;
  sensor.target = "/tmp/target.txt";
  sensor.poke_interval = 1s;

  auto task = TaskConfig::builder()
                  .id("file_sensor")
                  .name("file_sensor")
                  .executor(ExecutorType::Sensor)
                  .config(sensor)
                  .timeout(5s)
                  .retry(0, 1s)
                  .build();

  ASSERT_TRUE(task.has_value());
  EXPECT_EQ(task->executor, ExecutorType::Sensor);
}

TEST(SensorExecutorTest, BuildCommandSensorTaskConfig) {
  SensorExecutorConfig sensor;
  sensor.type = SensorType::Command;
  sensor.target = "echo ok";

  auto task = TaskConfig::builder()
                  .id("cmd_sensor")
                  .name("cmd_sensor")
                  .executor(ExecutorType::Sensor)
                  .config(sensor)
                  .timeout(3s)
                  .retry(1, 1s)
                  .build();

  ASSERT_TRUE(task.has_value());
  EXPECT_EQ(task->max_retries, 1);
}

class SensorExecutorRuntimeTest : public ::testing::Test {
protected:
  void SetUp() override {
    runtime_ = std::make_unique<Runtime>(1);
    ASSERT_TRUE(runtime_->start().has_value());
    executor_ = create_sensor_executor(*runtime_);
  }

  void TearDown() override {
    executor_.reset();
    if (runtime_) {
      runtime_->stop();
      runtime_.reset();
    }
  }

  std::unique_ptr<Runtime> runtime_;
  std::unique_ptr<IExecutor> executor_;
};

TEST_F(SensorExecutorRuntimeTest, CommandSensorEventuallySucceedsWhenProbeTurnsTrue) {
  const auto path = std::filesystem::temp_directory_path() /
                    ("dagforge_sensor_" + std::to_string(::getpid()) + ".flag");
  std::error_code ec;
  std::filesystem::remove(path, ec);

  SensorExecutorConfig config;
  config.type = SensorType::Command;
  config.target = std::string("[ -f \"") + path.string() + "\" ]";
  config.poke_interval = 1s;

  ExecutorResult result;
  std::atomic<bool> completed{false};

  ExecutorRequest req;
  req.instance_id = InstanceId{"cmd_sensor_probe"};
  req.execution_timeout = 3s;
  req.config = config;

  ExecutionSink sink;
  sink.on_complete = [&](const InstanceId &, ExecutorResult r) {
    result = std::move(r);
    completed = true;
  };

  ASSERT_TRUE(executor_->start(req, std::move(sink)).has_value());

  std::thread creator([path]() {
    std::this_thread::sleep_for(150ms);
    std::ofstream out(path);
    out << "ok\n";
  });

  const auto deadline = std::chrono::steady_clock::now() + 5s;
  while (!completed && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(10ms);
  }
  creator.join();
  std::filesystem::remove(path, ec);

  ASSERT_TRUE(completed);
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_FALSE(result.timed_out);
}

TEST_F(SensorExecutorRuntimeTest, CommandSensorCancelStopsActiveCommand) {
  SensorExecutorConfig config;
  config.type = SensorType::Command;
  config.target = "sleep 10";
  config.poke_interval = 1s;

  ExecutorResult result;
  std::atomic<bool> completed{false};

  const InstanceId instance_id{"cmd_sensor_cancel"};
  ExecutorRequest req;
  req.instance_id = instance_id;
  req.execution_timeout = 30s;
  req.config = config;

  ExecutionSink sink;
  sink.on_complete = [&](const InstanceId &, ExecutorResult r) {
    result = std::move(r);
    completed = true;
  };

  ASSERT_TRUE(executor_->start(req, std::move(sink)).has_value());
  std::this_thread::sleep_for(100ms);
  executor_->cancel(instance_id);

  const auto deadline = std::chrono::steady_clock::now() + 5s;
  while (!completed && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(10ms);
  }

  ASSERT_TRUE(completed);
  EXPECT_NE(result.error.find("Sensor cancelled"), std::string::npos);
}

TEST_F(SensorExecutorRuntimeTest, CommandSensorTimeoutReturnsTimedOutResult) {
  SensorExecutorConfig config;
  config.type = SensorType::Command;
  config.target = "sleep 5";
  config.poke_interval = 1s;

  ExecutorResult result;
  std::atomic<bool> completed{false};

  ExecutorRequest req;
  req.instance_id = InstanceId{"cmd_sensor_timeout"};
  req.execution_timeout = 1s;
  req.config = config;

  ExecutionSink sink;
  sink.on_complete = [&](const InstanceId &, ExecutorResult r) {
    result = std::move(r);
    completed = true;
  };

  ASSERT_TRUE(executor_->start(req, std::move(sink)).has_value());

  const auto deadline = std::chrono::steady_clock::now() + 5s;
  while (!completed && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(10ms);
  }

  ASSERT_TRUE(completed);
  EXPECT_EQ(result.exit_code, 1);
  EXPECT_TRUE(result.timed_out);
  EXPECT_NE(result.error.find("Command sensor execution_timeout"),
            std::string::npos);
}

TEST_F(SensorExecutorRuntimeTest, ConsolidatesWaitingSensorsIntoFewIoTimers) {
  constexpr int kSensorCount = 24;

  const auto missing_path =
      std::filesystem::temp_directory_path() /
      ("dagforge_sensor_missing_" + std::to_string(::getpid()) + ".flag");
  std::error_code ec;
  std::filesystem::remove(missing_path, ec);

  SensorExecutorConfig config;
  config.type = SensorType::File;
  config.target = missing_path.string();
  config.poke_interval = 2s;

  std::atomic<int> state_hits{0};
  std::vector<InstanceId> instance_ids;
  instance_ids.reserve(kSensorCount);

  for (int i = 0; i < kSensorCount; ++i) {
    const InstanceId instance_id{
        "file_sensor_wait_" + std::to_string(i)};
    instance_ids.push_back(instance_id);

    ExecutorRequest req;
    req.instance_id = instance_id;
    req.execution_timeout = 4s;
    req.config = config;

    ExecutionSink sink;
    sink.on_state = [&](const InstanceId &, std::string_view) {
      state_hits.fetch_add(1, std::memory_order_acq_rel);
    };

    ASSERT_TRUE(executor_->start(req, std::move(sink)).has_value());
  }

  const auto deadline = std::chrono::steady_clock::now() + 2s;
  while (state_hits.load(std::memory_order_acquire) < kSensorCount &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(10ms);
  }
  ASSERT_GE(state_hits.load(std::memory_order_acquire), kSensorCount);

  std::this_thread::sleep_for(50ms);
  EXPECT_LE(runtime_->io_context_timer_depth(0), 4U);
}

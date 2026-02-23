#include "dagforge/config/task_config.hpp"

#include "gtest/gtest.h"

using namespace dagforge;
using namespace std::chrono_literals;

TEST(SensorExecutorTest, BuildFileSensorTaskConfig) {
  SensorTaskConfig sensor;
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
  SensorTaskConfig sensor;
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

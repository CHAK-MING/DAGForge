#include "dagforge/app/config_builder.hpp"
#include "dagforge/core/runtime.hpp"
#include "dagforge/dag/dag_manager.hpp"
#include "dagforge/executor/composite_executor.hpp"
#include "dagforge/executor/executor.hpp"

#include <chrono>
#include <string_view>
#include <thread>

#include "gtest/gtest.h"

using namespace dagforge;

TEST(ExecutorTypeRegistryTest, Shell_StringToType) {
  EXPECT_EQ(parse<ExecutorType>("shell"), ExecutorType::Shell);
}

TEST(ExecutorTypeRegistryTest, Shell_TypeToString) {
  EXPECT_EQ(to_string_view(ExecutorType::Shell), "shell");
}

TEST(ExecutorTypeRegistryTest, UnknownTypeNumber_ReturnsUnknownString) {
  EXPECT_EQ(to_string_view(static_cast<ExecutorType>(99)), "unknown");
}

TEST(ExecutorTypeRegistryTest, UnknownString_ReturnsDefaultShell) {
  EXPECT_EQ(parse<ExecutorType>("nonexistent"), ExecutorType::Shell);
}

TEST(ExecutorTypeConversionTest, HelperFunctions_MatchRegistry) {
  EXPECT_EQ(to_string_view(ExecutorType::Shell), "shell");
  EXPECT_EQ(parse<ExecutorType>("shell"), ExecutorType::Shell);
}

namespace {
class CountingExecutor final : public IExecutor {
public:
  auto start(ExecutorRequest, ExecutionSink) -> Result<void> override {
    return ok();
  }
  auto cancel(const InstanceId &) -> void override { ++cancel_count_; }

  int cancel_count_{0};
};
} // namespace

TEST(CompositeExecutorTest, CancelWithoutMappingBroadcastsToAllExecutors) {
  CompositeExecutor composite;

  auto shell = std::make_unique<CountingExecutor>();
  auto docker = std::make_unique<CountingExecutor>();
  auto *shell_raw = shell.get();
  auto *docker_raw = docker.get();

  composite.register_executor(ExecutorType::Shell, std::move(shell));
  composite.register_executor(ExecutorType::Docker, std::move(docker));

  composite.cancel(InstanceId{"recovered_run_task"});

  EXPECT_EQ(shell_raw->cancel_count_, 1);
  EXPECT_EQ(docker_raw->cancel_count_, 1);
}

TEST(ExecutorResultTest, DefaultConstruction_HasZeroValues) {
  ExecutorResult result;

  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.stdout_output.empty());
  EXPECT_TRUE(result.stderr_output.empty());
  EXPECT_TRUE(result.error.empty());
  EXPECT_FALSE(result.timed_out);
}

TEST(ShellExecutorConfigTest, DefaultConstruction_HasExpectedDefaults) {
  ShellExecutorConfig config;

  EXPECT_TRUE(config.command.empty());
  EXPECT_TRUE(config.working_dir.empty());
  EXPECT_EQ(config.execution_timeout, std::chrono::seconds(3600));
}

TEST(ShellExecutorConfigTest, CustomValues_ArePreserved) {
  ShellExecutorConfig config;
  config.command = "echo hello";
  config.working_dir = "/tmp";
  config.execution_timeout = std::chrono::seconds(60);

  EXPECT_EQ(config.command, "echo hello");
  EXPECT_EQ(config.working_dir, "/tmp");
  EXPECT_EQ(config.execution_timeout, std::chrono::seconds(60));
}

class ShellExecutorTest : public ::testing::Test {
protected:
  void SetUp() override {
    runtime_ = std::make_unique<Runtime>(1);
    runtime_->start();
    executor_ = create_shell_executor(*runtime_);
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

TEST_F(ShellExecutorTest, ExecuteValidCommand_ReturnsZeroExitCode) {
  ShellExecutorConfig config;
  config.command = "echo hello";
  config.execution_timeout = std::chrono::seconds(5);

  ExecutorResult result;
  std::atomic<bool> completed{false};

  ExecutorRequest req;
  req.instance_id = InstanceId("test_instance");
  req.config = config;

  ExecutionSink sink;
  sink.on_complete = [&](const InstanceId &, ExecutorResult r) {
    result = std::move(r);
    completed = true;
  };

  executor_->start(req, std::move(sink));

  auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (!completed && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  ASSERT_TRUE(completed);
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_FALSE(result.timed_out);
}

TEST_F(ShellExecutorTest, ExecuteFailingCommand_ReturnsNonZeroExitCode) {
  ShellExecutorConfig config;
  config.command = "exit 42";
  config.execution_timeout = std::chrono::seconds(5);

  ExecutorResult result;
  std::atomic<bool> completed{false};

  ExecutorRequest req;
  req.instance_id = InstanceId("test_failing");
  req.config = config;

  ExecutionSink sink;
  sink.on_complete = [&](const InstanceId &, ExecutorResult r) {
    result = std::move(r);
    completed = true;
  };

  executor_->start(req, std::move(sink));

  auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (!completed && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  ASSERT_TRUE(completed);
  EXPECT_EQ(result.exit_code, 42);
  EXPECT_FALSE(result.timed_out);
}

TEST_F(ShellExecutorTest, ExecuteInvalidCommand_ReturnsError) {
  ShellExecutorConfig config;
  config.command = "nonexistent_command_12345";
  config.execution_timeout = std::chrono::seconds(5);

  ExecutorResult result;
  std::atomic<bool> completed{false};

  ExecutorRequest req;
  req.instance_id = InstanceId("test_invalid");
  req.config = config;

  ExecutionSink sink;
  sink.on_complete = [&](const InstanceId &, ExecutorResult r) {
    result = std::move(r);
    completed = true;
  };

  executor_->start(req, std::move(sink));

  auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (!completed && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  ASSERT_TRUE(completed);
  EXPECT_NE(result.exit_code, 0);
}

TEST_F(ShellExecutorTest, ExecuteWithMaliciousEnvKey_ReturnsError) {
  ShellExecutorConfig config;
  config.command = "echo hello";
  config.env["VALID_KEY"] = "value";
  config.env["MALICIOUS_KEY; rm -rf /"] = "value";
  config.execution_timeout = std::chrono::seconds(5);

  ExecutorRequest req;
  req.instance_id = InstanceId("test_malicious");
  req.config = config;

  ExecutionSink sink;

  auto result = executor_->start(req, std::move(sink));
  ASSERT_FALSE(result);
  EXPECT_EQ(result.error(), Error::InvalidArgument);
}

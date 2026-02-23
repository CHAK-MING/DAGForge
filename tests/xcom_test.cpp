#include <gtest/gtest.h>

#include "dagforge/config/task_config.hpp"
#include "dagforge/executor/executor.hpp"
#include "dagforge/xcom/xcom_extractor.hpp"
#include "dagforge/xcom/xcom_types.hpp"

namespace dagforge {
namespace {

auto expect_json_string(const JsonValue &value, std::string_view expected)
    -> void {
  ASSERT_TRUE(value.is_string());
  EXPECT_EQ(value.as<std::string>(), expected);
}

auto expect_json_int(const JsonValue &value, int64_t expected) -> void {
  ASSERT_TRUE(value.is_number());
  EXPECT_EQ(value.as<int64_t>(), expected);
}

auto expect_json_bool(const JsonValue &value, bool expected) -> void {
  ASSERT_TRUE(value.is_boolean());
  EXPECT_EQ(value.get<bool>(), expected);
}

class XComExtractorTest : public ::testing::Test {};

TEST(XComCacheTest, SetAndGetByCompositeKey) {
  XComCache cache;
  cache.set(DAGRunId("run_1"), TaskId("task_a"), "result",
            JsonValue{{"ok", true}});

  auto got = cache.get(DAGRunId("run_1"), TaskId("task_a"), "result");
  ASSERT_TRUE(got);
  expect_json_bool(got->get()["ok"], true);
}

TEST(XComCacheTest, ClearRunRemovesOnlyTargetRunEntries) {
  XComCache cache;
  cache.set(DAGRunId("run_1"), TaskId("task_a"), "k1", "v1");
  cache.set(DAGRunId("run_1"), TaskId("task_b"), "k2", "v2");
  cache.set(DAGRunId("run_2"), TaskId("task_a"), "k1", "v3");

  cache.clear_run(DAGRunId("run_1"));

  EXPECT_FALSE(cache.get(DAGRunId("run_1"), TaskId("task_a"), "k1"));
  EXPECT_FALSE(cache.get(DAGRunId("run_1"), TaskId("task_b"), "k2"));

  auto retained = cache.get(DAGRunId("run_2"), TaskId("task_a"), "k1");
  ASSERT_TRUE(retained);
  expect_json_string(*retained, "v3");
}

TEST_F(XComExtractorTest, ExtractStdoutAsString) {
  ExecutorResult result{.exit_code = 0,
                        .stdout_output = "hello world",
                        .stderr_output = "",
                        .error = "",
                        .timed_out = false};
  std::vector<XComPushConfig> configs{{.key = "output",
                                       .source = XComSource::Stdout,
                                       .json_path = "",
                                       .regex_pattern = ""}};

  auto extracted = xcom::extract(result, configs);
  ASSERT_TRUE(extracted);
  ASSERT_EQ(extracted->size(), 1);
  EXPECT_EQ((*extracted)[0].key, "output");
  expect_json_string((*extracted)[0].value, "hello world");
}

TEST_F(XComExtractorTest, ExtractStderrAsString) {
  ExecutorResult result{.exit_code = 1,
                        .stdout_output = "",
                        .stderr_output = "error message",
                        .error = "",
                        .timed_out = false};
  std::vector<XComPushConfig> configs{{.key = "error",
                                       .source = XComSource::Stderr,
                                       .json_path = "",
                                       .regex_pattern = ""}};

  auto extracted = xcom::extract(result, configs);
  ASSERT_TRUE(extracted);
  ASSERT_EQ(extracted->size(), 1);
  expect_json_string((*extracted)[0].value, "error message");
}

TEST_F(XComExtractorTest, ExtractExitCode) {
  ExecutorResult result{.exit_code = 42,
                        .stdout_output = "",
                        .stderr_output = "",
                        .error = "",
                        .timed_out = false};
  std::vector<XComPushConfig> configs{{.key = "code",
                                       .source = XComSource::ExitCode,
                                       .json_path = "",
                                       .regex_pattern = ""}};

  auto extracted = xcom::extract(result, configs);
  ASSERT_TRUE(extracted);
  expect_json_int((*extracted)[0].value, 42);
}

TEST_F(XComExtractorTest, ExtractJsonFromStdout) {
  ExecutorResult result{.exit_code = 0,
                        .stdout_output = R"({"name": "test", "value": 123})",
                        .stderr_output = "",
                        .error = "",
                        .timed_out = false};
  std::vector<XComPushConfig> configs{{.key = "data",
                                       .source = XComSource::Json,
                                       .json_path = "",
                                       .regex_pattern = ""}};

  auto extracted = xcom::extract(result, configs);
  ASSERT_TRUE(extracted);
  expect_json_string((*extracted)[0].value["name"], "test");
  expect_json_int((*extracted)[0].value["value"], 123);
}

TEST_F(XComExtractorTest, ExtractWithJsonPath) {
  ExecutorResult result{.exit_code = 0,
                        .stdout_output = R"({"result": {"status": "ok"}})",
                        .stderr_output = "",
                        .error = "",
                        .timed_out = false};
  std::vector<XComPushConfig> configs{{.key = "status",
                                       .source = XComSource::Json,
                                       .json_path = "result.status",
                                       .regex_pattern = ""}};

  auto extracted = xcom::extract(result, configs);
  ASSERT_TRUE(extracted);
  expect_json_string((*extracted)[0].value, "ok");
}

TEST_F(XComExtractorTest, ExtractWithRegex) {
  ExecutorResult result{.exit_code = 0,
                        .stdout_output = "Result: 42 items processed",
                        .stderr_output = "",
                        .error = "",
                        .timed_out = false};
  std::vector<XComPushConfig> configs{{.key = "count",
                                       .source = XComSource::Stdout,
                                       .json_path = "",
                                       .regex_pattern = R"(Result: (\d+))",
                                       .regex_group = 1}};

  auto extracted = xcom::extract(result, configs);
  ASSERT_TRUE(extracted);
  expect_json_string((*extracted)[0].value, "42");
}

TEST_F(XComExtractorTest, ExtractMultipleXComs) {
  ExecutorResult result{.exit_code = 0,
                        .stdout_output = "success",
                        .stderr_output = "warnings",
                        .error = "",
                        .timed_out = false};
  std::vector<XComPushConfig> configs{{.key = "out",
                                       .source = XComSource::Stdout,
                                       .json_path = "",
                                       .regex_pattern = ""},
                                      {.key = "err",
                                       .source = XComSource::Stderr,
                                       .json_path = "",
                                       .regex_pattern = ""},
                                      {.key = "code",
                                       .source = XComSource::ExitCode,
                                       .json_path = "",
                                       .regex_pattern = ""}};

  auto extracted = xcom::extract(result, configs);
  ASSERT_TRUE(extracted);
  ASSERT_EQ(extracted->size(), 3);
  expect_json_string((*extracted)[0].value, "success");
  expect_json_string((*extracted)[1].value, "warnings");
  expect_json_int((*extracted)[2].value, 0);
}

TEST_F(XComExtractorTest, InvalidJsonReturnsError) {
  ExecutorResult result{.exit_code = 0,
                        .stdout_output = "not json",
                        .stderr_output = "",
                        .error = "",
                        .timed_out = false};
  std::vector<XComPushConfig> configs{{.key = "data",
                                       .source = XComSource::Json,
                                       .json_path = "",
                                       .regex_pattern = ""}};

  auto extracted = xcom::extract(result, configs);
  EXPECT_FALSE(extracted);
}

TEST_F(XComExtractorTest, RegexNoMatchReturnsError) {
  ExecutorResult result{.exit_code = 0,
                        .stdout_output = "no numbers here",
                        .stderr_output = "",
                        .error = "",
                        .timed_out = false};
  std::vector<XComPushConfig> configs{{.key = "num",
                                       .source = XComSource::Stdout,
                                       .json_path = "",
                                       .regex_pattern = R"(\d+)"}};

  auto extracted = xcom::extract(result, configs);
  EXPECT_FALSE(extracted);
}

TEST_F(XComExtractorTest, JsonPathNotFoundReturnsError) {
  ExecutorResult result{.exit_code = 0,
                        .stdout_output = R"({"a": 1})",
                        .stderr_output = "",
                        .error = "",
                        .timed_out = false};
  std::vector<XComPushConfig> configs{{.key = "missing",
                                       .source = XComSource::Json,
                                       .json_path = "b.c.d",
                                       .regex_pattern = ""}};

  auto extracted = xcom::extract(result, configs);
  EXPECT_FALSE(extracted);
}

TEST_F(XComExtractorTest, JsonPathWithArrayIndex) {
  ExecutorResult result{.exit_code = 0,
                        .stdout_output = R"({"items": [1, 2, 3]})",
                        .stderr_output = "",
                        .error = "",
                        .timed_out = false};
  std::vector<XComPushConfig> configs{{.key = "second",
                                       .source = XComSource::Json,
                                       .json_path = "items[1]",
                                       .regex_pattern = ""}};

  auto extracted = xcom::extract(result, configs);
  ASSERT_TRUE(extracted);
  expect_json_int((*extracted)[0].value, 2);
}

TEST_F(XComExtractorTest, ExtractJsonFromMixedOutput_LastLine) {
  ExecutorResult result{.exit_code = 0,
                        .stdout_output = "Data size: 42\n[\"large_path\"]",
                        .stderr_output = "",
                        .error = "",
                        .timed_out = false};
  std::vector<XComPushConfig> configs{{.key = "branch",
                                       .source = XComSource::Json,
                                       .json_path = "",
                                       .regex_pattern = ""}};

  auto extracted = xcom::extract(result, configs);
  ASSERT_TRUE(extracted);
  EXPECT_EQ((*extracted)[0].value.size(), 1);
  expect_json_string((*extracted)[0].value[0], "large_path");
}

TEST_F(XComExtractorTest, ExtractJsonFromMixedOutput_PrecedingLogs) {
  ExecutorResult result{.exit_code = 0,
                        .stdout_output =
                            "Processing started...\n[\"small_path\"]",
                        .stderr_output = "",
                        .error = "",
                        .timed_out = false};
  std::vector<XComPushConfig> configs{{.key = "branch",
                                       .source = XComSource::Json,
                                       .json_path = "",
                                       .regex_pattern = "",
                                       .regex_group = 0}};

  auto extracted = xcom::extract(result, configs);
  ASSERT_TRUE(extracted);
  expect_json_string((*extracted)[0].value[0], "small_path");
}

TEST_F(XComExtractorTest, ExtractJsonFromMixedOutput_MultipleLogLines) {
  ExecutorResult result{
      .exit_code = 0,
      .stdout_output =
          "Starting process\nSize: 100\nStatus: OK\n{\"result\": \"success\"}",
      .stderr_output = "",
      .error = "",
      .timed_out = false};
  std::vector<XComPushConfig> configs{{.key = "data",
                                       .source = XComSource::Json,
                                       .json_path = "",
                                       .regex_pattern = ""}};

  auto extracted = xcom::extract(result, configs);
  ASSERT_TRUE(extracted);
  expect_json_string((*extracted)[0].value["result"], "success");
}

} // namespace
} // namespace dagforge

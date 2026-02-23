#include "gtest/gtest.h"

#include <array>
#include <cstdio>
#include <string>

#ifndef DAGFORGE_BIN_PATH
#define DAGFORGE_BIN_PATH "dagforge"
#endif

namespace {

struct CmdResult {
  int exit_code{0};
  std::string output;
};

auto run_cmd(const std::string &cmd) -> CmdResult {
  CmdResult result;
  std::array<char, 256> buf{};
  std::string full = cmd + " 2>&1";
  FILE *pipe = ::popen(full.c_str(), "r");
  if (!pipe) return result;
  while (::fgets(buf.data(), buf.size(), pipe)) {
    result.output += buf.data();
  }
  result.exit_code = ::pclose(pipe);
  if (result.exit_code != 0) result.exit_code = 1;
  return result;
}

const std::string kBin = DAGFORGE_BIN_PATH;

} // namespace

TEST(CLIBinarySmokeTest, NoArgsShowsHelp) {
  auto r = run_cmd(kBin);
  EXPECT_NE(r.exit_code, 0);
  EXPECT_FALSE(r.output.empty());
}

TEST(CLIBinarySmokeTest, HelpFlag) {
  auto r = run_cmd(kBin + " --help");
  EXPECT_EQ(r.exit_code, 0);
  EXPECT_NE(r.output.find("serve"), std::string::npos);
  EXPECT_NE(r.output.find("trigger"), std::string::npos);
  EXPECT_NE(r.output.find("list"), std::string::npos);
  EXPECT_NE(r.output.find("pause"), std::string::npos);
  EXPECT_NE(r.output.find("unpause"), std::string::npos);
  EXPECT_NE(r.output.find("clear"), std::string::npos);
  EXPECT_NE(r.output.find("inspect"), std::string::npos);
  EXPECT_NE(r.output.find("validate"), std::string::npos);
  EXPECT_NE(r.output.find("db"), std::string::npos);
}

TEST(CLIBinarySmokeTest, DagsSubcommandHelp) {
  auto r = run_cmd(kBin + " dags --help");
  EXPECT_EQ(r.exit_code, 0);
  EXPECT_NE(r.output.find("pause"), std::string::npos);
  EXPECT_NE(r.output.find("unpause"), std::string::npos);
}

TEST(CLIBinarySmokeTest, DagsPauseRequiresConfig) {
  auto r = run_cmd(kBin + " dags pause my_dag");
  EXPECT_NE(r.exit_code, 0);
}

TEST(CLIBinarySmokeTest, DagsUnpauseRequiresConfig) {
  auto r = run_cmd(kBin + " dags unpause my_dag");
  EXPECT_NE(r.exit_code, 0);
}

TEST(CLIBinarySmokeTest, ListSubcommandHelp) {
  auto r = run_cmd(kBin + " list --help");
  EXPECT_EQ(r.exit_code, 0);
  EXPECT_NE(r.output.find("dags"), std::string::npos);
  EXPECT_NE(r.output.find("runs"), std::string::npos);
  EXPECT_NE(r.output.find("tasks"), std::string::npos);
}

TEST(CLIBinarySmokeTest, ListTasksRequiresDagId) {
  auto r = run_cmd(kBin + " list tasks");
  EXPECT_NE(r.exit_code, 0);
}

TEST(CLIBinarySmokeTest, ClearRequiresRunFlag) {
  auto r = run_cmd(kBin + " clear -c /nonexistent.toml my_dag");
  EXPECT_NE(r.exit_code, 0);
}

TEST(CLIBinarySmokeTest, TriggerRequiresDagId) {
  auto r = run_cmd(kBin + " trigger -c /nonexistent.toml");
  EXPECT_NE(r.exit_code, 0);
}

TEST(CLIBinarySmokeTest, ValidateRequiresConfigOrFile) {
  auto r = run_cmd(kBin + " validate");
  EXPECT_NE(r.exit_code, 0);
}

TEST(CLIBinarySmokeTest, ValidateWithNonexistentFile) {
  auto r = run_cmd(kBin + " validate -f /nonexistent.toml");
  EXPECT_NE(r.exit_code, 0);
}

TEST(CLIBinarySmokeTest, DbSubcommandHelp) {
  auto r = run_cmd(kBin + " db --help");
  EXPECT_EQ(r.exit_code, 0);
  EXPECT_NE(r.output.find("init"), std::string::npos);
  EXPECT_NE(r.output.find("migrate"), std::string::npos);
}

TEST(CLIBinarySmokeTest, TriggerNoConfOption) {
  auto r = run_cmd(kBin + " trigger --help");
  EXPECT_EQ(r.exit_code, 0);
  EXPECT_EQ(r.output.find("--conf "), std::string::npos);
}

TEST(CLIBinarySmokeTest, ServeHelpHasLogLevelOverride) {
  auto r = run_cmd(kBin + " serve start --help");
  EXPECT_EQ(r.exit_code, 0);
  EXPECT_NE(r.output.find("--log-level"), std::string::npos);
}

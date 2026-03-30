#include "test_utils.hpp"

#include "gtest/gtest.h"

#include <array>
#include <csignal>
#include <cstdio>
#include <fstream>
#include <string>
#include <sys/wait.h>
#include <unistd.h>

#ifndef DAGFORGE_BIN_PATH
#define DAGFORGE_BIN_PATH "./bin/dagforge"
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

auto make_serve_config_with_pid_file(std::string_view pid_file) -> std::string {
  auto path = dagforge::test::make_temp_path("dagforge_cli_serve_");
  if (path.empty()) {
    return path;
  }

  std::ofstream out(path, std::ios::trunc);
  if (!out.is_open()) {
    return {};
  }

  out << "[scheduler]\n";
  out << "pid_file = \"" << pid_file << "\"\n\n";
  out << "[api]\n";
  out << "enabled = false\n";
  return path;
}

auto write_pid_file(std::string_view path, pid_t pid) -> void {
  std::ofstream out(std::string(path), std::ios::trunc);
  ASSERT_TRUE(out.is_open());
  out << pid << '\n';
}

auto spawn_pausing_child(bool ignore_sigterm) -> pid_t {
  const auto child = ::fork();
  EXPECT_NE(child, -1);
  if (child == 0) {
    if (ignore_sigterm) {
      std::signal(SIGTERM, SIG_IGN);
    }
    for (;;) {
      ::pause();
    }
  }
  return child;
}

auto reap_child(pid_t child) -> void {
  if (child <= 0) {
    return;
  }
  int status = 0;
  (void)::waitpid(child, &status, 0);
}

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

TEST(CLIBinarySmokeTest, ServeSubcommandHelp) {
  auto r = run_cmd(kBin + " serve --help");
  EXPECT_EQ(r.exit_code, 0);
  EXPECT_NE(r.output.find("start"), std::string::npos);
  EXPECT_NE(r.output.find("status"), std::string::npos);
  EXPECT_NE(r.output.find("stop"), std::string::npos);
}

TEST(CLIBinarySmokeTest, PauseRequiresConfig) {
  auto r = run_cmd(kBin + " pause my_dag");
  EXPECT_NE(r.exit_code, 0);
}

TEST(CLIBinarySmokeTest, UnpauseRequiresConfig) {
  auto r = run_cmd(kBin + " unpause my_dag");
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
  EXPECT_NE(r.output.find("--pid-file"), std::string::npos);
}

TEST(CLIBinarySmokeTest, ServeStartRejectsZeroShards) {
  auto r = run_cmd(kBin + " serve start -c /nonexistent.toml --shards 0");
  EXPECT_NE(r.exit_code, 0);
}

TEST(CLIBinarySmokeTest, ServeStopRejectsZeroTimeout) {
  auto r = run_cmd(kBin + " serve stop -c /nonexistent.toml --timeout 0");
  EXPECT_NE(r.exit_code, 0);
}

TEST(CLIBinarySmokeTest, LogsRejectsZeroAttempt) {
  auto r = run_cmd(kBin + " logs -c /nonexistent.toml my_dag --attempt 0");
  EXPECT_NE(r.exit_code, 0);
}

TEST(CLIBinarySmokeTest, ListDagsRejectsZeroLimit) {
  auto r = run_cmd(kBin + " list dags -c /nonexistent.toml --limit 0");
  EXPECT_NE(r.exit_code, 0);
}

TEST(CLIBinarySmokeTest, ListRunsRejectsZeroLimit) {
  auto r = run_cmd(kBin + " list runs -c /nonexistent.toml --limit 0");
  EXPECT_NE(r.exit_code, 0);
}

TEST(CLIBinaryServeStopTest, RemovesStalePidFile) {
  const auto pid_file = dagforge::test::make_temp_path("dagforge_pid_");
  const auto config_file = make_serve_config_with_pid_file(pid_file);
  ASSERT_FALSE(pid_file.empty());
  ASSERT_FALSE(config_file.empty());

  const auto child = ::fork();
  ASSERT_NE(child, -1);
  if (child == 0) {
    _exit(0);
  }
  reap_child(child);

  write_pid_file(pid_file, child);

  auto r = run_cmd(kBin + " serve stop -c " + config_file);
  EXPECT_EQ(r.exit_code, 0);
  EXPECT_NE(r.output.find("stale pid file removed"), std::string::npos);

  std::ifstream pid_in(pid_file);
  EXPECT_FALSE(pid_in.good());

  ::unlink(config_file.c_str());
}

TEST(CLIBinaryServeStopTest, ReportsTimeoutWithoutForceForUnresponsiveProcess) {
  const auto pid_file = dagforge::test::make_temp_path("dagforge_pid_");
  const auto config_file = make_serve_config_with_pid_file(pid_file);
  ASSERT_FALSE(pid_file.empty());
  ASSERT_FALSE(config_file.empty());

  const auto child = spawn_pausing_child(true);
  ASSERT_GT(child, 0);
  write_pid_file(pid_file, child);

  auto r = run_cmd(kBin + " serve stop -c " + config_file + " --timeout 1");
  EXPECT_NE(r.exit_code, 0);
  EXPECT_NE(r.output.find("Retry with --force"), std::string::npos);
  EXPECT_EQ(::kill(child, 0), 0);

  ASSERT_EQ(::kill(child, SIGKILL), 0);
  reap_child(child);
  ::unlink(pid_file.c_str());
  ::unlink(config_file.c_str());
}

TEST(CLIBinaryServeStopTest, ForceKillsUnresponsiveProcess) {
  const auto pid_file = dagforge::test::make_temp_path("dagforge_pid_");
  const auto config_file = make_serve_config_with_pid_file(pid_file);
  ASSERT_FALSE(pid_file.empty());
  ASSERT_FALSE(config_file.empty());

  const auto child = spawn_pausing_child(true);
  ASSERT_GT(child, 0);
  write_pid_file(pid_file, child);

  auto r =
      run_cmd(kBin + " serve stop -c " + config_file + " --timeout 1 --force");
  EXPECT_EQ(r.exit_code, 0);
  EXPECT_NE(r.output.find("DAGForge killed"), std::string::npos);
  reap_child(child);
  EXPECT_NE(::kill(child, 0), 0);

  std::ifstream pid_in(pid_file);
  EXPECT_FALSE(pid_in.good());

  ::unlink(config_file.c_str());
}

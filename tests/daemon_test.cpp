#include "dagforge/util/daemon.hpp"
#include "test_utils.hpp"

#include "gtest/gtest.h"

#include <chrono>
#include <fstream>
#include <future>
#include <sys/wait.h>
#include <thread>
#include <unistd.h>

using namespace dagforge;

namespace {

auto make_pid_file_path() -> std::string {
  std::string path = dagforge::test::make_temp_path("dagforge_pid_test_");
  if (!path.empty()) {
    ::unlink(path.c_str());
  }
  return path;
}

} // namespace

TEST(DaemonTest, PidFileGuardAcquireWritesPidAndRemovesOnRelease) {
  const auto path = make_pid_file_path();
  ASSERT_FALSE(path.empty());

  {
    auto guard = PidFileGuard::acquire(path);
    ASSERT_TRUE(guard.has_value()) << guard.error().message();

    auto pid = read_pid_file(path);
    ASSERT_TRUE(pid.has_value()) << pid.error().message();
    EXPECT_EQ(*pid, static_cast<std::int64_t>(::getpid()));
  }

  auto pid = read_pid_file(path);
  ASSERT_FALSE(pid.has_value());
  EXPECT_EQ(pid.error(), make_error_code(Error::FileNotFound));
}

TEST(DaemonTest, ReadPidFileRejectsInvalidContent) {
  const auto path = make_pid_file_path();
  ASSERT_FALSE(path.empty());

  {
    std::ofstream out(path, std::ios::trunc);
    ASSERT_TRUE(out.is_open());
    out << "not-a-pid\n";
  }

  auto pid = read_pid_file(path);
  ASSERT_FALSE(pid.has_value());
  EXPECT_EQ(pid.error(), make_error_code(Error::ParseError));

  ::unlink(path.c_str());
}

TEST(DaemonTest, SendSignalAndWaitForProcessExit) {
  const pid_t child = ::fork();
  ASSERT_NE(child, -1);

  if (child == 0) {
    for (;;) {
      ::pause();
    }
  }

  ASSERT_TRUE(is_process_alive(child));
  auto sent = send_signal(child, SIGTERM);
  ASSERT_TRUE(sent.has_value()) << sent.error().message();
  EXPECT_TRUE(wait_for_process_exit(child, std::chrono::seconds(2)));
}

TEST(DaemonTest, WaitForShutdownReturnsAfterSignalHandlerRuns) {
  g_shutdown_requested.store(false, std::memory_order_release);
  setup_signal_handlers();

  std::promise<void> done;
  auto future = done.get_future();
  std::thread waiter([&done]() {
    wait_for_shutdown();
    done.set_value();
  });

  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  ASSERT_EQ(::kill(::getpid(), SIGTERM), 0);
  EXPECT_EQ(future.wait_for(std::chrono::seconds(2)),
            std::future_status::ready);

  waiter.join();
  g_shutdown_requested.store(false, std::memory_order_release);
}

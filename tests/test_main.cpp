#include <csignal>
#include <gtest/gtest.h>

int main(int argc, char **argv) {
  // Global signal handling for tests
  std::signal(SIGPIPE, SIG_IGN);

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

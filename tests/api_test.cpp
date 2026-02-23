#include "dagforge/app/api/api_server.hpp"
#include "dagforge/app/application.hpp"

#include "gtest/gtest.h"

using namespace dagforge;

TEST(ApiTest, ApiServerConstructs) {
  Config cfg;
  cfg.api.enabled = false;
  Application app(std::move(cfg));
  ApiServer server(app);
  EXPECT_FALSE(server.is_running());
}

TEST(ApiTest, ApplicationConfigAccessible) {
  Application app;
  EXPECT_EQ(app.config().api.host, "127.0.0.1");
}

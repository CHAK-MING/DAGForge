#pragma once

#include <glaze/json.hpp>

#include <cstdint>
#include <string>

namespace dagforge::executor_dto {

struct SensorExecutorConfigJson {
  std::string type;
  std::string target;
  std::int64_t poke_interval{30};
  bool soft_fail{false};
  std::int64_t expected_status{200};
  std::string http_method{"GET"};
};

struct DockerExecutorConfigJson {
  std::string image;
  std::string socket{"/var/run/docker.sock"};
  std::string pull_policy;
};

} // namespace dagforge::executor_dto

namespace glz {

template <> struct meta<dagforge::executor_dto::SensorExecutorConfigJson> {
  using T = dagforge::executor_dto::SensorExecutorConfigJson;
  static constexpr auto value =
      object("type", &T::type, "target", &T::target, "poke_interval",
             &T::poke_interval, "soft_fail", &T::soft_fail, "expected_status",
             &T::expected_status, "http_method", &T::http_method);
};

template <> struct meta<dagforge::executor_dto::DockerExecutorConfigJson> {
  using T = dagforge::executor_dto::DockerExecutorConfigJson;
  static constexpr auto value = object("image", &T::image, "socket", &T::socket,
                                       "pull_policy", &T::pull_policy);
};

} // namespace glz

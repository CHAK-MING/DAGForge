#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/util/enum.hpp"
#endif

#include <chrono>
#include <cstdint>
#include <flat_map>
#include <memory>
#include <string>

#include <boost/describe/enum.hpp>

namespace dagforge {

enum class ExecutorType : std::uint8_t {
  Shell,
  Docker,
  Sensor,
  Noop,
};
BOOST_DESCRIBE_ENUM(ExecutorType, Shell, Docker, Sensor, Noop)

enum class ImagePullPolicy : std::uint8_t {
  Never,
  IfNotPresent,
  Always,
};
BOOST_DESCRIBE_ENUM(ImagePullPolicy, Never, IfNotPresent, Always)
DAGFORGE_DEFINE_ENUM_SERDE(ExecutorType, ExecutorType::Shell)
DAGFORGE_DEFINE_ENUM_SERDE(ImagePullPolicy, ImagePullPolicy::Never)

struct ShellExecutorConfig {
  std::flat_map<std::string, std::string> env;
};

struct DockerExecutorConfig {
  std::string image;
  std::flat_map<std::string, std::string> env;
  std::string docker_socket{"/var/run/docker.sock"};
  ImagePullPolicy pull_policy{ImagePullPolicy::Never};
};

enum class SensorType : std::uint8_t {
  File,
  Http,
  Command,
};
BOOST_DESCRIBE_ENUM(SensorType, File, Http, Command)
DAGFORGE_DEFINE_ENUM_SERDE(SensorType, SensorType::File)

struct SensorExecutorConfig {
  SensorType type{SensorType::File};
  std::string target;
  std::chrono::seconds poke_interval{std::chrono::seconds(30)};
  bool soft_fail{false};
  int expected_status{200};
  std::string http_method{"GET"};
};

struct NoopExecutorConfig {
  int exit_code{0};
};

template <typename T> struct ExecutorConfigTraits;

template <> struct ExecutorConfigTraits<ShellExecutorConfig> {
  static constexpr ExecutorType type = ExecutorType::Shell;
};

template <> struct ExecutorConfigTraits<DockerExecutorConfig> {
  static constexpr ExecutorType type = ExecutorType::Docker;
};

template <> struct ExecutorConfigTraits<SensorExecutorConfig> {
  static constexpr ExecutorType type = ExecutorType::Sensor;
};

template <> struct ExecutorConfigTraits<NoopExecutorConfig> {
  static constexpr ExecutorType type = ExecutorType::Noop;
};

class ExecutorConfig {
public:
  ExecutorConfig() : ExecutorConfig(ShellExecutorConfig{}) {}

  template <typename T>
  ExecutorConfig(T value)
      : payload_(std::make_shared<Model<T>>(std::move(value))) {}

  template <typename T> auto operator=(T value) -> ExecutorConfig & {
    payload_ = std::make_shared<Model<T>>(std::move(value));
    return *this;
  }

  [[nodiscard]] auto type() const noexcept -> ExecutorType {
    return payload_ != nullptr ? payload_->type() : ExecutorType::Shell;
  }

  template <typename T> [[nodiscard]] auto as() noexcept -> T * {
    auto *model = dynamic_cast<Model<T> *>(payload_.get());
    return model != nullptr ? &model->value : nullptr;
  }

  template <typename T> [[nodiscard]] auto as() const noexcept -> const T * {
    auto *model = dynamic_cast<const Model<T> *>(payload_.get());
    return model != nullptr ? &model->value : nullptr;
  }

private:
  struct Concept {
    virtual ~Concept() = default;
    [[nodiscard]] virtual auto type() const noexcept -> ExecutorType = 0;
  };

  template <typename T> struct Model final : Concept {
    explicit Model(T value_) : value(std::move(value_)) {}
    [[nodiscard]] auto type() const noexcept -> ExecutorType override {
      return ExecutorConfigTraits<T>::type;
    }
    T value;
  };

  std::shared_ptr<Concept> payload_;
};

inline constexpr int kExitCodeTimeout = 124;
inline constexpr int kExitCodeSkip = 100;
inline constexpr int kExitCodeImmediateFail = 101;

} // namespace dagforge

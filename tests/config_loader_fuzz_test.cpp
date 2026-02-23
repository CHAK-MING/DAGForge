#include "dagforge/config/dag_definition.hpp"
#include "dagforge/core/error.hpp"

#include <gtest/gtest.h>

#include <random>
#include <string>
#include <vector>

namespace dagforge::test {
namespace {

auto is_expected_parse_failure(std::error_code ec) -> bool {
  return ec == make_error_code(Error::InvalidArgument) ||
         ec == make_error_code(Error::ParseError);
}

} // namespace

TEST(ConfigLoaderFuzzTest, MalformedInputsNeverCrashAndReturnValidationErrors) {
  std::vector<std::string> corpus = {
      "",
      "id = \"\"\n[[tasks]]\nid = \"t1\"\ncommand = \"echo ok\"\n",
      "id = \"fuzz\"\nname = \"fuzz\"\ntasks = []\n",
      "id = \"fuzz\"\n[[tasks]]\nid = \"\"\ncommand = \"echo\"\n",
      "id = \"fuzz\"\n[[tasks]]\nid = \"t1\"\ncommand = \"echo\"\ndependencies "
      "= [\"missing\"]\n",
      "id = \"fuzz\"\nname = \"fuzz\"\n[[tasks]]\nid = \"t1\"\ncommand = "
      "\"echo\"\n[default_args\n",
      "id = \"fuzz\"\nname = \"fuzz\"\n[[tasks]]\nid = \"t1\"\ncommand = "
      "\"echo\"\n" +
          std::string(4096, 'a'),
  };

  std::mt19937 rng(42);
  std::uniform_int_distribution<int> len_dist(1, 512);
  std::uniform_int_distribution<int> byte_dist(0, 255);
  for (int i = 0; i < 32; ++i) {
    std::string noise;
    const int len = len_dist(rng);
    noise.reserve(static_cast<std::size_t>(len));
    for (int j = 0; j < len; ++j) {
      noise.push_back(static_cast<char>(byte_dist(rng)));
    }
    corpus.push_back(std::move(noise));
  }

  for (const auto &input : corpus) {
    Result<DAGInfo> result;
    ASSERT_NO_THROW(result = DAGDefinitionLoader::load_from_string(input));

    if (!result.has_value()) {
      EXPECT_TRUE(is_expected_parse_failure(result.error()))
          << "unexpected error: " << result.error().message();
    }
  }
}

} // namespace dagforge::test

#include "dagforge/xcom/xcom_extractor.hpp"

#include <boost/algorithm/string/trim.hpp>
#include <ranges>
#include <regex>
#include <utility>

namespace dagforge::xcom {

namespace {

[[nodiscard]] auto get_source_text(const ExecutorResult &result,
                                   XComSource source) -> std::string {
  switch (source) {
  case XComSource::Stdout:
    return std::string(result.stdout_output);
  case XComSource::Stderr:
    return std::string(result.stderr_output);
  case XComSource::ExitCode:
    return std::to_string(result.exit_code);
  case XComSource::Json:
    return std::string(result.stdout_output);
  }
  std::unreachable();
}

[[nodiscard]] auto extract_json_from_output(std::string_view output)
    -> std::string {
  if (output.empty())
    return "";

  if (is_valid_json(output)) {
    return std::string(output);
  }

  auto last_start = output.find_last_of("{[");
  if (last_start != std::string_view::npos) {
    auto potential_json = boost::trim_copy(output.substr(last_start));
    if (is_valid_json(potential_json)) {
      return std::string{potential_json};
    }
  }

  return std::string(output);
}

[[nodiscard]] auto apply_json_path(const JsonValue &json, std::string_view path)
    -> Result<JsonValue> {
  const JsonValue *cur = &json;

  if (!path.empty() && path.front() == '.') {
    path.remove_prefix(1);
  }

  for (auto part : path | std::views::split('.')) {
    std::string_view token(part.begin(), part.end());
    if (token.empty())
      continue;

    // Split token on '[' to separate key from optional array index
    auto bracket = token.find('[');
    std::string_view key = token.substr(0, bracket);

    if (!key.empty()) {
      if (!cur->contains(key))
        return fail(Error::NotFound);
      cur = &(*cur)[key];
    }

    // Process all bracket subscripts: [0][1]...
    while (bracket != std::string_view::npos) {
      auto close = token.find(']', bracket);
      if (close == std::string_view::npos)
        return fail(Error::InvalidArgument);
      auto index_sv = token.substr(bracket + 1, close - bracket - 1);
      std::size_t index{};
      auto [p, ec] = std::from_chars(index_sv.data(),
                                     index_sv.data() + index_sv.size(), index);
      if (ec != std::errc{} || !cur->is_array() || index >= cur->size()) {
        return fail(Error::NotFound);
      }
      cur = &cur->get_array()[index];
      bracket = token.find('[', close + 1);
    }
  }

  return ok(*cur);
}

[[nodiscard]] auto extract_one(const ExecutorResult &result,
                               const XComPushConfig &config)
    -> Result<ExtractedXCom> {
  std::string source_text = get_source_text(result, config.source);
  auto text_res = ok(std::move(source_text));
  if (!config.regex_pattern.empty()) {
    try {
      std::regex re(config.regex_pattern);
      std::match_results<std::string::const_iterator> match;
      const auto &text = *text_res;

      if (!std::regex_search(text.begin(), text.end(), match, re)) {
        return fail(Error::NotFound);
      }

      if (config.regex_group < 0 ||
          static_cast<size_t>(config.regex_group) >= match.size()) {
        return fail(Error::InvalidArgument);
      }

      text_res = ok(std::string(match[config.regex_group].first,
                                match[config.regex_group].second));
    } catch (const std::regex_error &) {
      return fail(Error::InvalidArgument);
    }
  }

  return std::move(text_res).and_then(
      [&](std::string &&text) -> Result<ExtractedXCom> {
        if (config.source == XComSource::Json || !config.json_path.empty()) {
          if (config.source == XComSource::Json) {
            text = extract_json_from_output(text);
          }
          auto parsed = parse_json(text);
          if (!parsed) {
            return fail(Error::InvalidArgument);
          }

          if (!config.json_path.empty()) {
            return apply_json_path(*parsed, config.json_path)
                .transform([&](auto &&val) {
                  return ExtractedXCom{.key = config.key, .value = val};
                });
          }
          return ok(
              ExtractedXCom{.key = config.key, .value = std::move(*parsed)});
        }

        JsonValue value = (config.source == XComSource::ExitCode)
                              ? JsonValue(result.exit_code)
                              : JsonValue(std::move(text));

        return ok(ExtractedXCom{.key = config.key, .value = std::move(value)});
      });
}

} // namespace

auto extract(const ExecutorResult &result,
             const std::vector<XComPushConfig> &configs)
    -> Result<std::vector<ExtractedXCom>> {
  std::vector<ExtractedXCom> extracted;
  extracted.reserve(configs.size());

  for (const auto &config : configs) {
    auto xcom_result = extract_one(result, config);
    if (!xcom_result) {
      return fail(xcom_result.error());
    }
    extracted.emplace_back(std::move(*xcom_result));
  }

  return ok(extracted);
}

} // namespace dagforge::xcom

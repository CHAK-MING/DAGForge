#include "dagforge/config/dag_file_loader.hpp"
#include "dagforge/util/log.hpp"


namespace dagforge {

DAGFileLoader::DAGFileLoader(std::string_view directory)
    : directory_(directory) {}

auto DAGFileLoader::load_all() -> Result<std::vector<DAGFile>> {
  return load_all_impl(true);
}

auto DAGFileLoader::load_all_strict() -> Result<std::vector<DAGFile>> {
  return load_all_impl(false);
}

auto DAGFileLoader::load_all_impl(bool ignore_invalid_files)
    -> Result<std::vector<DAGFile>> {
  if (!std::filesystem::exists(directory_)) {
    log::warn("DAG directory does not exist: {}", directory_.string());
    return ok(std::vector<DAGFile>{});
  }

  auto canonical_dir = std::filesystem::canonical(directory_);
  std::vector<DAGFile> dags;

  for (const auto &entry : std::filesystem::directory_iterator(directory_)) {
    if (!entry.is_regular_file()) {
      continue;
    }

    auto ext = entry.path().extension().string();
    if (ext != ".toml") {
      continue;
    }

    auto canonical_path = std::filesystem::canonical(entry.path());
    auto [iter, _] = std::mismatch(canonical_dir.begin(), canonical_dir.end(),
                                   canonical_path.begin());
    if (iter != canonical_dir.end()) {
      log::warn("Skipping file outside DAG directory (possible symlink "
                "traversal): {}",
                entry.path().string());
      continue;
    }

    auto result = load_file(entry.path());
    if (result) {
      dags.emplace_back(std::move(*result));
      log::debug("Loaded DAG '{}' from file: {}", dags.back().dag_id,
                 entry.path().string());
    } else {
      if (!ignore_invalid_files) {
        log::error("Failed to load DAG from {}: {}", entry.path().string(),
                   result.error().message());
        return fail(result.error());
      }
      log::warn("Failed to load DAG from {}: {}", entry.path().string(),
                result.error().message());
    }
  }

  log::debug("Loaded {} DAG(s) from {}", dags.size(), directory_.string());
  return ok(std::move(dags));
}

auto DAGFileLoader::load_file(const std::filesystem::path &path)
    -> Result<DAGFile> {
  return DAGInfoLoader::load_from_file(path.string())
      .transform([&](auto &&info) {
        return DAGFile{std::move(DAGId{path.stem().string()}),
                       std::forward<decltype(info)>(info)};
      });
}

auto DAGFileLoader::directory() const -> const std::filesystem::path & {
  return directory_;
}

} // namespace dagforge

#include "dagforge/scheduler/cron.hpp"
#include "dagforge/util/conv.hpp"

#include <array>
#include <bitset>
#include <boost/algorithm/string/predicate.hpp>
#include <boost/algorithm/string/trim.hpp>
#include <ranges>
#include <string>
#include <vector>

namespace dagforge {
namespace {

constexpr std::array<std::pair<std::string_view, std::string_view>, 6> kMacros{{
    {"@yearly", "0 0 1 1 *"},
    {"@annually", "0 0 1 1 *"},
    {"@monthly", "0 0 1 * *"},
    {"@weekly", "0 0 * * 0"},
    {"@daily", "0 0 * * *"},
    {"@hourly", "0 * * * *"},
}};

constexpr std::array<std::string_view, 12> kMonthNames{
    "jan", "feb", "mar", "apr", "may", "jun",
    "jul", "aug", "sep", "oct", "nov", "dec"};

constexpr std::array<std::string_view, 7> kDowNames{"sun", "mon", "tue", "wed",
                                                    "thu", "fri", "sat"};

auto parse_name(std::string_view s,
                const std::array<std::string_view, 12> &names12,
                const std::array<std::string_view, 7> &names7, bool use_12)
    -> Result<int> {
  if (use_12) {
    for (auto [i, name] : std::views::enumerate(names12)) {
      if (boost::algorithm::iequals(s, name)) {
        return ok(static_cast<int>(i + 1));
      }
    }
  } else {
    for (auto [i, name] : std::views::enumerate(names7)) {
      if (boost::algorithm::iequals(s, name)) {
        return ok(static_cast<int>(i));
      }
    }
  }
  return fail(Error::ParseError);
}

template <std::size_t N>
auto parse_field(std::string_view field, std::bitset<N> &bs, int min_val,
                 int max_val, bool &is_restricted,
                 const std::array<std::string_view, 12> *month_names = nullptr,
                 const std::array<std::string_view, 7> *dow_names = nullptr)
    -> Result<void> {
  bs.reset();
  is_restricted = true;

  auto parse_value = [&](std::string_view s) -> Result<int> {
    if (auto v = util::parse_int<int>(s))
      return *v;
    if (month_names) {
      if (auto v = parse_name(s, *month_names, kDowNames, true))
        return *v;
    }
    if (dow_names) {
      if (auto v = parse_name(s, kMonthNames, *dow_names, false)) {
        return (*v == 7) ? 0 : *v;
      }
    }
    return fail(Error::ParseError);
  };

  std::vector<std::string> parts;
  for (auto chunk : std::string_view(field) | std::views::split(',')) {
    parts.emplace_back(boost::trim_copy(std::string_view(chunk)));
  }
  for (auto &part : parts) {
    if (part.empty())
      return fail(Error::ParseError);

    int step = 1;
    if (auto slash = part.find('/'); slash != std::string::npos) {
      auto step_opt = util::parse_int<int>(part.substr(slash + 1));
      if (!step_opt || *step_opt <= 0)
        return fail(Error::ParseError);
      step = *step_opt;
      part = part.substr(0, slash);
    }

    int start;
    int end;
    if (part == "*" || part == "?") {
      start = min_val;
      end = max_val;
      if (step == 1)
        is_restricted = false;
    } else if (auto dash = part.find('-'); dash != std::string::npos) {
      auto left = part.substr(0, dash);
      auto right = part.substr(dash + 1);
      left = boost::trim_copy(left);
      right = boost::trim_copy(right);
      auto a = parse_value(left);
      auto b = parse_value(right);
      if (!a || !b)
        return fail(Error::ParseError);
      start = *a;
      end = *b;
      if (dow_names && end == 7) {
        for (int v = start; v < 7; v += step) {
          if (v >= min_val && v <= max_val)
            bs.set(static_cast<std::size_t>(v));
        }
        bs.set(0);
        continue;
      }
    } else {
      auto v = parse_value(part);
      if (!v)
        return fail(Error::ParseError);
      start = end = (dow_names && *v == 7) ? 0 : *v;
    }

    if (start < min_val || end > max_val || start > end)
      return fail(Error::ParseError);
    for (int v = start; v <= end; v += step) {
      bs.set(static_cast<std::size_t>(v));
    }
  }
  if (!bs.any())
    return fail(Error::ParseError);
  return ok();
}

constexpr int days_in_month(int year, int month) {
  const auto y = std::chrono::year{year};
  const auto m = std::chrono::month{static_cast<unsigned>(month)};
  const auto last =
      std::chrono::year_month_day_last{y, std::chrono::month_day_last{m}};
  return static_cast<int>(static_cast<unsigned>(last.day()));
}

template <std::size_t N>
auto next_set(const std::bitset<N> &bs, int from, int max_val)
    -> std::optional<int> {
  auto range = std::views::iota(from, max_val + 1);
  auto it = std::ranges::find_if(
      range, [&bs](int v) { return bs.test(static_cast<std::size_t>(v)); });
  if (it != range.end()) {
    return *it;
  }
  return std::nullopt;
}

template <std::size_t N>
auto first_set(const std::bitset<N> &bs, int min_val, int max_val) -> int {
  auto range = std::views::iota(min_val, max_val + 1);
  auto it = std::ranges::find_if(
      range, [&bs](int v) { return bs.test(static_cast<std::size_t>(v)); });
  return it != range.end() ? *it : min_val;
}

} // namespace

CronExpr::CronExpr(std::string raw, Fields fields)
    : raw_(std::move(raw)), fields_(fields) {}

auto CronExpr::parse(std::string_view expr) -> Result<CronExpr> {
  std::string trimmed(boost::trim_copy(expr));
  if (trimmed.empty())
    return fail(Error::InvalidArgument);

  std::string to_parse = trimmed;
  if (!to_parse.empty() && to_parse[0] == '@') {
    bool found = false;
    for (const auto &[macro, expansion] : kMacros) {
      if (boost::algorithm::iequals(to_parse, macro)) {
        to_parse = std::string(expansion);
        found = true;
        break;
      }
    }
    if (!found)
      return fail(Error::ParseError);
  }

  std::vector<std::string> tokens;
  for (auto chunk : std::string_view(to_parse) | std::views::split(' ')) {
    tokens.emplace_back(std::string_view(chunk));
  }
  if (tokens.size() != 5)
    return fail(Error::ParseError);

  Fields f{};
  bool dummy;
  if (!parse_field(tokens[0], f.minute, 0, 59, dummy))
    return fail(Error::ParseError);
  if (!parse_field(tokens[1], f.hour, 0, 23, dummy))
    return fail(Error::ParseError);
  if (!parse_field(tokens[2], f.dom, 1, 31, f.dom_restricted))
    return fail(Error::ParseError);
  if (!parse_field(tokens[3], f.month, 1, 12, dummy, &kMonthNames))
    return fail(Error::ParseError);
  if (!parse_field(tokens[4], f.dow, 0, 6, f.dow_restricted, nullptr,
                   &kDowNames))
    return fail(Error::ParseError);

  return ok(CronExpr(std::string(trimmed), f));
}

auto CronExpr::next_after(std::chrono::system_clock::time_point after) const
    -> std::chrono::system_clock::time_point {
  using namespace std::chrono;

  auto candidate = floor<minutes>(after) + minutes{1};
  const auto max_day = floor<days>(after) + days{366 * 5 + 2};

  auto day_ok = [this](const year_month_day &ymd, int dow) {
    const auto dom = static_cast<unsigned>(ymd.day());
    bool dom_ok = !fields_.dom_restricted ||
                  fields_.dom.test(static_cast<std::size_t>(dom));
    bool dow_ok = !fields_.dow_restricted ||
                  fields_.dow.test(static_cast<std::size_t>(dow));
    if (!fields_.dom_restricted && !fields_.dow_restricted)
      return true;
    if (!fields_.dom_restricted)
      return dow_ok;
    if (!fields_.dow_restricted)
      return dom_ok;
    return dom_ok || dow_ok;
  };

  while (floor<days>(candidate) <= max_day) {
    auto day_tp = floor<days>(candidate);
    auto ymd = year_month_day{day_tp};

    const int current_month =
        static_cast<int>(static_cast<unsigned>(ymd.month()));
    if (auto m = next_set(fields_.month, current_month, 12)) {
      if (*m != current_month) {
        day_tp = sys_days{ymd.year() / month{static_cast<unsigned>(*m)} / 1};
        candidate = day_tp;
        continue;
      }
    } else {
      const auto next_year = ymd.year() + years{1};
      day_tp = sys_days{
          next_year /
          month{static_cast<unsigned>(first_set(fields_.month, 1, 12))} / 1};
      candidate = day_tp;
      continue;
    }

    ymd = year_month_day{day_tp};
    const int dim =
        days_in_month(static_cast<int>(ymd.year()),
                      static_cast<int>(static_cast<unsigned>(ymd.month())));
    if (static_cast<int>(static_cast<unsigned>(ymd.day())) > dim) {
      candidate = day_tp + days{1};
      continue;
    }

    const int dow = static_cast<int>(weekday{day_tp}.c_encoding());
    if (!day_ok(ymd, dow)) {
      candidate = day_tp + days{1};
      continue;
    }

    const auto min_of_day = duration_cast<minutes>(candidate - day_tp).count();
    int cur_hour = static_cast<int>(min_of_day / 60);
    int cur_min = static_cast<int>(min_of_day % 60);

    if (auto h = next_set(fields_.hour, cur_hour, 23)) {
      cur_hour = *h;
      const int minute_start =
          (cur_hour == static_cast<int>(min_of_day / 60)) ? cur_min : 0;
      if (auto m = next_set(fields_.minute, minute_start, 59)) {
        return day_tp + hours{cur_hour} + minutes{*m};
      }

      if (auto next_h = next_set(fields_.hour, cur_hour + 1, 23)) {
        return day_tp + hours{*next_h} +
               minutes{first_set(fields_.minute, 0, 59)};
      }
    }

    candidate = day_tp + days{1};
  }

  return std::chrono::system_clock::time_point::max();
}

auto CronExpr::all_between(std::chrono::system_clock::time_point start,
                           std::chrono::system_clock::time_point end,
                           size_t max_count) const
    -> std::vector<std::chrono::system_clock::time_point> {
  std::vector<std::chrono::system_clock::time_point> result;
  result.reserve(std::min(max_count, size_t{64}));

  auto current = start - std::chrono::minutes(1);
  while (result.size() < max_count) {
    current = next_after(current);
    if (current >= end ||
        current == std::chrono::system_clock::time_point::max()) {
      break;
    }
    result.emplace_back(current);
  }

  return result;
}

} // namespace dagforge

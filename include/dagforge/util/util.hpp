#pragma once


#if !defined(DAGFORGE_CONSUME_NAMED_MODULES) ||                                  \
    !DAGFORGE_CONSUME_NAMED_MODULES
#include "dagforge/util/conv.hpp"
#include "dagforge/util/enum.hpp"
#include "dagforge/util/hash.hpp"
#include "dagforge/util/time.hpp"
#endif
#include "dagforge/util/encoding.hpp"
#include "dagforge/util/string_hash.hpp"
#include "dagforge/util/url.hpp"

namespace dagforge {

template <class... Ts> struct overloaded : Ts... {
  using Ts::operator()...;
};
template <class... Ts> overloaded(Ts...) -> overloaded<Ts...>;

} // namespace dagforge

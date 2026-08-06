#pragma once

#include <cstddef>

namespace openevent {

inline constexpr std::size_t kObjectTokenRandomBytes = 32;
inline constexpr std::size_t kObjectTokenEncodedBytes = 43;
inline constexpr std::size_t kMaxObjectNameBytes = 255;
inline constexpr std::size_t kMaxObjectTypeBytes = 255;
inline constexpr std::size_t kMaxObjectDescriptionBytes = 4096;
inline constexpr std::size_t kMaxObjectBytes = 4ULL * 1024 * 1024;
inline constexpr int kMaxObjectKeys = 1024;

}  // namespace openevent

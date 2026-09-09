#pragma once

#include <cstddef>

namespace openevent {

inline constexpr int kGrpcMessageBytes = 64 * 1024 * 1024;
inline constexpr std::size_t kMessagePageSoftBytes = 17 * 1024 * 1024;

}  // namespace openevent

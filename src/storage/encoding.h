#pragma once

#include <cstdint>
#include <string>

namespace openevent {

inline std::string EncodeUint64(uint64_t value)
{
    std::string out(8, '\0');
    for (int i = 7; i >= 0; --i) {
        out[static_cast<size_t>(7 - i)] = static_cast<char>((value >> (i * 8)) & 0xff);
    }
    return out;
}

inline uint64_t DecodeUint64(const std::string& value)
{
    if (value.size() != 8) {
        return 0;
    }
    uint64_t out = 0;
    for (unsigned char ch : value) {
        out = (out << 8) | ch;
    }
    return out;
}

}  // namespace openevent

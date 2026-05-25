#pragma once

#include <algorithm>
#include <cstdint>
#include <cstring>
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

inline std::string PaddedKey(const char* prefix, uint64_t value)
{
    std::string key(prefix);
    std::string digits = std::to_string(value);
    if (digits.size() < 20) {
        key.append(20 - digits.size(), '0');
    }
    key += digits;
    return key;
}

inline bool StartsWith(const std::string& value, const char* prefix)
{
    const size_t n = std::strlen(prefix);
    return value.size() >= n && value.compare(0, n, prefix) == 0;
}

inline uint64_t ParsePaddedId(const std::string& key, const char* prefix)
{
    const size_t n = std::strlen(prefix);
    if (key.size() <= n) {
        return 0;
    }
    return std::stoull(key.substr(n));
}

}  // namespace openevent

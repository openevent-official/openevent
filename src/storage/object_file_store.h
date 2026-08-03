#pragma once

#include <cstdint>
#include <memory>
#include <string>

#include "common/status.h"

namespace openevent {

class ObjectFileStore {
public:
    ~ObjectFileStore();

    ObjectFileStore(const ObjectFileStore&) = delete;
    ObjectFileStore& operator=(const ObjectFileStore&) = delete;

    static Result<std::unique_ptr<ObjectFileStore>> Open(const std::string& path);
    static Result<std::string> Sha256(const std::string& data);
    static bool ConstantTimeEquals(const std::string& left, const std::string& right);

    Status Write(uint64_t object_id, const std::string& data) const;
    Status Cleanup(uint64_t object_id) const;
    Result<std::string> ReadAndValidate(uint64_t object_id,
                                       uint64_t expected_size,
                                       const std::string& expected_sha256) const;

private:
    explicit ObjectFileStore(int directory_fd);

    int directory_fd_ = -1;
};

}  // namespace openevent

#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "common/status.h"
#include "storage/fault_injector.h"

namespace openevent {

class ObjectFileStore {
public:
    ~ObjectFileStore();

    ObjectFileStore(const ObjectFileStore&) = delete;
    ObjectFileStore& operator=(const ObjectFileStore&) = delete;

    static Result<std::unique_ptr<ObjectFileStore>> Open(
        const std::string& path,
        StorageFaultInjector fault_injector = {});
    static bool ConstantTimeEquals(const std::string& left, const std::string& right);

    Status Write(uint64_t object_id, const std::string& data) const;
    Status Cleanup(uint64_t object_id) const;
    Status Cleanup(const std::vector<uint64_t>& object_ids) const;
    Result<std::string> ReadRange(uint64_t object_id,
                                  uint64_t expected_size,
                                  uint64_t offset,
                                  uint64_t nbytes) const;

private:
    ObjectFileStore(int directory_fd, StorageFaultInjector fault_injector);

    int directory_fd_ = -1;
    StorageFaultInjector fault_injector_;
};

}  // namespace openevent

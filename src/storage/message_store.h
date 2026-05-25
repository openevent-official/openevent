#pragma once

#include <cstdint>
#include <vector>

#include "common/status.h"
#include "openevent.pb.h"

namespace openevent {

struct StoredMessage {
    uint64_t offset = 0;
    ::openevent::Message message;
};

struct FetchRecordsResult {
    std::vector<StoredMessage> records;
    bool has_more = false;
};

class MessageStore {
public:
    virtual ~MessageStore() = default;

    virtual Result<uint64_t> Append(const ::openevent::Message& message) = 0;
    virtual Result<FetchRecordsResult> Fetch(uint64_t from_offset, uint32_t limit) = 0;
};

}  // namespace openevent

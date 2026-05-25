#pragma once

#include <memory>
#include <mutex>
#include <string>

#include <rocksdb/db.h>

#include "storage/message_store.h"

namespace openevent {

class RocksDBMessageStore final : public MessageStore {
public:
    explicit RocksDBMessageStore(std::unique_ptr<rocksdb::DB> db);

    static Result<std::unique_ptr<RocksDBMessageStore>> Open(const std::string& path);

    Result<uint64_t> Append(const ::openevent::Message& message) override;
    Result<FetchRecordsResult> Fetch(uint64_t from_offset, uint32_t limit) override;

private:
    Result<uint64_t> GetMaxOffset() const;

    std::unique_ptr<rocksdb::DB> db_;
    mutable std::mutex mu_;
};

}  // namespace openevent

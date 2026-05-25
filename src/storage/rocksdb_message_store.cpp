#include "storage/rocksdb_message_store.h"

#include <algorithm>
#include <filesystem>

#include <rocksdb/options.h>
#include <rocksdb/write_batch.h>

#include "storage/encoding.h"

namespace openevent {
namespace {

constexpr const char* kMaxOffsetKey = "meta:max_offset";
constexpr const char* kMinOffsetKey = "meta:min_offset";
constexpr const char* kMessagePrefix = "msg:";

Status RocksToStatus(const rocksdb::Status& status, const std::string& prefix)
{
    if (status.ok()) {
        return Status::Ok();
    }
    return Status(grpc::StatusCode::UNAVAILABLE, prefix + ": " + status.ToString());
}

}  // namespace

RocksDBMessageStore::RocksDBMessageStore(std::unique_ptr<rocksdb::DB> db)
    : db_(std::move(db))
{
}

Result<std::unique_ptr<RocksDBMessageStore>> RocksDBMessageStore::Open(const std::string& path)
{
    std::error_code ec;
    std::filesystem::create_directories(path, ec);
    if (ec) {
        return Status(grpc::StatusCode::UNAVAILABLE, "create message store directory: " + ec.message());
    }

    rocksdb::Options options;
    options.create_if_missing = true;

    rocksdb::DB* raw_db = nullptr;
    rocksdb::Status status = rocksdb::DB::Open(options, path, &raw_db);
    if (!status.ok()) {
        return RocksToStatus(status, "open message store");
    }

    return std::make_unique<RocksDBMessageStore>(std::unique_ptr<rocksdb::DB>(raw_db));
}

Result<uint64_t> RocksDBMessageStore::GetMaxOffset() const
{
    std::string value;
    rocksdb::Status status = db_->Get(rocksdb::ReadOptions(), kMaxOffsetKey, &value);
    if (status.IsNotFound()) {
        return uint64_t{0};
    }
    if (!status.ok()) {
        return RocksToStatus(status, "read max offset");
    }
    return DecodeUint64(value);
}

Result<uint64_t> RocksDBMessageStore::Append(const ::openevent::Message& message)
{
    std::lock_guard<std::mutex> lock(mu_);

    auto max_result = GetMaxOffset();
    if (!max_result.ok()) {
        return max_result.status();
    }

    const uint64_t offset = max_result.value() + 1;
    std::string payload;
    if (!message.SerializeToString(&payload)) {
        return Status(grpc::StatusCode::INTERNAL, "serialize message failed");
    }

    rocksdb::WriteBatch batch;
    batch.Put(PaddedKey(kMessagePrefix, offset), payload);
    batch.Put(kMaxOffsetKey, EncodeUint64(offset));
    if (offset == 1) {
        batch.Put(kMinOffsetKey, EncodeUint64(uint64_t{1}));
    }

    rocksdb::Status status = db_->Write(rocksdb::WriteOptions(), &batch);
    if (!status.ok()) {
        return RocksToStatus(status, "append message");
    }
    return offset;
}

Result<FetchRecordsResult> RocksDBMessageStore::Fetch(uint64_t from_offset, uint32_t limit)
{
    FetchRecordsResult result;
    if (limit == 0) {
        return result;
    }

    std::lock_guard<std::mutex> lock(mu_);

    auto max_result = GetMaxOffset();
    if (!max_result.ok()) {
        return max_result.status();
    }
    const uint64_t max_offset = max_result.value();
    if (from_offset == 0) {
        from_offset = 1;
    }
    if (from_offset > max_offset) {
        return result;
    }

    const uint64_t scan_end = max_offset;
    for (uint64_t offset = from_offset; offset <= scan_end; ++offset) {
        std::string value;
        rocksdb::Status status = db_->Get(rocksdb::ReadOptions(), PaddedKey(kMessagePrefix, offset), &value);
        if (status.IsNotFound()) {
            continue;
        }
        if (!status.ok()) {
            return RocksToStatus(status, "fetch message");
        }

        StoredMessage stored;
        stored.offset = offset;
        if (!stored.message.ParseFromString(value)) {
            return Status(grpc::StatusCode::INTERNAL, "parse stored message failed");
        }
        result.records.push_back(std::move(stored));
        if (result.records.size() >= limit) {
            result.has_more = offset < max_offset;
            break;
        }
    }

    return result;
}

}  // namespace openevent

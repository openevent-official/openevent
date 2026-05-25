#include "storage/metadata_store.h"

#include <filesystem>
#include <memory>

#include <rocksdb/options.h>
#include <rocksdb/write_batch.h>

#include "storage/encoding.h"

namespace openevent {
namespace {

constexpr const char* kMaxSeqKey = "meta:max_seq";
constexpr const char* kMinSeqKey = "meta:min_seq";
constexpr const char* kNextChannelIdKey = "meta:next_channel_id";
constexpr const char* kChannelPrefix = "ch:";
constexpr const char* kChannelLastPrefix = "ch_last:";
constexpr const char* kSeqPrefix = "seq:";
constexpr const char* kOffsetPrefix = "offset:";
constexpr const char* kSeqTimestampPrefix = "seq_ts:";
constexpr const char* kPendingMessagePrefix = "pend_msg:";
constexpr const char* kPendingStatePrefix = "pend_state:";
constexpr const char* kPendingOffsetPrefix = "pend_offset:";
constexpr const char* kTokenPrefix = "token:";

Status RocksToStatus(const rocksdb::Status& status, const std::string& prefix)
{
    if (status.ok()) {
        return Status::Ok();
    }
    return Status(grpc::StatusCode::UNAVAILABLE, prefix + ": " + status.ToString());
}

}  // namespace

MetadataStore::MetadataStore(std::unique_ptr<rocksdb::DB> db)
    : db_(std::move(db))
{
}

Result<std::unique_ptr<MetadataStore>> MetadataStore::Open(const std::string& path)
{
    std::error_code ec;
    std::filesystem::create_directories(path, ec);
    if (ec) {
        return Status(grpc::StatusCode::UNAVAILABLE, "create metadata directory: " + ec.message());
    }

    rocksdb::Options options;
    options.create_if_missing = true;

    rocksdb::DB* raw_db = nullptr;
    rocksdb::Status status = rocksdb::DB::Open(options, path, &raw_db);
    if (!status.ok()) {
        return RocksToStatus(status, "open metadata store");
    }

    return std::make_unique<MetadataStore>(std::unique_ptr<rocksdb::DB>(raw_db));
}

Result<uint64_t> MetadataStore::GetUint64(const std::string& key) const
{
    std::string value;
    rocksdb::Status status = db_->Get(rocksdb::ReadOptions(), key, &value);
    if (status.IsNotFound()) {
        return uint64_t{0};
    }
    if (!status.ok()) {
        return RocksToStatus(status, "read " + key);
    }
    return DecodeUint64(value);
}

Status MetadataStore::PutUint64(const std::string& key, uint64_t value)
{
    rocksdb::Status status = db_->Put(rocksdb::WriteOptions(), key, EncodeUint64(value));
    return RocksToStatus(status, "write " + key);
}

Result<uint64_t> MetadataStore::GetMaxSeq() const
{
    return GetUint64(kMaxSeqKey);
}

Result<uint64_t> MetadataStore::GetMinSeq() const
{
    return GetUint64(kMinSeqKey);
}

Result<uint64_t> MetadataStore::GetNextChannelId() const
{
    return GetUint64(kNextChannelIdKey);
}

Status MetadataStore::PutPendingMessage(uint64_t seq, const ::openevent::Message& message)
{
    std::string payload;
    if (!message.SerializeToString(&payload)) {
        return Status(grpc::StatusCode::INTERNAL, "serialize pending message failed");
    }

    rocksdb::WriteBatch batch;
    batch.Put(PaddedKey(kPendingMessagePrefix, seq), payload);
    batch.Put(PaddedKey(kPendingStatePrefix, seq), EncodeUint64(static_cast<uint64_t>(PendingState::kPending)));
    return RocksToStatus(db_->Write(rocksdb::WriteOptions(), &batch), "write pending message");
}

Status MetadataStore::PutPendingOffset(uint64_t seq, uint64_t offset)
{
    return PutUint64(PaddedKey(kPendingOffsetPrefix, seq), offset);
}

Status MetadataStore::CommitMessage(uint64_t seq, uint64_t offset, uint64_t channel_id, uint64_t timestamp_ms)
{
    rocksdb::WriteBatch batch;
    batch.Put(PaddedKey(kSeqPrefix, seq), EncodeUint64(offset));
    batch.Put(PaddedKey(kOffsetPrefix, offset), EncodeUint64(seq));
    batch.Put(PaddedKey(kSeqTimestampPrefix, seq), EncodeUint64(timestamp_ms));
    batch.Put(PaddedKey(kChannelLastPrefix, channel_id), EncodeUint64(seq));
    batch.Put(kMaxSeqKey, EncodeUint64(seq));
    if (seq == 1) {
        batch.Put(kMinSeqKey, EncodeUint64(uint64_t{1}));
    }
    batch.Put(PaddedKey(kPendingStatePrefix, seq), EncodeUint64(static_cast<uint64_t>(PendingState::kCommitted)));
    return RocksToStatus(db_->Write(rocksdb::WriteOptions(), &batch), "commit message");
}

Result<std::vector<uint64_t>> MetadataStore::ListPendingSeqs() const
{
    std::vector<uint64_t> seqs;
    std::unique_ptr<rocksdb::Iterator> it(db_->NewIterator(rocksdb::ReadOptions()));
    for (it->Seek(kPendingStatePrefix); it->Valid() && StartsWith(it->key().ToString(), kPendingStatePrefix);
         it->Next()) {
        const uint64_t state = DecodeUint64(it->value().ToString());
        if (state == static_cast<uint64_t>(PendingState::kPending)) {
            seqs.push_back(ParsePaddedId(it->key().ToString(), kPendingStatePrefix));
        }
    }
    if (!it->status().ok()) {
        return RocksToStatus(it->status(), "list pending states");
    }
    return seqs;
}

Result<std::optional<::openevent::Message>> MetadataStore::GetPendingMessage(uint64_t seq) const
{
    std::string value;
    rocksdb::Status status = db_->Get(rocksdb::ReadOptions(), PaddedKey(kPendingMessagePrefix, seq), &value);
    if (status.IsNotFound()) {
        return std::optional<::openevent::Message>{};
    }
    if (!status.ok()) {
        return RocksToStatus(status, "read pending message");
    }

    ::openevent::Message message;
    if (!message.ParseFromString(value)) {
        return Status(grpc::StatusCode::INTERNAL, "parse pending message failed");
    }
    return std::optional<::openevent::Message>{std::move(message)};
}

Result<std::optional<uint64_t>> MetadataStore::GetPendingOffset(uint64_t seq) const
{
    std::string value;
    rocksdb::Status status = db_->Get(rocksdb::ReadOptions(), PaddedKey(kPendingOffsetPrefix, seq), &value);
    if (status.IsNotFound()) {
        return std::optional<uint64_t>{};
    }
    if (!status.ok()) {
        return RocksToStatus(status, "read pending offset");
    }
    return std::optional<uint64_t>{DecodeUint64(value)};
}

Result<std::optional<uint64_t>> MetadataStore::GetOffsetForSeq(uint64_t seq) const
{
    std::string value;
    rocksdb::Status status = db_->Get(rocksdb::ReadOptions(), PaddedKey(kSeqPrefix, seq), &value);
    if (status.IsNotFound()) {
        return std::optional<uint64_t>{};
    }
    if (!status.ok()) {
        return RocksToStatus(status, "read seq mapping");
    }
    return std::optional<uint64_t>{DecodeUint64(value)};
}

Result<std::optional<uint64_t>> MetadataStore::GetSeqForOffset(uint64_t offset) const
{
    std::string value;
    rocksdb::Status status = db_->Get(rocksdb::ReadOptions(), PaddedKey(kOffsetPrefix, offset), &value);
    if (status.IsNotFound()) {
        return std::optional<uint64_t>{};
    }
    if (!status.ok()) {
        return RocksToStatus(status, "read offset mapping");
    }
    return std::optional<uint64_t>{DecodeUint64(value)};
}

Result<uint64_t> MetadataStore::AllocateChannelId()
{
    auto next_result = GetNextChannelId();
    if (!next_result.ok()) {
        return next_result.status();
    }
    uint64_t channel_id = next_result.value();
    if (channel_id == 0) {
        channel_id = 1;
    }
    Status status = PutUint64(kNextChannelIdKey, channel_id + 1);
    if (!status.ok()) {
        return status;
    }
    return channel_id;
}

Status MetadataStore::PutChannel(const ::openevent::ChannelInfo& channel)
{
    std::string payload;
    if (!channel.SerializeToString(&payload)) {
        return Status(grpc::StatusCode::INTERNAL, "serialize channel failed");
    }
    return RocksToStatus(db_->Put(rocksdb::WriteOptions(), PaddedKey(kChannelPrefix, channel.channel_id()), payload),
                         "write channel");
}

Result<std::optional<::openevent::ChannelInfo>> MetadataStore::GetChannel(uint64_t channel_id) const
{
    std::string value;
    rocksdb::Status status = db_->Get(rocksdb::ReadOptions(), PaddedKey(kChannelPrefix, channel_id), &value);
    if (status.IsNotFound()) {
        return std::optional<::openevent::ChannelInfo>{};
    }
    if (!status.ok()) {
        return RocksToStatus(status, "read channel");
    }

    ::openevent::ChannelInfo channel;
    if (!channel.ParseFromString(value)) {
        return Status(grpc::StatusCode::INTERNAL, "parse channel failed");
    }
    return std::optional<::openevent::ChannelInfo>{std::move(channel)};
}

Result<std::vector<::openevent::ChannelInfo>> MetadataStore::ListChannels() const
{
    std::vector<::openevent::ChannelInfo> channels;
    std::unique_ptr<rocksdb::Iterator> it(db_->NewIterator(rocksdb::ReadOptions()));
    for (it->Seek(kChannelPrefix); it->Valid() && StartsWith(it->key().ToString(), kChannelPrefix); it->Next()) {
        ::openevent::ChannelInfo channel;
        if (!channel.ParseFromString(it->value().ToString())) {
            return Status(grpc::StatusCode::INTERNAL, "parse channel failed");
        }
        channels.push_back(std::move(channel));
    }
    if (!it->status().ok()) {
        return RocksToStatus(it->status(), "list channels");
    }
    return channels;
}

Status MetadataStore::PutToken(const std::string& token, uint64_t principal)
{
    return RocksToStatus(db_->Put(rocksdb::WriteOptions(), std::string(kTokenPrefix) + token, EncodeUint64(principal)),
                         "write token");
}

Status MetadataStore::DeleteToken(const std::string& token)
{
    return RocksToStatus(db_->Delete(rocksdb::WriteOptions(), std::string(kTokenPrefix) + token), "delete token");
}

Result<std::optional<uint64_t>> MetadataStore::GetPrincipalForToken(const std::string& token) const
{
    std::string value;
    rocksdb::Status status = db_->Get(rocksdb::ReadOptions(), std::string(kTokenPrefix) + token, &value);
    if (status.IsNotFound()) {
        return std::optional<uint64_t>{};
    }
    if (!status.ok()) {
        return RocksToStatus(status, "read token");
    }
    return std::optional<uint64_t>{DecodeUint64(value)};
}

Result<std::vector<TokenBindingRecord>> MetadataStore::ListTokens() const
{
    std::vector<TokenBindingRecord> bindings;
    std::unique_ptr<rocksdb::Iterator> it(db_->NewIterator(rocksdb::ReadOptions()));
    for (it->Seek(kTokenPrefix); it->Valid() && StartsWith(it->key().ToString(), kTokenPrefix); it->Next()) {
        TokenBindingRecord record;
        record.token = it->key().ToString().substr(std::strlen(kTokenPrefix));
        record.principal = DecodeUint64(it->value().ToString());
        bindings.push_back(std::move(record));
    }
    if (!it->status().ok()) {
        return RocksToStatus(it->status(), "list tokens");
    }
    return bindings;
}

}  // namespace openevent

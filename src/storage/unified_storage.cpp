#include "storage/unified_storage.h"

#include <algorithm>
#include <cstring>
#include <filesystem>
#include <memory>
#include <set>
#include <utility>

#include <rocksdb/options.h>

#include "storage/encoding.h"

namespace openevent {
namespace {

constexpr uint64_t kSchemaVersion = 1;
constexpr uint64_t kInitializing = 1;
constexpr const char* kMessagesColumnFamily = "messages";
constexpr const char* kSchemaVersionKey = "meta:schema_version";
constexpr const char* kInitStateKey = "meta:init_state";
constexpr const char* kMaxSeqKey = "meta:max_seq";
constexpr const char* kNextChannelIdKey = "meta:next_channel_id";
constexpr const char* kChannelPrefix = "ch/";
constexpr const char* kTokenPrefix = "token:";
constexpr const char* kMessagePrefix = "msg/";

Status RocksToStatus(const rocksdb::Status& status, const std::string& prefix)
{
    if (status.ok()) {
        return Status::Ok();
    }
    return Status(grpc::StatusCode::UNAVAILABLE, prefix + ": " + status.ToString());
}

rocksdb::WriteOptions SynchronousWriteOptions()
{
    rocksdb::WriteOptions options;
    options.sync = true;
    options.disableWAL = false;
    return options;
}

std::string NumericKey(const char* prefix, uint64_t value)
{
    return std::string(prefix) + EncodeUint64(value);
}

Result<uint64_t> DecodeRequiredUint64(const std::string& value, const std::string& description)
{
    if (value.size() != sizeof(uint64_t)) {
        return Status(grpc::StatusCode::INTERNAL, "invalid " + description + " encoding");
    }
    return DecodeUint64(value);
}

Result<uint64_t> DecodeNumericKey(const rocksdb::Slice& key, const char* prefix, const std::string& description)
{
    const size_t prefix_size = std::strlen(prefix);
    if (key.size() != prefix_size + sizeof(uint64_t) ||
        std::memcmp(key.data(), prefix, prefix_size) != 0) {
        return Status(grpc::StatusCode::INTERNAL, "invalid " + description + " key");
    }
    return DecodeUint64(std::string(key.data() + prefix_size, sizeof(uint64_t)));
}

bool IsNewStoragePath(const std::string& path)
{
    std::error_code ec;
    if (!std::filesystem::exists(path, ec)) {
        return !ec;
    }
    if (ec || !std::filesystem::is_directory(path, ec) || ec) {
        return false;
    }
    return std::filesystem::directory_iterator(path, ec) == std::filesystem::directory_iterator() && !ec;
}

Status DestroyHandles(rocksdb::DB* db, std::vector<rocksdb::ColumnFamilyHandle*>* handles)
{
    for (auto* handle : *handles) {
        rocksdb::Status status = db->DestroyColumnFamilyHandle(handle);
        if (!status.ok()) {
            return RocksToStatus(status, "destroy column family handle");
        }
    }
    handles->clear();
    return Status::Ok();
}

Result<bool> IsInitializationOnly(rocksdb::DB* db,
                                  rocksdb::ColumnFamilyHandle* meta,
                                  rocksdb::ColumnFamilyHandle* messages)
{
    std::unique_ptr<rocksdb::Iterator> meta_it(db->NewIterator(rocksdb::ReadOptions(), meta));
    size_t meta_keys = 0;
    bool has_marker = false;
    for (meta_it->SeekToFirst(); meta_it->Valid(); meta_it->Next()) {
        ++meta_keys;
        if (meta_it->key().ToString() == kInitStateKey) {
            auto state = DecodeRequiredUint64(meta_it->value().ToString(), "initialization marker");
            if (!state.ok()) {
                return state.status();
            }
            has_marker = state.value() == kInitializing;
        }
    }
    if (!meta_it->status().ok()) {
        return RocksToStatus(meta_it->status(), "scan initialization metadata");
    }
    if (!has_marker || meta_keys != 1) {
        return false;
    }

    if (messages != nullptr) {
        std::unique_ptr<rocksdb::Iterator> message_it(db->NewIterator(rocksdb::ReadOptions(), messages));
        message_it->SeekToFirst();
        if (!message_it->status().ok()) {
            return RocksToStatus(message_it->status(), "scan initialization messages");
        }
        if (message_it->Valid()) {
            return false;
        }
    }
    return true;
}

}  // namespace

ReadSnapshot::ReadSnapshot(const UnifiedStorage* owner, const rocksdb::Snapshot* snapshot)
    : owner_(owner), snapshot_(snapshot)
{
}

ReadSnapshot::ReadSnapshot(ReadSnapshot&& other) noexcept
    : owner_(std::exchange(other.owner_, nullptr)), snapshot_(std::exchange(other.snapshot_, nullptr))
{
}

ReadSnapshot& ReadSnapshot::operator=(ReadSnapshot&& other) noexcept
{
    if (this != &other) {
        Reset();
        owner_ = std::exchange(other.owner_, nullptr);
        snapshot_ = std::exchange(other.snapshot_, nullptr);
    }
    return *this;
}

ReadSnapshot::~ReadSnapshot()
{
    Reset();
}

void ReadSnapshot::Reset()
{
    if (owner_ != nullptr && snapshot_ != nullptr) {
        owner_->ReleaseSnapshot(snapshot_);
    }
    owner_ = nullptr;
    snapshot_ = nullptr;
}

UnifiedStorage::UnifiedStorage(std::unique_ptr<rocksdb::DB> db,
                               rocksdb::ColumnFamilyHandle* meta,
                               rocksdb::ColumnFamilyHandle* messages)
    : db_(std::move(db)), meta_(meta), messages_(messages)
{
}

UnifiedStorage::~UnifiedStorage()
{
    if (db_ != nullptr) {
        if (messages_ != nullptr) {
            db_->DestroyColumnFamilyHandle(messages_);
        }
        if (meta_ != nullptr) {
            db_->DestroyColumnFamilyHandle(meta_);
        }
    }
}

Result<std::unique_ptr<UnifiedStorage>> UnifiedStorage::Open(const std::string& path)
{
    if (path.empty()) {
        return Status(grpc::StatusCode::INVALID_ARGUMENT, "storage path must not be empty");
    }

    if (IsNewStoragePath(path)) {
        Status status = InitializeNew(path);
        if (!status.ok()) {
            return status;
        }
    } else {
        std::vector<std::string> column_families;
        rocksdb::Options options;
        rocksdb::Status list_status = rocksdb::DB::ListColumnFamilies(options, path, &column_families);
        if (!list_status.ok()) {
            return RocksToStatus(list_status, "list storage column families");
        }
        const std::set<std::string> names(column_families.begin(), column_families.end());
        const std::set<std::string> supported{rocksdb::kDefaultColumnFamilyName, kMessagesColumnFamily};
        if (names != supported && names != std::set<std::string>{rocksdb::kDefaultColumnFamilyName}) {
            return Status(grpc::StatusCode::INTERNAL, "unsupported storage column family layout");
        }
        Status status = CompleteInitialization(path, column_families);
        if (!status.ok()) {
            return status;
        }
    }

    return OpenExisting(path);
}

Status UnifiedStorage::InitializeNew(const std::string& path)
{
    std::error_code ec;
    std::filesystem::create_directories(path, ec);
    if (ec) {
        return Status(grpc::StatusCode::UNAVAILABLE, "create storage directory: " + ec.message());
    }

    rocksdb::Options options;
    options.create_if_missing = true;
    rocksdb::DB* raw_db = nullptr;
    rocksdb::Status open_status = rocksdb::DB::Open(options, path, &raw_db);
    if (!open_status.ok()) {
        return RocksToStatus(open_status, "create storage");
    }
    std::unique_ptr<rocksdb::DB> db(raw_db);

    rocksdb::Status marker_status = db->Put(SynchronousWriteOptions(), kInitStateKey, EncodeUint64(kInitializing));
    if (!marker_status.ok()) {
        return RocksToStatus(marker_status, "write storage initialization marker");
    }

    rocksdb::ColumnFamilyHandle* messages = nullptr;
    rocksdb::Status cf_status = db->CreateColumnFamily(rocksdb::ColumnFamilyOptions(), kMessagesColumnFamily, &messages);
    if (!cf_status.ok()) {
        return RocksToStatus(cf_status, "create messages column family");
    }

    rocksdb::WriteBatch batch;
    batch.Put(kSchemaVersionKey, EncodeUint64(kSchemaVersion));
    batch.Put(kMaxSeqKey, EncodeUint64(uint64_t{0}));
    batch.Put(kNextChannelIdKey, EncodeUint64(uint64_t{1}));
    batch.Delete(kInitStateKey);
    rocksdb::Status init_status = db->Write(SynchronousWriteOptions(), &batch);
    rocksdb::Status destroy_status = db->DestroyColumnFamilyHandle(messages);
    if (!init_status.ok()) {
        return RocksToStatus(init_status, "finish storage initialization");
    }
    if (!destroy_status.ok()) {
        return RocksToStatus(destroy_status, "destroy messages column family handle");
    }
    return Status::Ok();
}

Status UnifiedStorage::CompleteInitialization(const std::string& path,
                                              const std::vector<std::string>& column_families)
{
    std::vector<rocksdb::ColumnFamilyDescriptor> descriptors;
    for (const auto& name : column_families) {
        descriptors.emplace_back(name, rocksdb::ColumnFamilyOptions());
    }
    rocksdb::DBOptions options;
    std::vector<rocksdb::ColumnFamilyHandle*> handles;
    rocksdb::DB* raw_db = nullptr;
    rocksdb::Status open_status = rocksdb::DB::Open(options, path, descriptors, &handles, &raw_db);
    if (!open_status.ok()) {
        return RocksToStatus(open_status, "open partial storage");
    }
    std::unique_ptr<rocksdb::DB> db(raw_db);
    rocksdb::ColumnFamilyHandle* meta = handles.front();
    rocksdb::ColumnFamilyHandle* messages = handles.size() == 2 ? handles[1] : nullptr;

    auto initialization_only = IsInitializationOnly(db.get(), meta, messages);
    if (!initialization_only.ok()) {
        DestroyHandles(db.get(), &handles);
        return initialization_only.status();
    }
    if (!initialization_only.value()) {
        std::string marker;
        rocksdb::Status marker_status = db->Get(rocksdb::ReadOptions(), meta, kInitStateKey, &marker);
        if (marker_status.IsNotFound()) {
            return DestroyHandles(db.get(), &handles);
        }
        DestroyHandles(db.get(), &handles);
        if (!marker_status.ok()) {
            return RocksToStatus(marker_status, "read storage initialization marker");
        }
        return Status(grpc::StatusCode::INTERNAL, "incomplete storage has no valid initialization marker");
    }

    if (messages == nullptr) {
        rocksdb::Status cf_status =
            db->CreateColumnFamily(rocksdb::ColumnFamilyOptions(), kMessagesColumnFamily, &messages);
        if (!cf_status.ok()) {
            DestroyHandles(db.get(), &handles);
            return RocksToStatus(cf_status, "complete messages column family creation");
        }
        handles.push_back(messages);
    }

    rocksdb::WriteBatch batch;
    batch.Put(meta, kSchemaVersionKey, EncodeUint64(kSchemaVersion));
    batch.Put(meta, kMaxSeqKey, EncodeUint64(uint64_t{0}));
    batch.Put(meta, kNextChannelIdKey, EncodeUint64(uint64_t{1}));
    batch.Delete(meta, kInitStateKey);
    rocksdb::Status write_status = db->Write(SynchronousWriteOptions(), &batch);
    Status destroy_status = DestroyHandles(db.get(), &handles);
    if (!write_status.ok()) {
        return RocksToStatus(write_status, "complete storage initialization");
    }
    return destroy_status;
}

Result<std::unique_ptr<UnifiedStorage>> UnifiedStorage::OpenExisting(const std::string& path)
{
    rocksdb::DBOptions options;
    std::vector<rocksdb::ColumnFamilyDescriptor> descriptors{
        {rocksdb::kDefaultColumnFamilyName, rocksdb::ColumnFamilyOptions()},
        {kMessagesColumnFamily, rocksdb::ColumnFamilyOptions()},
    };
    std::vector<rocksdb::ColumnFamilyHandle*> handles;
    rocksdb::DB* raw_db = nullptr;
    rocksdb::Status open_status = rocksdb::DB::Open(options, path, descriptors, &handles, &raw_db);
    if (!open_status.ok()) {
        return RocksToStatus(open_status, "open unified storage");
    }
    if (handles.size() != 2) {
        std::unique_ptr<rocksdb::DB> db(raw_db);
        DestroyHandles(db.get(), &handles);
        return Status(grpc::StatusCode::INTERNAL, "unified storage column family count mismatch");
    }

    auto storage = std::unique_ptr<UnifiedStorage>(new UnifiedStorage(
        std::unique_ptr<rocksdb::DB>(raw_db), handles[0], handles[1]));
    Status validation = storage->Validate();
    if (!validation.ok()) {
        return validation;
    }
    return storage;
}

Result<ReadSnapshot> UnifiedStorage::CreateSnapshot() const
{
    const rocksdb::Snapshot* snapshot = db_->GetSnapshot();
    if (snapshot == nullptr) {
        return Status(grpc::StatusCode::UNAVAILABLE, "create storage snapshot failed");
    }
    return ReadSnapshot(this, snapshot);
}

void UnifiedStorage::ReleaseSnapshot(const rocksdb::Snapshot* snapshot) const
{
    db_->ReleaseSnapshot(snapshot);
}

Status UnifiedStorage::Commit(rocksdb::WriteBatch* batch)
{
    return RocksToStatus(db_->Write(SynchronousWriteOptions(), batch), "commit storage batch");
}

Result<uint64_t> UnifiedStorage::GetRequiredUint64(const ReadSnapshot& snapshot, const std::string& key) const
{
    rocksdb::ReadOptions options;
    options.snapshot = snapshot.snapshot_;
    std::string value;
    rocksdb::Status status = db_->Get(options, meta_, key, &value);
    if (status.IsNotFound()) {
        return Status(grpc::StatusCode::INTERNAL, "missing required storage key: " + key);
    }
    if (!status.ok()) {
        return RocksToStatus(status, "read " + key);
    }
    return DecodeRequiredUint64(value, key);
}

Result<uint64_t> UnifiedStorage::GetMaxSeq(const ReadSnapshot& snapshot) const
{
    return GetRequiredUint64(snapshot, kMaxSeqKey);
}

Result<uint64_t> UnifiedStorage::GetNextChannelId(const ReadSnapshot& snapshot) const
{
    return GetRequiredUint64(snapshot, kNextChannelIdKey);
}

Result<std::optional<uint64_t>> UnifiedStorage::GetPrincipalForToken(const ReadSnapshot& snapshot,
                                                                    const std::string& token) const
{
    rocksdb::ReadOptions options;
    options.snapshot = snapshot.snapshot_;
    std::string value;
    rocksdb::Status status = db_->Get(options, meta_, std::string(kTokenPrefix) + token, &value);
    if (status.IsNotFound()) {
        return std::optional<uint64_t>{};
    }
    if (!status.ok()) {
        return RocksToStatus(status, "read token binding");
    }
    auto principal = DecodeRequiredUint64(value, "token principal");
    if (!principal.ok()) {
        return principal.status();
    }
    return std::optional<uint64_t>{principal.value()};
}

Result<std::optional<ChannelInfo>> UnifiedStorage::GetChannel(const ReadSnapshot& snapshot, uint64_t channel_id) const
{
    rocksdb::ReadOptions options;
    options.snapshot = snapshot.snapshot_;
    std::string value;
    rocksdb::Status status = db_->Get(options, meta_, NumericKey(kChannelPrefix, channel_id), &value);
    if (status.IsNotFound()) {
        return std::optional<ChannelInfo>{};
    }
    if (!status.ok()) {
        return RocksToStatus(status, "read channel");
    }
    ChannelInfo channel;
    if (!channel.ParseFromString(value) || channel.channel_id() != channel_id) {
        return Status(grpc::StatusCode::INTERNAL, "invalid stored channel");
    }
    return std::optional<ChannelInfo>{std::move(channel)};
}

Result<std::vector<ChannelInfo>> UnifiedStorage::ListChannels(const ReadSnapshot& snapshot) const
{
    rocksdb::ReadOptions options;
    options.snapshot = snapshot.snapshot_;
    std::unique_ptr<rocksdb::Iterator> it(db_->NewIterator(options, meta_));
    std::vector<ChannelInfo> channels;
    for (it->Seek(kChannelPrefix); it->Valid() && it->key().starts_with(kChannelPrefix); it->Next()) {
        auto channel_id = DecodeNumericKey(it->key(), kChannelPrefix, "channel");
        if (!channel_id.ok()) {
            return channel_id.status();
        }
        ChannelInfo channel;
        if (!channel.ParseFromString(it->value().ToString()) || channel.channel_id() != channel_id.value()) {
            return Status(grpc::StatusCode::INTERNAL, "invalid stored channel");
        }
        channels.push_back(std::move(channel));
    }
    if (!it->status().ok()) {
        return RocksToStatus(it->status(), "list channels");
    }
    return channels;
}

Result<TokenBindingPage> UnifiedStorage::ListTokens(const ReadSnapshot& snapshot,
                                                    const std::string& start_after,
                                                    uint32_t limit) const
{
    rocksdb::ReadOptions options;
    options.snapshot = snapshot.snapshot_;
    std::unique_ptr<rocksdb::Iterator> it(db_->NewIterator(options, meta_));
    const std::string seek_key = std::string(kTokenPrefix) + start_after;
    it->Seek(seek_key);
    if (!start_after.empty() && it->Valid() && it->key().ToString() == seek_key) {
        it->Next();
    }

    TokenBindingPage page;
    for (; it->Valid() && it->key().starts_with(kTokenPrefix); it->Next()) {
        if (page.bindings.size() == limit) {
            page.has_more = true;
            break;
        }
        auto principal = DecodeRequiredUint64(it->value().ToString(), "token principal");
        if (!principal.ok()) {
            return principal.status();
        }
        TokenBindingRecord binding;
        binding.token = it->key().ToString().substr(std::strlen(kTokenPrefix));
        binding.principal = principal.value();
        page.bindings.push_back(std::move(binding));
    }
    if (!it->status().ok()) {
        return RocksToStatus(it->status(), "list tokens");
    }
    return page;
}

Result<uint64_t> UnifiedStorage::ScanMessages(const ReadSnapshot& snapshot,
                                              uint64_t from_seq,
                                              uint64_t last_seq,
                                              uint64_t max_records,
                                              const MessageVisitor& visitor) const
{
    rocksdb::ReadOptions options;
    options.snapshot = snapshot.snapshot_;
    std::unique_ptr<rocksdb::Iterator> it(db_->NewIterator(options, messages_));
    it->Seek(NumericKey(kMessagePrefix, from_seq));

    uint64_t scanned = 0;
    uint64_t next_seq = from_seq;
    for (; it->Valid() && it->key().starts_with(kMessagePrefix); it->Next()) {
        auto seq = DecodeNumericKey(it->key(), kMessagePrefix, "message");
        if (!seq.ok()) {
            return seq.status();
        }
        if (seq.value() > last_seq) {
            next_seq = last_seq + 1;
            break;
        }
        if (scanned >= max_records) {
            next_seq = seq.value();
            break;
        }

        EventMessage message;
        if (!message.ParseFromString(it->value().ToString()) || message.seq() != seq.value()) {
            return Status(grpc::StatusCode::INTERNAL, "invalid stored message");
        }
        auto action = visitor(message);
        if (!action.ok()) {
            return action.status();
        }
        if (action.value() == MessageScanAction::kStopBefore) {
            return seq.value();
        }

        ++scanned;
        next_seq = seq.value() + 1;
        if (action.value() == MessageScanAction::kStopAfter) {
            return next_seq;
        }
    }
    if (!it->status().ok()) {
        return RocksToStatus(it->status(), "scan messages");
    }
    if (!it->Valid() || !it->key().starts_with(kMessagePrefix)) {
        next_seq = last_seq + 1;
    }
    return next_seq;
}

Status UnifiedStorage::SetMaxSeq(rocksdb::WriteBatch* batch, uint64_t seq) const
{
    batch->Put(meta_, kMaxSeqKey, EncodeUint64(seq));
    return Status::Ok();
}

Status UnifiedStorage::SetNextChannelId(rocksdb::WriteBatch* batch, uint64_t channel_id) const
{
    batch->Put(meta_, kNextChannelIdKey, EncodeUint64(channel_id));
    return Status::Ok();
}

Status UnifiedStorage::PutMessage(rocksdb::WriteBatch* batch, const EventMessage& message) const
{
    std::string payload;
    if (!message.SerializeToString(&payload)) {
        return Status(grpc::StatusCode::INTERNAL, "serialize message failed");
    }
    batch->Put(messages_, NumericKey(kMessagePrefix, message.seq()), payload);
    return Status::Ok();
}

Status UnifiedStorage::PutChannel(rocksdb::WriteBatch* batch, const ChannelInfo& channel) const
{
    std::string payload;
    if (!channel.SerializeToString(&payload)) {
        return Status(grpc::StatusCode::INTERNAL, "serialize channel failed");
    }
    batch->Put(meta_, NumericKey(kChannelPrefix, channel.channel_id()), payload);
    return Status::Ok();
}

Status UnifiedStorage::PutToken(rocksdb::WriteBatch* batch, const std::string& token, uint64_t principal) const
{
    batch->Put(meta_, std::string(kTokenPrefix) + token, EncodeUint64(principal));
    return Status::Ok();
}

void UnifiedStorage::DeleteToken(rocksdb::WriteBatch* batch, const std::string& token) const
{
    batch->Delete(meta_, std::string(kTokenPrefix) + token);
}

Status UnifiedStorage::Validate() const
{
    auto snapshot_result = CreateSnapshot();
    if (!snapshot_result.ok()) {
        return snapshot_result.status();
    }
    ReadSnapshot snapshot = std::move(snapshot_result.value());

    rocksdb::ReadOptions options;
    options.snapshot = snapshot.snapshot_;
    std::string init_state;
    rocksdb::Status init_status = db_->Get(options, meta_, kInitStateKey, &init_state);
    if (!init_status.IsNotFound()) {
        if (!init_status.ok()) {
            return RocksToStatus(init_status, "read storage initialization marker");
        }
        return Status(grpc::StatusCode::INTERNAL, "storage initialization is incomplete");
    }

    auto schema = GetRequiredUint64(snapshot, kSchemaVersionKey);
    if (!schema.ok()) {
        return schema.status();
    }
    if (schema.value() != kSchemaVersion) {
        return Status(grpc::StatusCode::INTERNAL, "unsupported storage schema version");
    }
    auto max_seq = GetMaxSeq(snapshot);
    if (!max_seq.ok()) {
        return max_seq.status();
    }
    auto next_channel_id = GetNextChannelId(snapshot);
    if (!next_channel_id.ok()) {
        return next_channel_id.status();
    }

    std::unique_ptr<rocksdb::Iterator> it(db_->NewIterator(options, messages_));
    bool has_messages = false;
    uint64_t highest_seq = 0;
    for (it->Seek(kMessagePrefix); it->Valid() && it->key().starts_with(kMessagePrefix); it->Next()) {
        auto seq = DecodeNumericKey(it->key(), kMessagePrefix, "message");
        if (!seq.ok()) {
            return seq.status();
        }
        EventMessage message;
        if (!message.ParseFromString(it->value().ToString()) || message.seq() != seq.value()) {
            return Status(grpc::StatusCode::INTERNAL, "invalid stored message");
        }
        has_messages = true;
        highest_seq = seq.value();
    }
    if (!it->status().ok()) {
        return RocksToStatus(it->status(), "validate messages");
    }
    if ((!has_messages && max_seq.value() != 0) || (has_messages && highest_seq != max_seq.value())) {
        return Status(grpc::StatusCode::INTERNAL, "message watermark does not match stored messages");
    }
    return Status::Ok();
}

}  // namespace openevent

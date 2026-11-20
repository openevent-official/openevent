#include "storage/unified_storage.h"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstring>
#include <filesystem>
#include <limits>
#include <memory>
#include <set>
#include <utility>
#include <vector>

#include <google/protobuf/struct.pb.h>
#include <google/protobuf/util/json_util.h>
#include <rocksdb/options.h>

#include "common/object_limits.h"
#include "storage/encoding.h"
#include "storage/rocksdb_status.h"

namespace openevent {

using storage::internal::ObjectRecord;

namespace {

constexpr const char* kMessagesColumnFamily = "messages";
constexpr const char* kObjectsColumnFamily = "objects";
constexpr const char* kMaxSeqKey = "meta:max_seq";
constexpr const char* kNextUuidKey = "meta:next_uuid";
constexpr const char* kNextChannelIdKey = "meta:next_channel_id";
constexpr const char* kNextObjectIdKey = "meta:next_object_id";
constexpr const char* kChannelPrefix = "ch/";
constexpr const char* kTokenPrefix = "token/";
constexpr const char* kMessagePrefix = "msg/";
constexpr const char* kUuidPrefix = "uuid/";
constexpr const char* kPreparingObjectPrefix = "preparing/";
constexpr const char* kCommittedObjectPrefix = "object/";

EventMessage InitializationMessage()
{
    const auto event_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    EventMessage message;
    message.set_ts_ms(static_cast<uint64_t>(std::max<int64_t>(0, event_ms)));
    message.set_payload(R"({"kind":"system.initialization","data":{"schema_version":1}})");
    return message;
}

bool ValidInitializationMessage(const EventMessage& message)
{
    if (message.seq() != 0 || message.uuid() != 0 || message.principal() != 0 ||
        message.channel_id() != 0 || !message.recipients().empty() || !message.object_keys().empty()) {
        return false;
    }
    google::protobuf::Struct payload;
    if (!google::protobuf::util::JsonStringToMessage(message.payload(), &payload).ok()) {
        return false;
    }
    const auto& fields = payload.fields();
    auto kind = fields.find("kind");
    auto data = fields.find("data");
    if (kind == fields.end() || kind->second.kind_case() != google::protobuf::Value::kStringValue ||
        kind->second.string_value() != "system.initialization" || data == fields.end() ||
        data->second.kind_case() != google::protobuf::Value::kStructValue) {
        return false;
    }
    const auto& data_fields = data->second.struct_value().fields();
    auto schema = data_fields.find("schema_version");
    return schema != data_fields.end() &&
           schema->second.kind_case() == google::protobuf::Value::kNumberValue &&
           schema->second.number_value() == 1 &&
           !fields.contains("seq") && !fields.contains("channel_id") &&
           !fields.contains("principal") && !fields.contains("uuid");
}

bool ValidChannelSystemMessage(const EventMessage& message)
{
    if (!message.recipients().empty() || !message.object_keys().empty()) {
        return false;
    }
    google::protobuf::Struct payload;
    if (!google::protobuf::util::JsonStringToMessage(message.payload(), &payload).ok()) {
        return false;
    }
    const auto& fields = payload.fields();
    const auto kind = fields.find("kind");
    const auto data = fields.find("data");
    if (kind == fields.end() || kind->second.kind_case() != google::protobuf::Value::kStringValue ||
        data == fields.end() || data->second.kind_case() != google::protobuf::Value::kStructValue ||
        fields.contains("seq") || fields.contains("channel_id") || fields.contains("principal") ||
        fields.contains("uuid") || fields.contains("ts_ms")) {
        return false;
    }
    const auto& data_fields = data->second.struct_value().fields();
    const auto schema = data_fields.find("schema_version");
    if (schema == data_fields.end() ||
        schema->second.kind_case() != google::protobuf::Value::kNumberValue ||
        schema->second.number_value() != 1) {
        return false;
    }
    // Struct is used only to validate the JSON shape; the original payload is
    // returned unchanged, preserving uint64 principals above double precision.
    const auto is_principal = [](const google::protobuf::Value& value) {
        return value.kind_case() == google::protobuf::Value::kNumberValue &&
               value.number_value() > 0 && std::floor(value.number_value()) == value.number_value() &&
               value.number_value() <= static_cast<double>(std::numeric_limits<uint64_t>::max());
    };
    if (kind->second.string_value() == "system.member_added" ||
        kind->second.string_value() == "system.member_removed") {
        const auto member = data_fields.find("member");
        return member != data_fields.end() && is_principal(member->second);
    }
    if (kind->second.string_value() != "system.channel_initialized") {
        return false;
    }
    for (const char* field : {"name", "protocol", "description", "visibility"}) {
        const auto value = data_fields.find(field);
        if (value == data_fields.end() ||
            value->second.kind_case() != google::protobuf::Value::kStringValue) {
            return false;
        }
    }
    const auto& visibility = data_fields.at("visibility").string_value();
    if (visibility != "public" && visibility != "protected" && visibility != "private") {
        return false;
    }
    const auto creator = data_fields.find("creator");
    const auto members = data_fields.find("members");
    return creator != data_fields.end() && is_principal(creator->second) &&
           members != data_fields.end() &&
           members->second.kind_case() == google::protobuf::Value::kListValue &&
           !members->second.list_value().values().empty() &&
           std::all_of(members->second.list_value().values().begin(),
                       members->second.list_value().values().end(), is_principal);
}

std::filesystem::path DatabasePath(const std::string& root_path)
{
    return std::filesystem::path(root_path) / "db";
}

std::filesystem::path ObjectsPath(const std::string& root_path)
{
    return std::filesystem::path(root_path) / "objects";
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

std::string TokenKey(uint64_t principal, const std::string& token)
{
    return NumericKey(kTokenPrefix, principal) + token;
}

Result<uint64_t> DecodeRequiredUint64(const std::string& value, const std::string& description)
{
    if (value.size() != sizeof(uint64_t)) {
        return Status::StorageCorruption("decode " + description, "invalid " + description + " encoding");
    }
    return DecodeUint64(value);
}

Result<uint64_t> DecodeNumericKey(const rocksdb::Slice& key, const char* prefix, const std::string& description)
{
    const size_t prefix_size = std::strlen(prefix);
    if (key.size() != prefix_size + sizeof(uint64_t) ||
        std::memcmp(key.data(), prefix, prefix_size) != 0) {
        return Status::StorageCorruption("decode " + description, "invalid " + description + " key");
    }
    return DecodeUint64(std::string(key.data() + prefix_size, sizeof(uint64_t)));
}

bool IsNewStorageRoot(const std::filesystem::path& path)
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

Status ValidateStorageRootLayout(const std::filesystem::path& root)
{
    std::error_code ec;
    std::filesystem::directory_iterator it(root, ec);
    if (ec) {
        return Status(grpc::StatusCode::UNAVAILABLE,
                      "inspect storage root: " + ec.message());
    }

    std::set<std::string> entries;
    const std::filesystem::directory_iterator end;
    for (; it != end; it.increment(ec)) {
        if (ec) {
            return Status(grpc::StatusCode::UNAVAILABLE,
                          "inspect storage root: " + ec.message());
        }
        entries.insert(it->path().filename().string());
    }
    if (ec) {
        return Status(grpc::StatusCode::UNAVAILABLE,
                      "inspect storage root: " + ec.message());
    }

    const std::set<std::string> expected{"db", "objects"};
    if (entries != expected) {
        return Status(grpc::StatusCode::INTERNAL,
                      "storage root must contain only db and objects directories");
    }

    if (!std::filesystem::is_directory(root / "db", ec) || ec ||
        !std::filesystem::is_directory(root / "objects", ec) || ec) {
        return Status(grpc::StatusCode::INTERNAL,
                      "storage root must contain only db and objects directories");
    }
    return Status::Ok();
}

Status ValidateColumnFamilies(const std::filesystem::path& db_path)
{
    std::vector<std::string> column_families;
    auto status = rocksdb::DB::ListColumnFamilies(rocksdb::DBOptions(), db_path.string(), &column_families);
    if (!status.ok()) {
        return RocksToStatus(status, "list storage column families");
    }
    const std::set<std::string> names(column_families.begin(), column_families.end());
    const std::set<std::string> supported{
        rocksdb::kDefaultColumnFamilyName, kMessagesColumnFamily, kObjectsColumnFamily};
    if (names != supported) {
        return Status(grpc::StatusCode::INTERNAL, "unsupported storage column family layout");
    }
    return Status::Ok();
}

Result<std::string> SerializeObject(const ObjectRecord& record)
{
    std::string payload;
    if (!record.SerializeToString(&payload)) {
        return Status(grpc::StatusCode::INTERNAL, "serialize object metadata failed");
    }
    return payload;
}

bool IsBase64UrlToken(const std::string& token)
{
    return token.size() == kObjectTokenEncodedBytes &&
           std::all_of(token.begin(), token.end(), [](unsigned char ch) {
               return (ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z') ||
                      (ch >= '0' && ch <= '9') || ch == '-' || ch == '_';
           });
}

Result<ObjectRecord> ParseObject(const rocksdb::Slice& payload, uint64_t expected_id)
{
    ObjectRecord record;
    if (payload.size() > std::numeric_limits<int>::max() ||
        !record.ParseFromArray(payload.data(), static_cast<int>(payload.size())) ||
        record.object_id() != expected_id || expected_id == 0 ||
        !IsBase64UrlToken(record.object_token()) || record.creator_principal() == 0 || record.name().empty() ||
        record.name().size() > kMaxObjectNameBytes || record.type().empty() ||
        record.type().size() > kMaxObjectTypeBytes ||
        record.description().size() > kMaxObjectDescriptionBytes || record.nbytes() == 0 ||
        record.nbytes() > kMaxObjectBytes) {
        return Status::StorageCorruption("decode object metadata", "invalid stored object metadata");
    }
    return record;
}

UnifiedStorage::ObjectLookupResult DecodeObjectLookup(const rocksdb::Status& status,
                                                      const rocksdb::Slice& value,
                                                      uint64_t object_id)
{
    if (status.IsNotFound()) {
        return std::optional<ObjectRecord>{};
    }
    if (!status.ok()) {
        return RocksToStatus(status, "read object metadata");
    }
    auto object = ParseObject(value, object_id);
    if (!object.ok()) {
        return object.status();
    }
    return std::optional<ObjectRecord>{std::move(object.value())};
}

bool ValidStoredChannel(const ChannelInfo& channel)
{
    return channel.channel_id() != 0 && channel.has_creator() && channel.creator() != 0 &&
           !channel.name().empty() && channel.name().size() <= 255 &&
           channel.protocol().size() <= 255 && channel.description().size() <= 4096 &&
           Visibility_IsValid(channel.visibility()) &&
           std::find(channel.members().begin(), channel.members().end(), channel.creator()) !=
               channel.members().end() &&
           std::find(channel.members().begin(), channel.members().end(), uint64_t{0}) ==
               channel.members().end();
}

UnifiedStorage::ChannelLookupResult DecodeChannelLookup(const rocksdb::Status& status,
                                                         const rocksdb::Slice& value,
                                                         uint64_t channel_id)
{
    if (status.IsNotFound()) {
        return std::optional<ChannelInfo>{};
    }
    if (!status.ok()) {
        return RocksToStatus(status, "read channel");
    }
    ChannelInfo channel;
    if (value.size() > std::numeric_limits<int>::max() ||
        !channel.ParseFromArray(value.data(), static_cast<int>(value.size())) ||
        channel.channel_id() != channel_id || !ValidStoredChannel(channel)) {
        return Status::StorageCorruption("read channel", "invalid stored channel");
    }
    return std::optional<ChannelInfo>{std::move(channel)};
}

void RetainReadError(Status& selected, const Status& candidate)
{
    if (!candidate.ok() && selected.code() != grpc::StatusCode::DATA_LOSS &&
        (selected.ok() || candidate.code() == grpc::StatusCode::DATA_LOSS)) {
        selected = candidate;
    }
}

template <typename Decode, typename Visit>
void MultiGetById(
    rocksdb::DB& db, const rocksdb::Snapshot* snapshot, rocksdb::ColumnFamilyHandle* cf,
    const char* prefix, const std::vector<uint64_t>& ids, Decode decode, Visit visit)
{
    if (ids.empty()) {
        return;
    }
    std::vector<std::string> keys;
    keys.reserve(ids.size());
    for (uint64_t id : ids) {
        keys.push_back(NumericKey(prefix, id));
    }
    std::vector<rocksdb::Slice> key_slices(keys.begin(), keys.end());
    std::vector<rocksdb::PinnableSlice> values(ids.size());
    std::vector<rocksdb::Status> statuses(ids.size());
    rocksdb::ReadOptions options;
    options.snapshot = snapshot;
    db.MultiGet(options, cf, ids.size(), key_slices.data(), values.data(), statuses.data());

    for (size_t i = 0; i < ids.size(); ++i) {
        // Release this record's pin or owned buffer before decoding the next one.
        auto value = std::move(values[i]);
        visit(decode(statuses[i], value, ids[i]));
    }
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
                               rocksdb::ColumnFamilyHandle* messages,
                               rocksdb::ColumnFamilyHandle* objects,
                               StorageFaultInjector fault_injector)
    : db_(std::move(db)),
      meta_(meta),
      messages_(messages),
      objects_(objects),
      fault_injector_(std::move(fault_injector))
{
}

UnifiedStorage::~UnifiedStorage()
{
    object_files_.reset();
    if (db_ != nullptr) {
        if (objects_ != nullptr) {
            db_->DestroyColumnFamilyHandle(objects_);
        }
        if (messages_ != nullptr) {
            db_->DestroyColumnFamilyHandle(messages_);
        }
        if (meta_ != nullptr) {
            db_->DestroyColumnFamilyHandle(meta_);
        }
    }
}

Result<std::unique_ptr<UnifiedStorage>> UnifiedStorage::Open(
    const std::string& path,
    StorageFaultInjector fault_injector)
{
    if (path.empty()) {
        return Status(grpc::StatusCode::INVALID_ARGUMENT, "storage path must not be empty");
    }

    const bool create_new = IsNewStorageRoot(path);
    const auto db_path = DatabasePath(path);
    const auto objects_path = ObjectsPath(path);
    if (create_new) {
        for (const auto& directory : {db_path, objects_path}) {
            std::error_code ec;
            std::filesystem::create_directories(directory, ec);
            if (ec) {
                return Status(grpc::StatusCode::UNAVAILABLE,
                              "create storage directory " + directory.string() + ": " + ec.message());
            }
        }
    }

    Status layout_status = ValidateStorageRootLayout(path);
    if (!layout_status.ok()) {
        return layout_status;
    }

    if (!create_new) {
        Status status = ValidateColumnFamilies(db_path);
        if (!status.ok()) {
            return status;
        }
    }

    rocksdb::DBOptions options;
    options.wal_recovery_mode = rocksdb::WALRecoveryMode::kTolerateCorruptedTailRecords;
    options.create_if_missing = create_new;
    options.create_missing_column_families = create_new;
    options.error_if_exists = create_new;
    std::vector<rocksdb::ColumnFamilyDescriptor> descriptors{
        {rocksdb::kDefaultColumnFamilyName, rocksdb::ColumnFamilyOptions()},
        {kMessagesColumnFamily, rocksdb::ColumnFamilyOptions()},
        {kObjectsColumnFamily, rocksdb::ColumnFamilyOptions()},
    };
    std::vector<rocksdb::ColumnFamilyHandle*> handles;
    rocksdb::DB* raw_db = nullptr;
    rocksdb::Status open_status = rocksdb::DB::Open(
        options, db_path.string(), descriptors, &handles, &raw_db);
    if (!open_status.ok()) {
        return RocksToStatus(open_status, create_new ? "create storage" : "open unified storage");
    }

    auto storage = std::unique_ptr<UnifiedStorage>(new UnifiedStorage(
        std::unique_ptr<rocksdb::DB>(raw_db),
        handles[0],
        handles[1],
        handles[2],
        std::move(fault_injector)));
    if (create_new) {
        Status status = storage->WriteInitialState();
        if (!status.ok()) {
            return status;
        }
    }
    auto object_files = ObjectFileStore::Open(objects_path.string(), storage->fault_injector_);
    if (!object_files.ok()) {
        return object_files.status();
    }
    storage->object_files_ = std::move(object_files.value());
    Status validation = storage->Validate();
    if (!validation.ok()) {
        return validation;
    }
    Status recovery = storage->RecoverPreparingObjects();
    if (!recovery.ok()) {
        return recovery;
    }
    return storage;
}

Status UnifiedStorage::WriteInitialState()
{
    rocksdb::WriteBatch batch;
    batch.Put(kMaxSeqKey, EncodeUint64(uint64_t{0}));
    batch.Put(kNextUuidKey, EncodeUint64(uint64_t{1}));
    batch.Put(kNextChannelIdKey, EncodeUint64(uint64_t{1}));
    batch.Put(kNextObjectIdKey, EncodeUint64(uint64_t{1}));
    batch.Put(NumericKey(kUuidPrefix, 0), EncodeUint64(0));
    batch.Put(messages_, NumericKey(kMessagePrefix, 0), InitializationMessage().SerializeAsString());
    Status injected = InjectStorageFault(fault_injector_, StorageFaultPoint::kBeforeInitializationCommit);
    if (!injected.ok()) {
        return injected;
    }
    auto status = db_->Write(SynchronousWriteOptions(), &batch);
    if (!status.ok()) {
        return RocksToStatus(status, "finish storage initialization");
    }
    return InjectStorageFault(fault_injector_, StorageFaultPoint::kAfterInitializationCommit);
}

Result<ReadSnapshot> UnifiedStorage::CreateSnapshot() const
{
    const rocksdb::Snapshot* snapshot = db_->GetSnapshot();
    if (snapshot == nullptr) {
        return Status::Fatal(grpc::StatusCode::UNAVAILABLE, "create storage snapshot failed")
            .WithStorageDiagnostic("create storage snapshot", "RocksDB returned no snapshot");
    }
    return ReadSnapshot(this, snapshot);
}

void UnifiedStorage::ReleaseSnapshot(const rocksdb::Snapshot* snapshot) const
{
    db_->ReleaseSnapshot(snapshot);
}

Status UnifiedStorage::Commit(rocksdb::WriteBatch* batch)
{
    Status injected = InjectStorageFault(fault_injector_, StorageFaultPoint::kBeforeCommit);
    if (!injected.ok()) {
        return Status::Fatal(injected.code(), injected.message())
            .WithStorageDiagnostic("commit storage batch", "injected failure before write");
    }
    Status status = RocksToStatus(db_->Write(SynchronousWriteOptions(), batch), "commit storage batch");
    if (!status.ok()) {
        return status;
    }
    injected = InjectStorageFault(fault_injector_, StorageFaultPoint::kAfterCommit);
    return injected.ok() ? injected : Status::Fatal(injected.code(), injected.message())
        .WithStorageDiagnostic("commit storage batch", "injected failure after write");
}

Result<uint64_t> UnifiedStorage::GetRequiredUint64(const ReadSnapshot& snapshot, const std::string& key) const
{
    rocksdb::ReadOptions options;
    options.snapshot = snapshot.snapshot_;
    std::string value;
    rocksdb::Status status = db_->Get(options, meta_, key, &value);
    if (status.IsNotFound()) {
        return Status::StorageCorruption("read " + key, "missing required storage key: " + key);
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

Result<uint64_t> UnifiedStorage::GetNextUuid(const ReadSnapshot& snapshot) const
{
    return GetRequiredUint64(snapshot, kNextUuidKey);
}

Result<std::optional<uint64_t>> UnifiedStorage::GetUsedUuidSeq(const ReadSnapshot& snapshot,
                                                               uint64_t uuid) const
{
    rocksdb::ReadOptions options;
    options.snapshot = snapshot.snapshot_;
    std::string value;
    rocksdb::Status status = db_->Get(options, meta_, NumericKey(kUuidPrefix, uuid), &value);
    if (status.IsNotFound()) {
        return std::optional<uint64_t>{};
    }
    if (!status.ok()) {
        return RocksToStatus(status, "read used uuid");
    }
    auto seq = DecodeRequiredUint64(value, "used uuid");
    if (!seq.ok()) {
        return seq.status();
    }
    if ((uuid == 0) != (seq.value() == 0)) {
        return Status::StorageCorruption("read used uuid", "invalid stored UUID mapping");
    }
    return std::optional<uint64_t>{seq.value()};
}

Result<uint64_t> UnifiedStorage::GetNextChannelId(const ReadSnapshot& snapshot) const
{
    return GetRequiredUint64(snapshot, kNextChannelIdKey);
}

Result<uint64_t> UnifiedStorage::GetNextObjectId(const ReadSnapshot& snapshot) const
{
    return GetRequiredUint64(snapshot, kNextObjectIdKey);
}

Result<bool> UnifiedStorage::HasToken(const ReadSnapshot& snapshot, uint64_t principal,
                                     const std::string& token) const
{
    rocksdb::ReadOptions options;
    options.snapshot = snapshot.snapshot_;
    std::string value;
    rocksdb::Status status = db_->Get(options, meta_, TokenKey(principal, token), &value);
    if (status.IsNotFound()) {
        return false;
    }
    if (!status.ok()) {
        return RocksToStatus(status, "read credential");
    }
    if (!value.empty()) {
        return Status::StorageCorruption("read credential", "invalid stored credential");
    }
    return true;
}

UnifiedStorage::ChannelLookupResult UnifiedStorage::GetChannel(const ReadSnapshot& snapshot,
                                                              uint64_t channel_id) const
{
    rocksdb::ReadOptions options;
    options.snapshot = snapshot.snapshot_;
    rocksdb::PinnableSlice value;
    rocksdb::Status status = db_->Get(options, meta_, NumericKey(kChannelPrefix, channel_id), &value);
    return DecodeChannelLookup(status, value, channel_id);
}

Status UnifiedStorage::CheckChannels(
    const ReadSnapshot& snapshot, const std::vector<uint64_t>& channel_ids,
    const std::function<Status(const std::optional<ChannelInfo>&)>& check) const
{
    Status read_error;
    Status check_error;
    MultiGetById(*db_, snapshot.snapshot_, meta_, kChannelPrefix, channel_ids, DecodeChannelLookup,
                 [&](const ChannelLookupResult& result) {
                     if (!result.ok()) {
                         RetainReadError(read_error, result.status());
                     } else if (read_error.ok() && check_error.ok()) {
                         check_error = check(result.value());
                     }
                 });
    return read_error.ok() ? check_error : read_error;
}

Status UnifiedStorage::ScanChannels(const ReadSnapshot& snapshot,
                                    const std::function<void(ChannelInfo&&)>& visit) const
{
    rocksdb::ReadOptions options;
    options.snapshot = snapshot.snapshot_;
    std::unique_ptr<rocksdb::Iterator> it(db_->NewIterator(options, meta_));
    for (it->Seek(kChannelPrefix); it->Valid() && it->key().starts_with(kChannelPrefix); it->Next()) {
        auto channel_id = DecodeNumericKey(it->key(), kChannelPrefix, "channel");
        if (!channel_id.ok()) {
            return channel_id.status();
        }
        ChannelInfo channel;
        if (!channel.ParseFromString(it->value().ToString()) || channel.channel_id() != channel_id.value() ||
            !ValidStoredChannel(channel)) {
            return Status::StorageCorruption("list channels", "invalid stored channel");
        }
        visit(std::move(channel));
    }
    return RocksToStatus(it->status(), "list channels");
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
            break;
        }
        if (seq.value() != next_seq) {
            return Status::StorageCorruption("scan messages",
                          "missing stored message at seq " + std::to_string(next_seq));
        }
        if (scanned >= max_records) {
            break;
        }

        const rocksdb::Slice value = it->value();
        EventMessage message;
        if (value.size() > std::numeric_limits<int>::max() ||
            !message.ParseFromArray(value.data(), static_cast<int>(value.size())) ||
            message.seq() != seq.value()) {
            return Status::StorageCorruption("scan messages", "invalid stored message");
        }
        if (seq.value() == 0 ? !ValidInitializationMessage(message)
                             : (message.uuid() == 0 ||
                                message.channel_id() == 0 ||
                                (message.principal() == 0 && !ValidChannelSystemMessage(message)) ||
                                std::find(message.recipients().begin(), message.recipients().end(), 0) !=
                                    message.recipients().end())) {
            return Status::StorageCorruption("scan messages", "invalid stored message reserved fields");
        }
        if (message.object_keys_size() > static_cast<int>(kMaxObjectKeys) ||
            std::any_of(message.object_keys().begin(),
                        message.object_keys().end(),
                        [](const ObjectKey& object_key) {
                            return object_key.object_id() == 0 ||
                                   !IsBase64UrlToken(object_key.object_token());
                        })) {
            return Status::StorageCorruption("scan messages", "invalid stored message object reference");
        }
        auto action = visitor(std::move(message));
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
    if (scanned < max_records && next_seq <= last_seq &&
        (!it->Valid() || !it->key().starts_with(kMessagePrefix) ||
         DecodeUint64(it->key().ToString().substr(std::strlen(kMessagePrefix))) > last_seq)) {
        return Status::StorageCorruption("scan messages",
                      "missing stored message at seq " + std::to_string(next_seq));
    }
    return next_seq;
}

Result<std::optional<ObjectRecord>> UnifiedStorage::GetObject(const ReadSnapshot& snapshot,
                                                              const char* prefix,
                                                              uint64_t object_id) const
{
    rocksdb::ReadOptions options;
    options.snapshot = snapshot.snapshot_;
    rocksdb::PinnableSlice value;
    rocksdb::Status status = db_->Get(options, objects_, NumericKey(prefix, object_id), &value);
    return DecodeObjectLookup(status, value, object_id);
}

Result<std::optional<ObjectRecord>> UnifiedStorage::GetPreparingObject(const ReadSnapshot& snapshot,
                                                                       uint64_t object_id) const
{
    return GetObject(snapshot, kPreparingObjectPrefix, object_id);
}

Result<std::optional<ObjectRecord>> UnifiedStorage::GetCommittedObject(const ReadSnapshot& snapshot,
                                                                       uint64_t object_id) const
{
    return GetObject(snapshot, kCommittedObjectPrefix, object_id);
}

std::vector<UnifiedStorage::ObjectLookupResult> UnifiedStorage::GetCommittedObjects(
    const ReadSnapshot& snapshot,
    const std::vector<uint64_t>& object_ids) const
{
    std::vector<ObjectLookupResult> results;
    results.reserve(object_ids.size());
    MultiGetById(*db_, snapshot.snapshot_, objects_, kCommittedObjectPrefix, object_ids, DecodeObjectLookup,
                 [&](ObjectLookupResult&& result) { results.push_back(std::move(result)); });
    return results;
}

Status UnifiedStorage::MostSevereReadError(const std::vector<ObjectLookupResult>& results)
{
    Status selected;
    for (const auto& result : results) {
        RetainReadError(selected, result.status());
    }
    return selected;
}

Status UnifiedStorage::SetMaxSeq(rocksdb::WriteBatch* batch, uint64_t seq) const
{
    batch->Put(meta_, kMaxSeqKey, EncodeUint64(seq));
    return Status::Ok();
}

Status UnifiedStorage::SetNextUuid(rocksdb::WriteBatch* batch, uint64_t uuid) const
{
    if (uuid == 0) {
        return Status(grpc::StatusCode::INVALID_ARGUMENT, "next uuid must be nonzero");
    }
    batch->Put(meta_, kNextUuidKey, EncodeUint64(uuid));
    return Status::Ok();
}

Status UnifiedStorage::PutUsedUuid(rocksdb::WriteBatch* batch, uint64_t uuid, uint64_t seq) const
{
    if (uuid == 0 || seq == 0) {
        return Status(grpc::StatusCode::INVALID_ARGUMENT, "used uuid and seq must be nonzero");
    }
    batch->Put(meta_, NumericKey(kUuidPrefix, uuid), EncodeUint64(seq));
    return Status::Ok();
}

Status UnifiedStorage::SetNextChannelId(rocksdb::WriteBatch* batch, uint64_t channel_id) const
{
    batch->Put(meta_, kNextChannelIdKey, EncodeUint64(channel_id));
    return Status::Ok();
}

Status UnifiedStorage::SetNextObjectId(rocksdb::WriteBatch* batch, uint64_t object_id) const
{
    batch->Put(meta_, kNextObjectIdKey, EncodeUint64(object_id));
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

Status UnifiedStorage::PutToken(rocksdb::WriteBatch* batch, uint64_t principal, const std::string& token) const
{
    batch->Put(meta_, TokenKey(principal, token), rocksdb::Slice());
    return Status::Ok();
}

void UnifiedStorage::DeleteToken(rocksdb::WriteBatch* batch, uint64_t principal, const std::string& token) const
{
    batch->Delete(meta_, TokenKey(principal, token));
}

Status UnifiedStorage::PutPreparingObject(rocksdb::WriteBatch* batch, const ObjectRecord& object) const
{
    auto payload = SerializeObject(object);
    if (!payload.ok()) {
        return payload.status();
    }
    batch->Put(objects_, NumericKey(kPreparingObjectPrefix, object.object_id()), payload.value());
    return Status::Ok();
}

Status UnifiedStorage::PutCommittedObject(rocksdb::WriteBatch* batch, const ObjectRecord& object) const
{
    auto payload = SerializeObject(object);
    if (!payload.ok()) {
        return payload.status();
    }
    batch->Delete(objects_, NumericKey(kPreparingObjectPrefix, object.object_id()));
    batch->Put(objects_, NumericKey(kCommittedObjectPrefix, object.object_id()), payload.value());
    return Status::Ok();
}

void UnifiedStorage::DeletePreparingObject(rocksdb::WriteBatch* batch, uint64_t object_id) const
{
    batch->Delete(objects_, NumericKey(kPreparingObjectPrefix, object_id));
}

Status UnifiedStorage::WriteObjectFile(uint64_t object_id, const std::string& data) const
{
    return object_files_->Write(object_id, data);
}

Status UnifiedStorage::CleanupObjectFile(uint64_t object_id) const
{
    return object_files_->Cleanup(object_id);
}

Result<std::string> UnifiedStorage::ReadObjectFile(const ObjectRecord& object,
                                                   uint64_t offset,
                                                   uint64_t nbytes) const
{
    return object_files_->ReadRange(object.object_id(), object.nbytes(), offset, nbytes);
}

Result<std::vector<uint64_t>> UnifiedStorage::ListPreparingObjectIds() const
{
    std::unique_ptr<rocksdb::Iterator> it(db_->NewIterator(rocksdb::ReadOptions(), objects_));
    std::vector<uint64_t> ids;
    for (it->Seek(kPreparingObjectPrefix);
         it->Valid() && it->key().starts_with(kPreparingObjectPrefix);
         it->Next()) {
        auto object_id = DecodeNumericKey(it->key(), kPreparingObjectPrefix, "preparing object");
        if (!object_id.ok()) {
            return object_id.status();
        }
        auto object = ParseObject(it->value(), object_id.value());
        if (!object.ok()) {
            return object.status();
        }
        ids.push_back(object_id.value());
    }
    if (!it->status().ok()) {
        return RocksToStatus(it->status(), "list preparing objects");
    }
    return ids;
}

Status UnifiedStorage::RecoverPreparingObjects()
{
    auto ids = ListPreparingObjectIds();
    if (!ids.ok()) {
        return ids.status();
    }
    const auto& object_ids = ids.value();
    if (object_ids.empty()) {
        return Status::Ok();
    }
    {
        auto snapshot_result = CreateSnapshot();
        if (!snapshot_result.ok()) {
            return snapshot_result.status();
        }
        ReadSnapshot snapshot = std::move(snapshot_result.value());
        for (uint64_t object_id : object_ids) {
            auto committed = GetCommittedObject(snapshot, object_id);
            if (!committed.ok()) {
                return committed.status();
            }
            if (committed.value().has_value()) {
                return Status::StorageCorruption("recover preparing objects",
                              "object ID has both PREPARING and COMMITTED metadata");
            }
        }
    }
    Status file_cleanup = object_files_->Cleanup(object_ids);
    if (!file_cleanup.ok()) {
        return file_cleanup;
    }
    rocksdb::WriteBatch batch;
    for (uint64_t object_id : object_ids) {
        DeletePreparingObject(&batch, object_id);
    }
    return Commit(&batch);
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
    auto max_seq = GetMaxSeq(snapshot);
    if (!max_seq.ok()) {
        return max_seq.status();
    }
    auto next_uuid = GetNextUuid(snapshot);
    if (!next_uuid.ok() || next_uuid.value() == 0) {
        return next_uuid.ok()
                   ? Status::StorageCorruption("validate storage", "invalid next uuid")
                   : next_uuid.status();
    }
    auto next_channel_id = GetNextChannelId(snapshot);
    if (!next_channel_id.ok() || next_channel_id.value() == 0) {
        return next_channel_id.ok()
                   ? Status::StorageCorruption("validate storage", "invalid next channel ID")
                   : next_channel_id.status();
    }
    auto next_object_id = GetNextObjectId(snapshot);
    if (!next_object_id.ok() || next_object_id.value() == 0) {
        return next_object_id.ok()
                   ? Status::StorageCorruption("validate storage", "invalid next object ID")
                   : next_object_id.status();
    }

    std::string encoded_message;
    const auto message_status = db_->Get(options, messages_, NumericKey(kMessagePrefix, 0), &encoded_message);
    if (message_status.IsNotFound()) {
        return Status::StorageCorruption("validate storage", "missing system initialization message");
    }
    if (!message_status.ok()) {
        return RocksToStatus(message_status, "read system initialization message");
    }
    EventMessage message;
    if (!message.ParseFromString(encoded_message) || !ValidInitializationMessage(message)) {
        return Status::StorageCorruption("validate storage", "invalid system initialization message");
    }
    auto system_seq = GetUsedUuidSeq(snapshot, 0);
    if (!system_seq.ok()) {
        return system_seq.status();
    }
    if (!system_seq.value().has_value() || system_seq.value().value() != 0) {
        return Status::StorageCorruption("validate storage", "missing or conflicting system UUID mapping");
    }

    return Status::Ok();
}

}  // namespace openevent

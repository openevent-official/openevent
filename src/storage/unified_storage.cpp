#include "storage/unified_storage.h"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstring>
#include <filesystem>
#include <memory>
#include <set>
#include <utility>
#include <vector>

#include <fcntl.h>
#include <google/protobuf/struct.pb.h>
#include <google/protobuf/util/json_util.h>
#include <rocksdb/options.h>
#include <unistd.h>

#include "object_record.pb.h"
#include "common/object_limits.h"
#include "storage/encoding.h"

namespace openevent {
namespace {

constexpr uint64_t kSchemaVersion = 5;
constexpr uint64_t kInitializing = 1;
constexpr const char* kMessagesColumnFamily = "messages";
constexpr const char* kObjectsColumnFamily = "objects";
constexpr const char* kSchemaVersionKey = "meta:schema_version";
constexpr const char* kInitStateKey = "meta:init_state";
constexpr const char* kMaxSeqKey = "meta:max_seq";
constexpr const char* kNextUuidKey = "meta:next_uuid";
constexpr const char* kNextChannelIdKey = "meta:next_channel_id";
constexpr const char* kNextObjectIdKey = "meta:next_object_id";
constexpr const char* kChannelPrefix = "ch/";
constexpr const char* kTokenPrefix = "token:";
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
    message.set_payload("{\"kind\":\"system.initialization\",\"data\":{\"schema_version\":1},"
                        "\"timestamps\":{\"event_ms\":" + std::to_string(message.ts_ms()) + "}}");
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
    auto timestamps = fields.find("timestamps");
    if (kind == fields.end() || kind->second.kind_case() != google::protobuf::Value::kStringValue ||
        kind->second.string_value() != "system.initialization" || data == fields.end() ||
        data->second.kind_case() != google::protobuf::Value::kStructValue || timestamps == fields.end() ||
        timestamps->second.kind_case() != google::protobuf::Value::kStructValue) {
        return false;
    }
    const auto& data_fields = data->second.struct_value().fields();
    const auto& time_fields = timestamps->second.struct_value().fields();
    auto schema = data_fields.find("schema_version");
    auto event_ms = time_fields.find("event_ms");
    return schema != data_fields.end() &&
           schema->second.kind_case() == google::protobuf::Value::kNumberValue &&
           schema->second.number_value() == 1 && event_ms != time_fields.end() &&
           event_ms->second.kind_case() == google::protobuf::Value::kNumberValue &&
           event_ms->second.number_value() >= 0 &&
           std::floor(event_ms->second.number_value()) == event_ms->second.number_value() &&
           event_ms->second.number_value() == static_cast<double>(message.ts_ms()) &&
           !fields.contains("seq") && !fields.contains("channel_id") &&
           !fields.contains("principal") && !fields.contains("uuid");
}

std::filesystem::path DatabasePath(const std::string& root_path)
{
    return std::filesystem::path(root_path) / "db";
}

std::filesystem::path ObjectsPath(const std::string& root_path)
{
    return std::filesystem::path(root_path) / "objects";
}

Status RocksToStatus(const rocksdb::Status& status, const std::string& prefix)
{
    if (status.ok()) {
        return Status::Ok();
    }
    if (status.IsCorruption()) {
        return Status::Fatal(grpc::StatusCode::DATA_LOSS, prefix + ": " + status.ToString());
    }
    return Status::Fatal(grpc::StatusCode::UNAVAILABLE, prefix + ": " + status.ToString());
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
        return Status(grpc::StatusCode::DATA_LOSS, "invalid " + description + " encoding");
    }
    return DecodeUint64(value);
}

Result<uint64_t> DecodeNumericKey(const rocksdb::Slice& key, const char* prefix, const std::string& description)
{
    const size_t prefix_size = std::strlen(prefix);
    if (key.size() != prefix_size + sizeof(uint64_t) ||
        std::memcmp(key.data(), prefix, prefix_size) != 0) {
        return Status(grpc::StatusCode::DATA_LOSS, "invalid " + description + " key");
    }
    return DecodeUint64(std::string(key.data() + prefix_size, sizeof(uint64_t)));
}

bool IsEmptyDirectory(const std::filesystem::path& path)
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

bool IsNewStorageRoot(const std::string& path)
{
    return IsEmptyDirectory(path);
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

Status SyncDirectory(const std::filesystem::path& path)
{
    const int fd = ::open(path.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW);
    if (fd < 0) {
        return Status(grpc::StatusCode::UNAVAILABLE,
                      "open directory for fsync: " + std::string(std::strerror(errno)));
    }
    if (::fsync(fd) != 0) {
        const int error = errno;
        ::close(fd);
        return Status(grpc::StatusCode::UNAVAILABLE,
                      "fsync directory: " + std::string(std::strerror(error)));
    }
    if (::close(fd) != 0) {
        return Status(grpc::StatusCode::UNAVAILABLE,
                      "close directory after fsync: " + std::string(std::strerror(errno)));
    }
    return Status::Ok();
}

Status CreateDirectoryAndSyncParent(const std::filesystem::path& path)
{
    std::error_code ec;
    const bool created = std::filesystem::create_directory(path, ec);
    if (ec) {
        return Status(grpc::StatusCode::UNAVAILABLE,
                      "create storage directory " + path.string() + ": " + ec.message());
    }
    if (!created && (!std::filesystem::is_directory(path, ec) || ec)) {
        return Status(grpc::StatusCode::UNAVAILABLE,
                      "storage path component is not a directory: " + path.string());
    }
    return created ? SyncDirectory(path.parent_path()) : Status::Ok();
}

Status CreateDirectoryTreeDurably(const std::filesystem::path& root)
{
    std::vector<std::filesystem::path> missing;
    std::filesystem::path current = root;
    std::error_code ec;
    while (!std::filesystem::exists(current, ec)) {
        if (ec) {
            return Status(grpc::StatusCode::UNAVAILABLE,
                          "inspect storage directory " + current.string() + ": " + ec.message());
        }
        missing.push_back(current);
        const std::filesystem::path parent = current.parent_path();
        if (parent.empty() || parent == current) {
            return Status(grpc::StatusCode::UNAVAILABLE,
                          "storage path has no existing directory ancestor: " + root.string());
        }
        current = parent;
    }
    if (ec || !std::filesystem::is_directory(current, ec) || ec) {
        return Status(grpc::StatusCode::UNAVAILABLE,
                      "storage path ancestor is not a directory: " + current.string());
    }

    for (auto it = missing.rbegin(); it != missing.rend(); ++it) {
        Status status = CreateDirectoryAndSyncParent(*it);
        if (!status.ok()) {
            return status;
        }
    }
    return Status::Ok();
}

Status CreateStorageDirectories(const std::string& root_path)
{
    std::error_code ec;
    const std::filesystem::path root = std::filesystem::absolute(root_path, ec);
    if (ec) {
        return Status(grpc::StatusCode::UNAVAILABLE, "resolve storage root: " + ec.message());
    }
    Status root_status = CreateDirectoryTreeDurably(root);
    if (!root_status.ok()) {
        return root_status;
    }
    Status db_status = CreateDirectoryAndSyncParent(root / "db");
    if (!db_status.ok()) {
        return db_status;
    }
    Status objects_status = CreateDirectoryAndSyncParent(root / "objects");
    if (!objects_status.ok()) {
        return objects_status;
    }
    return Status::Ok();
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

Result<std::string> SerializeObject(const StoredObject& object)
{
    storage::internal::ObjectRecord record;
    record.set_object_id(object.object_id);
    record.set_object_token(object.object_token);
    record.set_creator_principal(object.creator_principal);
    record.set_name(object.name);
    record.set_type(object.type);
    record.set_description(object.description);
    record.set_nbytes(object.nbytes);
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

Result<StoredObject> ParseObject(const std::string& payload, uint64_t expected_id)
{
    storage::internal::ObjectRecord record;
    if (!record.ParseFromString(payload) || record.object_id() != expected_id || expected_id == 0 ||
        !IsBase64UrlToken(record.object_token()) || record.creator_principal() == 0 || record.name().empty() ||
        record.name().size() > kMaxObjectNameBytes || record.type().empty() ||
        record.type().size() > kMaxObjectTypeBytes ||
        record.description().size() > kMaxObjectDescriptionBytes || record.nbytes() == 0 ||
        record.nbytes() > kMaxObjectBytes) {
        return Status(grpc::StatusCode::DATA_LOSS, "invalid stored object metadata");
    }
    StoredObject object;
    object.object_id = record.object_id();
    object.object_token = record.object_token();
    object.creator_principal = record.creator_principal();
    object.name = record.name();
    object.type = record.type();
    object.description = record.description();
    object.nbytes = record.nbytes();
    return object;
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
                               std::unique_ptr<ObjectFileStore> object_files,
                               StorageFaultInjector fault_injector)
    : db_(std::move(db)),
      meta_(meta),
      messages_(messages),
      objects_(objects),
      object_files_(std::move(object_files)),
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

    if (IsNewStorageRoot(path)) {
        Status status = InitializeNew(path, fault_injector);
        if (!status.ok()) {
            return status;
        }
    }

    Status layout_status = ValidateStorageRootLayout(path);
    if (!layout_status.ok()) {
        return layout_status;
    }

    const std::filesystem::path db_path = DatabasePath(path);
    std::vector<std::string> column_families;
    rocksdb::Options options;
    rocksdb::Status list_status = rocksdb::DB::ListColumnFamilies(options, db_path.string(), &column_families);
    if (!list_status.ok()) {
        return RocksToStatus(list_status, "list storage column families");
    }
    const std::set<std::string> names(column_families.begin(), column_families.end());
    const std::set<std::string> supported{
        rocksdb::kDefaultColumnFamilyName, kMessagesColumnFamily, kObjectsColumnFamily};
    if (names != supported) {
        return Status(grpc::StatusCode::INTERNAL, "unsupported storage column family layout");
    }
    return OpenExisting(path, std::move(fault_injector));
}

Status UnifiedStorage::InitializeNew(const std::string& path, const StorageFaultInjector& fault_injector)
{
    Status directory_status = CreateStorageDirectories(path);
    if (!directory_status.ok()) {
        return directory_status;
    }

    const std::string db_path = DatabasePath(path).string();
    rocksdb::Options options;
    options.create_if_missing = true;
    rocksdb::DB* raw_db = nullptr;
    rocksdb::Status open_status = rocksdb::DB::Open(options, db_path, &raw_db);
    if (!open_status.ok()) {
        return RocksToStatus(open_status, "create storage");
    }
    std::unique_ptr<rocksdb::DB> db(raw_db);

    rocksdb::Status marker_status = db->Put(SynchronousWriteOptions(), kInitStateKey, EncodeUint64(kInitializing));
    if (!marker_status.ok()) {
        return RocksToStatus(marker_status, "write storage initialization marker");
    }
    Status injected = InjectStorageFault(fault_injector, StorageFaultPoint::kAfterInitializationMarker);
    if (!injected.ok()) {
        return injected;
    }

    rocksdb::ColumnFamilyHandle* messages = nullptr;
    rocksdb::Status status =
        db->CreateColumnFamily(rocksdb::ColumnFamilyOptions(), kMessagesColumnFamily, &messages);
    if (!status.ok()) {
        return RocksToStatus(status, "create messages column family");
    }
    rocksdb::ColumnFamilyHandle* objects = nullptr;
    status = db->CreateColumnFamily(rocksdb::ColumnFamilyOptions(), kObjectsColumnFamily, &objects);
    if (!status.ok()) {
        db->DestroyColumnFamilyHandle(messages);
        return RocksToStatus(status, "create objects column family");
    }

    rocksdb::WriteBatch batch;
    batch.Put(kSchemaVersionKey, EncodeUint64(kSchemaVersion));
    batch.Put(kMaxSeqKey, EncodeUint64(uint64_t{0}));
    batch.Put(kNextUuidKey, EncodeUint64(uint64_t{1}));
    batch.Put(kNextChannelIdKey, EncodeUint64(uint64_t{1}));
    batch.Put(kNextObjectIdKey, EncodeUint64(uint64_t{1}));
    batch.Put(NumericKey(kUuidPrefix, 0), EncodeUint64(0));
    batch.Put(messages, NumericKey(kMessagePrefix, 0), InitializationMessage().SerializeAsString());
    batch.Delete(kInitStateKey);
    injected = InjectStorageFault(fault_injector, StorageFaultPoint::kBeforeInitializationCommit);
    Status init_status = injected.ok()
                             ? RocksToStatus(db->Write(SynchronousWriteOptions(), &batch),
                                             "finish storage initialization")
                             : injected;
    if (init_status.ok()) {
        // The initialization marker is durably gone. Initialization is complete.
        init_status = InjectStorageFault(fault_injector, StorageFaultPoint::kAfterInitializationCommit);
    }
    rocksdb::Status objects_destroy = db->DestroyColumnFamilyHandle(objects);
    rocksdb::Status messages_destroy = db->DestroyColumnFamilyHandle(messages);
    if (!init_status.ok()) {
        return init_status;
    }
    if (!objects_destroy.ok()) {
        return RocksToStatus(objects_destroy, "destroy objects column family handle");
    }
    if (!messages_destroy.ok()) {
        return RocksToStatus(messages_destroy, "destroy messages column family handle");
    }
    return Status::Ok();
}

Result<std::unique_ptr<UnifiedStorage>> UnifiedStorage::OpenExisting(
    const std::string& root_path,
    StorageFaultInjector fault_injector)
{
    rocksdb::DBOptions options;
    std::vector<rocksdb::ColumnFamilyDescriptor> descriptors{
        {rocksdb::kDefaultColumnFamilyName, rocksdb::ColumnFamilyOptions()},
        {kMessagesColumnFamily, rocksdb::ColumnFamilyOptions()},
        {kObjectsColumnFamily, rocksdb::ColumnFamilyOptions()},
    };
    std::vector<rocksdb::ColumnFamilyHandle*> handles;
    rocksdb::DB* raw_db = nullptr;
    rocksdb::Status open_status = rocksdb::DB::Open(
        options, DatabasePath(root_path).string(), descriptors, &handles, &raw_db);
    if (!open_status.ok()) {
        return RocksToStatus(open_status, "open unified storage");
    }
    if (handles.size() != 3) {
        std::unique_ptr<rocksdb::DB> db(raw_db);
        DestroyHandles(db.get(), &handles);
        return Status(grpc::StatusCode::INTERNAL, "unified storage column family count mismatch");
    }

    auto object_files = ObjectFileStore::Open(ObjectsPath(root_path).string(), fault_injector);
    if (!object_files.ok()) {
        std::unique_ptr<rocksdb::DB> db(raw_db);
        DestroyHandles(db.get(), &handles);
        return object_files.status();
    }
    auto storage = std::unique_ptr<UnifiedStorage>(new UnifiedStorage(
        std::unique_ptr<rocksdb::DB>(raw_db),
        handles[0],
        handles[1],
        handles[2],
        std::move(object_files.value()),
        std::move(fault_injector)));
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

Result<ReadSnapshot> UnifiedStorage::CreateSnapshot() const
{
    const rocksdb::Snapshot* snapshot = db_->GetSnapshot();
    if (snapshot == nullptr) {
        return Status::Fatal(grpc::StatusCode::UNAVAILABLE, "create storage snapshot failed");
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
        return Status::Fatal(injected.code(), injected.message());
    }
    Status status = RocksToStatus(db_->Write(SynchronousWriteOptions(), batch), "commit storage batch");
    if (!status.ok()) {
        return status;
    }
    injected = InjectStorageFault(fault_injector_, StorageFaultPoint::kAfterCommit);
    return injected.ok() ? injected : Status::Fatal(injected.code(), injected.message());
}

Result<uint64_t> UnifiedStorage::GetRequiredUint64(const ReadSnapshot& snapshot, const std::string& key) const
{
    rocksdb::ReadOptions options;
    options.snapshot = snapshot.snapshot_;
    std::string value;
    rocksdb::Status status = db_->Get(options, meta_, key, &value);
    if (status.IsNotFound()) {
        return Status(grpc::StatusCode::DATA_LOSS, "missing required storage key: " + key);
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
        return Status(grpc::StatusCode::DATA_LOSS, "invalid stored UUID mapping");
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
    if (principal.value() == 0) {
        return Status(grpc::StatusCode::DATA_LOSS, "invalid stored token principal");
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
    if (!channel.ParseFromString(value) || channel.channel_id() != channel_id || !ValidStoredChannel(channel)) {
        return Status(grpc::StatusCode::DATA_LOSS, "invalid stored channel");
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
        if (!channel.ParseFromString(it->value().ToString()) || channel.channel_id() != channel_id.value() ||
            !ValidStoredChannel(channel)) {
            return Status(grpc::StatusCode::DATA_LOSS, "invalid stored channel");
        }
        channels.push_back(std::move(channel));
    }
    if (!it->status().ok()) {
        return RocksToStatus(it->status(), "list channels");
    }
    return channels;
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
            return Status(grpc::StatusCode::DATA_LOSS,
                          "missing stored message at seq " + std::to_string(next_seq));
        }
        if (scanned >= max_records) {
            break;
        }

        EventMessage message;
        if (!message.ParseFromString(it->value().ToString()) || message.seq() != seq.value()) {
            return Status(grpc::StatusCode::DATA_LOSS, "invalid stored message");
        }
        if (seq.value() == 0 ? !ValidInitializationMessage(message)
                             : (message.uuid() == 0 || message.principal() == 0 ||
                                message.channel_id() == 0 ||
                                std::find(message.recipients().begin(), message.recipients().end(), 0) !=
                                    message.recipients().end())) {
            return Status(grpc::StatusCode::DATA_LOSS, "invalid stored message reserved fields");
        }
        if (message.object_keys_size() > static_cast<int>(kMaxObjectKeys) ||
            std::any_of(message.object_keys().begin(),
                        message.object_keys().end(),
                        [](const ObjectKey& object_key) {
                            return object_key.object_id() == 0 ||
                                   !IsBase64UrlToken(object_key.object_token());
                        })) {
            return Status(grpc::StatusCode::DATA_LOSS, "invalid stored message object reference");
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
    if (scanned < max_records && next_seq <= last_seq &&
        (!it->Valid() || !it->key().starts_with(kMessagePrefix) ||
         DecodeUint64(it->key().ToString().substr(std::strlen(kMessagePrefix))) > last_seq)) {
        return Status(grpc::StatusCode::DATA_LOSS,
                      "missing stored message at seq " + std::to_string(next_seq));
    }
    return next_seq;
}

Result<std::optional<StoredObject>> UnifiedStorage::GetObject(const ReadSnapshot& snapshot,
                                                              const char* prefix,
                                                              uint64_t object_id) const
{
    rocksdb::ReadOptions options;
    options.snapshot = snapshot.snapshot_;
    std::string value;
    rocksdb::Status status = db_->Get(options, objects_, NumericKey(prefix, object_id), &value);
    if (status.IsNotFound()) {
        return std::optional<StoredObject>{};
    }
    if (!status.ok()) {
        return RocksToStatus(status, "read object metadata");
    }
    auto object = ParseObject(value, object_id);
    if (!object.ok()) {
        return object.status();
    }
    return std::optional<StoredObject>{std::move(object.value())};
}

Result<std::optional<StoredObject>> UnifiedStorage::GetPreparingObject(const ReadSnapshot& snapshot,
                                                                       uint64_t object_id) const
{
    return GetObject(snapshot, kPreparingObjectPrefix, object_id);
}

Result<std::optional<StoredObject>> UnifiedStorage::GetCommittedObject(const ReadSnapshot& snapshot,
                                                                       uint64_t object_id) const
{
    return GetObject(snapshot, kCommittedObjectPrefix, object_id);
}

Result<std::vector<std::optional<StoredObject>>> UnifiedStorage::GetCommittedObjects(
    const ReadSnapshot& snapshot,
    const std::vector<uint64_t>& object_ids) const
{
    if (object_ids.empty()) {
        return std::vector<std::optional<StoredObject>>{};
    }

    std::vector<std::string> keys;
    keys.reserve(object_ids.size());
    for (uint64_t object_id : object_ids) {
        keys.push_back(NumericKey(kCommittedObjectPrefix, object_id));
    }

    std::vector<rocksdb::Slice> key_slices;
    key_slices.reserve(keys.size());
    for (const auto& key : keys) {
        key_slices.emplace_back(key);
    }

    rocksdb::ReadOptions options;
    options.snapshot = snapshot.snapshot_;
    std::vector<rocksdb::ColumnFamilyHandle*> column_families(object_ids.size(), objects_);
    std::vector<std::string> values;
    std::vector<rocksdb::Status> statuses =
        db_->MultiGet(options, column_families, key_slices, &values);

    std::vector<std::optional<StoredObject>> objects;
    objects.reserve(object_ids.size());
    for (size_t i = 0; i < object_ids.size(); ++i) {
        if (statuses[i].IsNotFound()) {
            objects.emplace_back(std::nullopt);
            continue;
        }
        if (!statuses[i].ok()) {
            return RocksToStatus(statuses[i], "read object metadata");
        }
        auto object = ParseObject(values[i], object_ids[i]);
        if (!object.ok()) {
            return object.status();
        }
        objects.emplace_back(std::move(object.value()));
    }
    return objects;
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

Status UnifiedStorage::PutToken(rocksdb::WriteBatch* batch, const std::string& token, uint64_t principal) const
{
    batch->Put(meta_, std::string(kTokenPrefix) + token, EncodeUint64(principal));
    return Status::Ok();
}

void UnifiedStorage::DeleteToken(rocksdb::WriteBatch* batch, const std::string& token) const
{
    batch->Delete(meta_, std::string(kTokenPrefix) + token);
}

Status UnifiedStorage::PutPreparingObject(rocksdb::WriteBatch* batch, const StoredObject& object) const
{
    auto payload = SerializeObject(object);
    if (!payload.ok()) {
        return payload.status();
    }
    batch->Put(objects_, NumericKey(kPreparingObjectPrefix, object.object_id), payload.value());
    return Status::Ok();
}

Status UnifiedStorage::PutCommittedObject(rocksdb::WriteBatch* batch, const StoredObject& object) const
{
    auto payload = SerializeObject(object);
    if (!payload.ok()) {
        return payload.status();
    }
    batch->Delete(objects_, NumericKey(kPreparingObjectPrefix, object.object_id));
    batch->Put(objects_, NumericKey(kCommittedObjectPrefix, object.object_id), payload.value());
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

Status UnifiedStorage::CleanupObjectFiles(uint64_t object_id) const
{
    return object_files_->Cleanup(object_id);
}

Result<std::string> UnifiedStorage::ReadObjectFile(const StoredObject& object,
                                                   uint64_t offset,
                                                   uint64_t nbytes) const
{
    return object_files_->ReadRange(object.object_id, object.nbytes, offset, nbytes);
}

Result<std::vector<StoredObject>> UnifiedStorage::ListPreparingObjects() const
{
    std::unique_ptr<rocksdb::Iterator> it(db_->NewIterator(rocksdb::ReadOptions(), objects_));
    std::vector<StoredObject> objects;
    for (it->Seek(kPreparingObjectPrefix);
         it->Valid() && it->key().starts_with(kPreparingObjectPrefix);
         it->Next()) {
        auto object_id = DecodeNumericKey(it->key(), kPreparingObjectPrefix, "preparing object");
        if (!object_id.ok()) {
            return object_id.status();
        }
        auto object = ParseObject(it->value().ToString(), object_id.value());
        if (!object.ok()) {
            return object.status();
        }
        objects.push_back(std::move(object.value()));
    }
    if (!it->status().ok()) {
        return RocksToStatus(it->status(), "list preparing objects");
    }
    return objects;
}

Status UnifiedStorage::RecoverPreparingObjects()
{
    auto objects = ListPreparingObjects();
    if (!objects.ok()) {
        return objects.status();
    }
    if (objects.value().empty()) {
        return Status::Ok();
    }
    {
        auto snapshot_result = CreateSnapshot();
        if (!snapshot_result.ok()) {
            return snapshot_result.status();
        }
        ReadSnapshot snapshot = std::move(snapshot_result.value());
        for (const auto& object : objects.value()) {
            auto committed = GetCommittedObject(snapshot, object.object_id);
            if (!committed.ok()) {
                return committed.status();
            }
            if (committed.value().has_value()) {
                return Status(grpc::StatusCode::DATA_LOSS,
                              "object ID has both PREPARING and COMMITTED metadata");
            }
        }
    }
    std::vector<uint64_t> object_ids;
    object_ids.reserve(objects.value().size());
    for (const auto& object : objects.value()) {
        object_ids.push_back(object.object_id);
    }
    Status file_cleanup = object_files_->Cleanup(object_ids);
    if (!file_cleanup.ok()) {
        return file_cleanup;
    }
    rocksdb::WriteBatch batch;
    for (const auto& object : objects.value()) {
        DeletePreparingObject(&batch, object.object_id);
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
    std::string init_state;
    rocksdb::Status init_status = db_->Get(options, meta_, kInitStateKey, &init_state);
    if (!init_status.IsNotFound()) {
        if (!init_status.ok()) {
            return RocksToStatus(init_status, "read storage initialization marker");
        }
        return Status(grpc::StatusCode::DATA_LOSS, "storage initialization is incomplete");
    }

    auto schema = GetRequiredUint64(snapshot, kSchemaVersionKey);
    if (!schema.ok()) {
        return schema.status();
    }
    if (schema.value() != kSchemaVersion) {
        return Status(grpc::StatusCode::DATA_LOSS, "unsupported storage schema version");
    }
    auto max_seq = GetMaxSeq(snapshot);
    if (!max_seq.ok()) {
        return max_seq.status();
    }
    auto next_uuid = GetNextUuid(snapshot);
    if (!next_uuid.ok() || next_uuid.value() == 0) {
        return next_uuid.ok()
                   ? Status(grpc::StatusCode::DATA_LOSS, "invalid next uuid")
                   : next_uuid.status();
    }
    auto next_channel_id = GetNextChannelId(snapshot);
    if (!next_channel_id.ok() || next_channel_id.value() == 0) {
        return next_channel_id.ok()
                   ? Status(grpc::StatusCode::DATA_LOSS, "invalid next channel ID")
                   : next_channel_id.status();
    }
    auto next_object_id = GetNextObjectId(snapshot);
    if (!next_object_id.ok() || next_object_id.value() == 0) {
        return next_object_id.ok()
                   ? Status(grpc::StatusCode::DATA_LOSS, "invalid next object ID")
                   : next_object_id.status();
    }

    std::string encoded_message;
    const auto message_status = db_->Get(options, messages_, NumericKey(kMessagePrefix, 0), &encoded_message);
    if (message_status.IsNotFound()) {
        return Status(grpc::StatusCode::DATA_LOSS, "missing system initialization message");
    }
    if (!message_status.ok()) {
        return RocksToStatus(message_status, "read system initialization message");
    }
    EventMessage message;
    if (!message.ParseFromString(encoded_message) || !ValidInitializationMessage(message)) {
        return Status(grpc::StatusCode::DATA_LOSS, "invalid system initialization message");
    }
    auto system_seq = GetUsedUuidSeq(snapshot, 0);
    if (!system_seq.ok()) {
        return system_seq.status();
    }
    if (!system_seq.value().has_value() || system_seq.value().value() != 0) {
        return Status(grpc::StatusCode::DATA_LOSS, "missing or conflicting system UUID mapping");
    }

    return Status::Ok();
}

}  // namespace openevent

#include "storage/unified_storage.h"

#include <algorithm>
#include <cstring>
#include <filesystem>
#include <memory>
#include <set>
#include <utility>

#include <fcntl.h>
#include <rocksdb/options.h>
#include <unistd.h>

#include "object_record.pb.h"
#include "storage/encoding.h"

namespace openevent {
namespace {

constexpr uint64_t kSchemaVersion = 2;
constexpr uint64_t kInitializing = 1;
constexpr const char* kMessagesColumnFamily = "messages";
constexpr const char* kObjectsColumnFamily = "objects";
constexpr const char* kSchemaVersionKey = "meta:schema_version";
constexpr const char* kInitStateKey = "meta:init_state";
constexpr const char* kMaxSeqKey = "meta:max_seq";
constexpr const char* kNextChannelIdKey = "meta:next_channel_id";
constexpr const char* kNextObjectIdKey = "meta:next_object_id";
constexpr const char* kChannelPrefix = "ch/";
constexpr const char* kTokenPrefix = "token:";
constexpr const char* kMessagePrefix = "msg/";
constexpr const char* kPreparingObjectPrefix = "preparing/";
constexpr const char* kCommittedObjectPrefix = "object/";
constexpr size_t kObjectTokenBytes = 43;
constexpr size_t kSha256Bytes = 32;
constexpr size_t kMaxObjectNameBytes = 255;
constexpr size_t kMaxObjectTypeBytes = 255;
constexpr size_t kMaxObjectDescriptionBytes = 4096;
constexpr uint64_t kMaxObjectBytes = 4ULL * 1024 * 1024;

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

Status CreateStorageDirectories(const std::string& root_path)
{
    std::error_code ec;
    const std::filesystem::path root = std::filesystem::absolute(root_path, ec);
    if (ec) {
        return Status(grpc::StatusCode::UNAVAILABLE, "resolve storage root: " + ec.message());
    }
    std::filesystem::create_directories(root, ec);
    if (ec) {
        return Status(grpc::StatusCode::UNAVAILABLE, "create storage root: " + ec.message());
    }
    std::filesystem::create_directory(root / "db", ec);
    if (ec && ec != std::errc::file_exists) {
        return Status(grpc::StatusCode::UNAVAILABLE, "create RocksDB directory: " + ec.message());
    }
    ec.clear();
    std::filesystem::create_directory(root / "objects", ec);
    if (ec && ec != std::errc::file_exists) {
        return Status(grpc::StatusCode::UNAVAILABLE, "create object directory: " + ec.message());
    }
    Status root_sync = SyncDirectory(root);
    if (!root_sync.ok()) {
        return root_sync;
    }
    return SyncDirectory(root.parent_path());
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

rocksdb::ColumnFamilyHandle* FindHandle(const std::vector<std::string>& names,
                                       const std::vector<rocksdb::ColumnFamilyHandle*>& handles,
                                       const std::string& name)
{
    auto it = std::find(names.begin(), names.end(), name);
    if (it == names.end()) {
        return nullptr;
    }
    return handles[static_cast<size_t>(std::distance(names.begin(), it))];
}

Result<bool> IsInitializationOnly(rocksdb::DB* db,
                                  rocksdb::ColumnFamilyHandle* meta,
                                  const std::vector<rocksdb::ColumnFamilyHandle*>& data_handles)
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

    for (auto* handle : data_handles) {
        if (handle == nullptr) {
            continue;
        }
        std::unique_ptr<rocksdb::Iterator> it(db->NewIterator(rocksdb::ReadOptions(), handle));
        it->SeekToFirst();
        if (!it->status().ok()) {
            return RocksToStatus(it->status(), "scan initialization column family");
        }
        if (it->Valid()) {
            return false;
        }
    }
    return true;
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
    record.set_sha256(object.sha256);
    std::string payload;
    if (!record.SerializeToString(&payload)) {
        return Status(grpc::StatusCode::INTERNAL, "serialize object metadata failed");
    }
    return payload;
}

bool IsBase64UrlToken(const std::string& token)
{
    return token.size() == kObjectTokenBytes &&
           std::all_of(token.begin(), token.end(), [](unsigned char ch) {
               return (ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z') ||
                      (ch >= '0' && ch <= '9') || ch == '-' || ch == '_';
           });
}

Result<StoredObject> ParseObject(const std::string& payload, uint64_t expected_id)
{
    storage::internal::ObjectRecord record;
    if (!record.ParseFromString(payload) || record.object_id() != expected_id || expected_id == 0 ||
        !IsBase64UrlToken(record.object_token()) || record.name().empty() ||
        record.name().size() > kMaxObjectNameBytes || record.type().empty() ||
        record.type().size() > kMaxObjectTypeBytes ||
        record.description().size() > kMaxObjectDescriptionBytes || record.nbytes() == 0 ||
        record.nbytes() > kMaxObjectBytes || record.sha256().size() != kSha256Bytes) {
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
    object.sha256 = record.sha256();
    return object;
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
                               std::unique_ptr<ObjectFileStore> object_files)
    : db_(std::move(db)),
      meta_(meta),
      messages_(messages),
      objects_(objects),
      object_files_(std::move(object_files))
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

Result<std::unique_ptr<UnifiedStorage>> UnifiedStorage::Open(const std::string& path)
{
    if (path.empty()) {
        return Status(grpc::StatusCode::INVALID_ARGUMENT, "storage path must not be empty");
    }

    if (IsNewStorageRoot(path)) {
        Status status = InitializeNew(path);
        if (!status.ok()) {
            return status;
        }
    }

    const std::filesystem::path db_path = DatabasePath(path);
    const std::filesystem::path objects_path = ObjectsPath(path);
    std::error_code ec;
    if (!std::filesystem::is_directory(db_path, ec) || ec ||
        !std::filesystem::is_directory(objects_path, ec) || ec) {
        return Status(grpc::StatusCode::INTERNAL, "storage root must contain db and objects directories");
    }

    std::vector<std::string> column_families;
    rocksdb::Options options;
    rocksdb::Status list_status = rocksdb::DB::ListColumnFamilies(options, db_path.string(), &column_families);
    if (!list_status.ok()) {
        return RocksToStatus(list_status, "list storage column families");
    }
    const std::set<std::string> names(column_families.begin(), column_families.end());
    const std::set<std::string> supported{
        rocksdb::kDefaultColumnFamilyName, kMessagesColumnFamily, kObjectsColumnFamily};
    if (!std::includes(supported.begin(), supported.end(), names.begin(), names.end())) {
        return Status(grpc::StatusCode::INTERNAL, "unsupported storage column family layout");
    }
    Status initialization_status = CompleteInitialization(path, column_families);
    if (!initialization_status.ok()) {
        return initialization_status;
    }
    return OpenExisting(path);
}

Status UnifiedStorage::InitializeNew(const std::string& path)
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
    batch.Put(kNextChannelIdKey, EncodeUint64(uint64_t{1}));
    batch.Put(kNextObjectIdKey, EncodeUint64(uint64_t{1}));
    batch.Delete(kInitStateKey);
    rocksdb::Status init_status = db->Write(SynchronousWriteOptions(), &batch);
    rocksdb::Status objects_destroy = db->DestroyColumnFamilyHandle(objects);
    rocksdb::Status messages_destroy = db->DestroyColumnFamilyHandle(messages);
    if (!init_status.ok()) {
        return RocksToStatus(init_status, "finish storage initialization");
    }
    if (!objects_destroy.ok()) {
        return RocksToStatus(objects_destroy, "destroy objects column family handle");
    }
    if (!messages_destroy.ok()) {
        return RocksToStatus(messages_destroy, "destroy messages column family handle");
    }
    return Status::Ok();
}

Status UnifiedStorage::CompleteInitialization(const std::string& root_path,
                                              const std::vector<std::string>& column_families)
{
    std::vector<rocksdb::ColumnFamilyDescriptor> descriptors;
    for (const auto& name : column_families) {
        descriptors.emplace_back(name, rocksdb::ColumnFamilyOptions());
    }
    rocksdb::DBOptions options;
    std::vector<rocksdb::ColumnFamilyHandle*> handles;
    rocksdb::DB* raw_db = nullptr;
    rocksdb::Status open_status = rocksdb::DB::Open(
        options, DatabasePath(root_path).string(), descriptors, &handles, &raw_db);
    if (!open_status.ok()) {
        return RocksToStatus(open_status, "open partial storage");
    }
    std::unique_ptr<rocksdb::DB> db(raw_db);
    rocksdb::ColumnFamilyHandle* meta = FindHandle(column_families, handles, rocksdb::kDefaultColumnFamilyName);
    rocksdb::ColumnFamilyHandle* messages = FindHandle(column_families, handles, kMessagesColumnFamily);
    rocksdb::ColumnFamilyHandle* objects = FindHandle(column_families, handles, kObjectsColumnFamily);
    if (meta == nullptr) {
        DestroyHandles(db.get(), &handles);
        return Status(grpc::StatusCode::INTERNAL, "partial storage is missing default column family");
    }

    std::string marker;
    rocksdb::Status marker_status = db->Get(rocksdb::ReadOptions(), meta, kInitStateKey, &marker);
    if (marker_status.IsNotFound()) {
        const bool complete_layout = messages != nullptr && objects != nullptr;
        Status destroy_status = DestroyHandles(db.get(), &handles);
        if (!destroy_status.ok()) {
            return destroy_status;
        }
        return complete_layout
                   ? Status::Ok()
                   : Status(grpc::StatusCode::INTERNAL,
                            "incomplete storage has no valid initialization marker");
    }
    if (!marker_status.ok()) {
        DestroyHandles(db.get(), &handles);
        return RocksToStatus(marker_status, "read storage initialization marker");
    }

    auto initialization_only = IsInitializationOnly(db.get(), meta, {messages, objects});
    if (!initialization_only.ok()) {
        DestroyHandles(db.get(), &handles);
        return initialization_only.status();
    }
    if (!initialization_only.value()) {
        DestroyHandles(db.get(), &handles);
        return Status(grpc::StatusCode::INTERNAL, "incomplete storage has no valid initialization marker");
    }

    if (messages == nullptr) {
        rocksdb::Status status =
            db->CreateColumnFamily(rocksdb::ColumnFamilyOptions(), kMessagesColumnFamily, &messages);
        if (!status.ok()) {
            DestroyHandles(db.get(), &handles);
            return RocksToStatus(status, "complete messages column family creation");
        }
        handles.push_back(messages);
    }
    if (objects == nullptr) {
        rocksdb::Status status =
            db->CreateColumnFamily(rocksdb::ColumnFamilyOptions(), kObjectsColumnFamily, &objects);
        if (!status.ok()) {
            DestroyHandles(db.get(), &handles);
            return RocksToStatus(status, "complete objects column family creation");
        }
        handles.push_back(objects);
    }

    rocksdb::WriteBatch batch;
    batch.Put(meta, kSchemaVersionKey, EncodeUint64(kSchemaVersion));
    batch.Put(meta, kMaxSeqKey, EncodeUint64(uint64_t{0}));
    batch.Put(meta, kNextChannelIdKey, EncodeUint64(uint64_t{1}));
    batch.Put(meta, kNextObjectIdKey, EncodeUint64(uint64_t{1}));
    batch.Delete(meta, kInitStateKey);
    rocksdb::Status write_status = db->Write(SynchronousWriteOptions(), &batch);
    Status destroy_status = DestroyHandles(db.get(), &handles);
    if (!write_status.ok()) {
        return RocksToStatus(write_status, "complete storage initialization");
    }
    return destroy_status;
}

Result<std::unique_ptr<UnifiedStorage>> UnifiedStorage::OpenExisting(const std::string& root_path)
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

    auto object_files = ObjectFileStore::Open(ObjectsPath(root_path).string());
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
        std::move(object_files.value())));
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

Result<std::string> UnifiedStorage::ReadObjectFile(const StoredObject& object) const
{
    return object_files_->ReadAndValidate(object.object_id, object.nbytes, object.sha256);
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
    for (const auto& object : objects.value()) {
        Status status = object_files_->Cleanup(object.object_id);
        if (!status.ok()) {
            return status;
        }
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
    auto next_object_id = GetNextObjectId(snapshot);
    if (!next_object_id.ok() || next_object_id.value() == 0) {
        return next_object_id.ok()
                   ? Status(grpc::StatusCode::INTERNAL, "invalid next object ID")
                   : next_object_id.status();
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

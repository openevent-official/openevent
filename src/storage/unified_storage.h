#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <rocksdb/db.h>
#include <rocksdb/write_batch.h>

#include "admin.pb.h"
#include "common/status.h"
#include "openevent.pb.h"
#include "storage/object_file_store.h"

namespace openevent {

class UnifiedStorage;

class ReadSnapshot {
public:
    ReadSnapshot() = default;
    ReadSnapshot(const ReadSnapshot&) = delete;
    ReadSnapshot& operator=(const ReadSnapshot&) = delete;
    ReadSnapshot(ReadSnapshot&& other) noexcept;
    ReadSnapshot& operator=(ReadSnapshot&& other) noexcept;
    ~ReadSnapshot();

private:
    friend class UnifiedStorage;

    ReadSnapshot(const UnifiedStorage* owner, const rocksdb::Snapshot* snapshot);
    void Reset();

    const UnifiedStorage* owner_ = nullptr;
    const rocksdb::Snapshot* snapshot_ = nullptr;
};

struct TokenBindingRecord {
    std::string token;
    uint64_t principal = 0;
};

struct TokenBindingPage {
    std::vector<TokenBindingRecord> bindings;
    bool has_more = false;
};

struct StoredObject {
    uint64_t object_id = 0;
    std::string object_token;
    uint64_t creator_principal = 0;
    std::string name;
    std::string type;
    std::string description;
    uint64_t nbytes = 0;
};

enum class MessageScanAction {
    kContinue,
    kStopBefore,
    kStopAfter,
};

class UnifiedStorage {
public:
    using MessageVisitor = std::function<Result<MessageScanAction>(const EventMessage&)>;

    ~UnifiedStorage();

    UnifiedStorage(const UnifiedStorage&) = delete;
    UnifiedStorage& operator=(const UnifiedStorage&) = delete;

    static Result<std::unique_ptr<UnifiedStorage>> Open(
        const std::string& path,
        StorageFaultInjector fault_injector = {});

    Result<ReadSnapshot> CreateSnapshot() const;
    Status Commit(rocksdb::WriteBatch* batch);

    Result<uint64_t> GetMaxSeq(const ReadSnapshot& snapshot) const;
    Result<uint64_t> GetNextChannelId(const ReadSnapshot& snapshot) const;
    Result<uint64_t> GetNextObjectId(const ReadSnapshot& snapshot) const;
    Result<std::optional<uint64_t>> GetPrincipalForToken(const ReadSnapshot& snapshot,
                                                         const std::string& token) const;
    Result<std::optional<ChannelInfo>> GetChannel(const ReadSnapshot& snapshot, uint64_t channel_id) const;
    Result<std::vector<ChannelInfo>> ListChannels(const ReadSnapshot& snapshot) const;
    Result<TokenBindingPage> ListTokens(const ReadSnapshot& snapshot,
                                        const std::string& start_after,
                                        uint32_t limit) const;
    Result<uint64_t> ScanMessages(const ReadSnapshot& snapshot,
                                  uint64_t from_seq,
                                  uint64_t last_seq,
                                  uint64_t max_records,
                                  const MessageVisitor& visitor) const;
    Result<std::optional<StoredObject>> GetPreparingObject(const ReadSnapshot& snapshot,
                                                           uint64_t object_id) const;
    Result<std::optional<StoredObject>> GetCommittedObject(const ReadSnapshot& snapshot,
                                                           uint64_t object_id) const;
    Result<std::vector<std::optional<StoredObject>>> GetCommittedObjects(
        const ReadSnapshot& snapshot,
        const std::vector<uint64_t>& object_ids) const;

    Status SetMaxSeq(rocksdb::WriteBatch* batch, uint64_t seq) const;
    Status SetNextChannelId(rocksdb::WriteBatch* batch, uint64_t channel_id) const;
    Status SetNextObjectId(rocksdb::WriteBatch* batch, uint64_t object_id) const;
    Status PutMessage(rocksdb::WriteBatch* batch, const EventMessage& message) const;
    Status PutChannel(rocksdb::WriteBatch* batch, const ChannelInfo& channel) const;
    Status PutToken(rocksdb::WriteBatch* batch, const std::string& token, uint64_t principal) const;
    void DeleteToken(rocksdb::WriteBatch* batch, const std::string& token) const;
    Status PutPreparingObject(rocksdb::WriteBatch* batch, const StoredObject& object) const;
    Status PutCommittedObject(rocksdb::WriteBatch* batch, const StoredObject& object) const;
    void DeletePreparingObject(rocksdb::WriteBatch* batch, uint64_t object_id) const;

    Status WriteObjectFile(uint64_t object_id, const std::string& data) const;
    Status CleanupObjectFiles(uint64_t object_id) const;
    Result<std::string> ReadObjectFile(const StoredObject& object,
                                       uint64_t offset,
                                       uint64_t nbytes) const;

private:
    friend class ReadSnapshot;

    UnifiedStorage(std::unique_ptr<rocksdb::DB> db,
                   rocksdb::ColumnFamilyHandle* meta,
                   rocksdb::ColumnFamilyHandle* messages,
                   rocksdb::ColumnFamilyHandle* objects,
                   std::unique_ptr<ObjectFileStore> object_files,
                   StorageFaultInjector fault_injector);

    static Status InitializeNew(const std::string& root_path);
    static Result<std::unique_ptr<UnifiedStorage>> OpenExisting(
        const std::string& root_path,
        StorageFaultInjector fault_injector);

    Result<uint64_t> GetRequiredUint64(const ReadSnapshot& snapshot, const std::string& key) const;
    Result<std::optional<StoredObject>> GetObject(const ReadSnapshot& snapshot,
                                                  const char* prefix,
                                                  uint64_t object_id) const;
    Result<std::vector<StoredObject>> ListPreparingObjects() const;
    Status RecoverPreparingObjects();
    Status Validate() const;
    void ReleaseSnapshot(const rocksdb::Snapshot* snapshot) const;

    std::unique_ptr<rocksdb::DB> db_;
    rocksdb::ColumnFamilyHandle* meta_ = nullptr;
    rocksdb::ColumnFamilyHandle* messages_ = nullptr;
    rocksdb::ColumnFamilyHandle* objects_ = nullptr;
    std::unique_ptr<ObjectFileStore> object_files_;
    StorageFaultInjector fault_injector_;
};

}  // namespace openevent

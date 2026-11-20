#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <rocksdb/db.h>
#include <rocksdb/write_batch.h>

#include "common/status.h"
#include "object_record.pb.h"
#include "openevent/sdk/proto/admin.pb.h"
#include "openevent/sdk/proto/openevent.pb.h"
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

enum class MessageScanAction {
    kContinue,
    kStopBefore,
    kStopAfter,
};

class UnifiedStorage {
public:
    using ObjectLookupResult = Result<std::optional<storage::internal::ObjectRecord>>;
    using ChannelLookupResult = Result<std::optional<ChannelInfo>>;
    using MessageVisitor = std::function<Result<MessageScanAction>(EventMessage&&)>;

    ~UnifiedStorage();

    UnifiedStorage(const UnifiedStorage&) = delete;
    UnifiedStorage& operator=(const UnifiedStorage&) = delete;

    static Result<std::unique_ptr<UnifiedStorage>> Open(
        const std::string& path,
        StorageFaultInjector fault_injector = {});

    Result<ReadSnapshot> CreateSnapshot() const;
    Status Commit(rocksdb::WriteBatch* batch);

    Result<uint64_t> GetMaxSeq(const ReadSnapshot& snapshot) const;
    Result<uint64_t> GetNextUuid(const ReadSnapshot& snapshot) const;
    Result<std::optional<uint64_t>> GetUsedUuidSeq(const ReadSnapshot& snapshot, uint64_t uuid) const;
    Result<uint64_t> GetNextChannelId(const ReadSnapshot& snapshot) const;
    Result<uint64_t> GetNextObjectId(const ReadSnapshot& snapshot) const;
    Result<bool> HasToken(const ReadSnapshot& snapshot, uint64_t principal,
                          const std::string& token) const;
    ChannelLookupResult GetChannel(const ReadSnapshot& snapshot, uint64_t channel_id) const;
    Status CheckChannels(const ReadSnapshot& snapshot, const std::vector<uint64_t>& channel_ids,
                         const std::function<Status(const std::optional<ChannelInfo>&)>& check) const;
    Status ScanChannels(const ReadSnapshot& snapshot, const std::function<void(ChannelInfo&&)>& visit) const;
    Result<uint64_t> ScanMessages(const ReadSnapshot& snapshot,
                                  uint64_t from_seq,
                                  uint64_t last_seq,
                                  uint64_t max_records,
                                  const MessageVisitor& visitor) const;
    ObjectLookupResult GetPreparingObject(
        const ReadSnapshot& snapshot, uint64_t object_id) const;
    ObjectLookupResult GetCommittedObject(
        const ReadSnapshot& snapshot, uint64_t object_id) const;
    std::vector<ObjectLookupResult> GetCommittedObjects(
        const ReadSnapshot& snapshot,
        const std::vector<uint64_t>& object_ids) const;
    static Status MostSevereReadError(const std::vector<ObjectLookupResult>& results);

    Status SetMaxSeq(rocksdb::WriteBatch* batch, uint64_t seq) const;
    Status SetNextUuid(rocksdb::WriteBatch* batch, uint64_t uuid) const;
    Status PutUsedUuid(rocksdb::WriteBatch* batch, uint64_t uuid, uint64_t seq) const;
    Status SetNextChannelId(rocksdb::WriteBatch* batch, uint64_t channel_id) const;
    Status SetNextObjectId(rocksdb::WriteBatch* batch, uint64_t object_id) const;
    Status PutMessage(rocksdb::WriteBatch* batch, const EventMessage& message) const;
    Status PutChannel(rocksdb::WriteBatch* batch, const ChannelInfo& channel) const;
    Status PutToken(rocksdb::WriteBatch* batch, uint64_t principal, const std::string& token) const;
    void DeleteToken(rocksdb::WriteBatch* batch, uint64_t principal, const std::string& token) const;
    Status PutPreparingObject(rocksdb::WriteBatch* batch, const storage::internal::ObjectRecord& object) const;
    Status PutCommittedObject(rocksdb::WriteBatch* batch, const storage::internal::ObjectRecord& object) const;
    void DeletePreparingObject(rocksdb::WriteBatch* batch, uint64_t object_id) const;

    Status WriteObjectFile(uint64_t object_id, const std::string& data) const;
    Status CleanupObjectFile(uint64_t object_id) const;
    Result<std::string> ReadObjectFile(const storage::internal::ObjectRecord& object,
                                       uint64_t offset,
                                       uint64_t nbytes) const;

private:
    friend class ReadSnapshot;

    UnifiedStorage(std::unique_ptr<rocksdb::DB> db,
                   rocksdb::ColumnFamilyHandle* meta,
                   rocksdb::ColumnFamilyHandle* messages,
                   rocksdb::ColumnFamilyHandle* objects,
                   StorageFaultInjector fault_injector);

    Status WriteInitialState();

    Result<uint64_t> GetRequiredUint64(const ReadSnapshot& snapshot, const std::string& key) const;
    ObjectLookupResult GetObject(
        const ReadSnapshot& snapshot, const char* prefix, uint64_t object_id) const;
    Result<std::vector<uint64_t>> ListPreparingObjectIds() const;
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

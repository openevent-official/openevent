#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>

#include "admin.pb.h"
#include "common/status.h"
#include "openevent.pb.h"
#include "storage/unified_storage.h"

namespace openevent {

class OpenEventCore {
public:
    using FatalErrorHandler = std::function<void(const Status&)>;

    OpenEventCore(std::unique_ptr<UnifiedStorage> storage,
                  size_t max_payload_bytes,
                  uint64_t max_scan_records = 10000,
                  size_t response_soft_limit_bytes = 0,
                  FatalErrorHandler fatal_error_handler = {});

    Status GetStatus(const GetStatusRequest& request, GetStatusResponse* response);
    Status Publish(const PublishRequest& request, PublishResponse* response);
    Status PublishAutoSeq(const PublishAutoSeqRequest& request, PublishAutoSeqResponse* response);
    Status Fetch(const FetchRequest& request, FetchResponse* response);

    Status WriteObject(const WriteObjectRequest& request, WriteObjectResponse* response);
    Status GetObjectMetadata(const GetObjectMetadataRequest& request, GetObjectMetadataResponse* response);
    Status ReadObject(const ReadObjectRequest& request, ReadObjectResponse* response);

    Status CreateChannel(const CreateChannelRequest& request, CreateChannelResponse* response);
    Status GetChannel(const GetChannelRequest& request, GetChannelResponse* response);
    Status ListChannels(const ListChannelsRequest& request, ListChannelsResponse* response);
    Status AddMember(const AddMemberRequest& request, AddMemberResponse* response);
    Status RemoveMember(const RemoveMemberRequest& request, RemoveMemberResponse* response);

    Status AddToken(const AddTokenRequest& request, AddTokenResponse* response);
    Status DeleteToken(const DeleteTokenRequest& request, DeleteTokenResponse* response);
    Status ListTokens(const ListTokensRequest& request, ListTokensResponse* response);
    Status ListMessages(const ListMessagesRequest& request, ListMessagesResponse* response);

    Status GetSubscriptionMaxSeq(uint64_t principal, const std::string& token, uint64_t* max_seq) const;
    Status FetchSubscriptionBatch(uint64_t principal,
                                  const std::string& token,
                                  uint64_t from_seq,
                                  uint32_t limit,
                                  bool only_my_recipient,
                                  FetchResponse* response) const;
    uint64_t CommitGeneration() const;
    void WaitForCommit(uint64_t observed_generation, std::chrono::milliseconds timeout) const;

    Result<uint64_t> MaxSeq() const;

private:
    Result<ReadSnapshot> CreateLinearizedSnapshot();
    Status Authenticate(const ReadSnapshot& snapshot, uint64_t principal, const std::string& token) const;
    Status BuildPublishBatch(const ReadSnapshot& snapshot,
                             uint64_t principal,
                             uint64_t channel_id,
                             uint64_t seq,
                             const google::protobuf::RepeatedField<uint64_t>& recipients,
                             const std::string& payload,
                             const google::protobuf::RepeatedPtrField<ObjectKey>& object_keys,
                             uint64_t ts_ms,
                             rocksdb::WriteBatch* batch) const;
    Status CommitBatch(rocksdb::WriteBatch* batch);
    Status ValidatePayloadSize(const std::string& payload) const;
    Status ValidateObjectKeysShape(const google::protobuf::RepeatedPtrField<ObjectKey>& object_keys) const;
    Result<StoredObject> LoadAuthorizedObject(const ReadSnapshot& snapshot,
                                              uint64_t object_id,
                                              const std::string& object_token) const;
    Status CleanupPreparingObject(uint64_t object_id, const Status& result);
    Status FatalStorageError(const Status& status) const;
    Status FetchVisible(const ReadSnapshot& snapshot,
                        uint64_t principal,
                        uint64_t from_seq,
                        uint32_t limit,
                        bool only_my_recipient,
                        const google::protobuf::RepeatedField<uint64_t>& channels,
                        FetchResponse* response) const;
    Status ListAllMessages(const ReadSnapshot& snapshot,
                           uint64_t from_seq,
                           uint32_t limit,
                           ListMessagesResponse* response) const;

    Result<std::optional<ChannelInfo>> LoadChannel(const ReadSnapshot& snapshot, uint64_t channel_id) const;
    bool CanRead(const ChannelInfo& channel, uint64_t principal) const;
    bool CanWrite(const ChannelInfo& channel, uint64_t principal) const;
    bool IsMember(const ChannelInfo& channel, uint64_t principal) const;
    Status ValidateRecipients(const ChannelInfo& channel,
                              const google::protobuf::RepeatedField<uint64_t>& recipients) const;
    bool HasRecipient(const EventMessage& message, uint64_t principal) const;
    ChannelInfo SystemChannel() const;
    Status ValidateVisibility(Visibility visibility) const;
    Status ValidateFilter(ChannelFilter filter) const;

    std::unique_ptr<UnifiedStorage> storage_;
    size_t max_payload_bytes_ = 0;
    uint64_t max_scan_records_ = 0;
    size_t response_soft_limit_bytes_ = 0;
    mutable std::mutex coordinator_mu_;
    std::atomic<uint64_t> commit_generation_{0};
    mutable std::mutex commit_wait_mu_;
    mutable std::condition_variable commit_cv_;
    FatalErrorHandler fatal_error_handler_;
    mutable std::atomic<bool> fatal_error_triggered_{false};
};

}  // namespace openevent

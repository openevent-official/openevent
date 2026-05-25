#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <string>

#include "common/status.h"
#include "openevent.pb.h"
#include "storage/message_store.h"
#include "storage/metadata_store.h"

namespace openevent {

class OpenEventCore {
public:
    OpenEventCore(std::unique_ptr<MetadataStore> metadata,
                  std::unique_ptr<MessageStore> message_store,
                  size_t max_payload_bytes);

    Status Authenticate(uint64_t principal, const std::string& token) const;

    Status GetStatus(const GetStatusRequest& request, GetStatusResponse* response);
    Status Publish(const PublishRequest& request, PublishResponse* response);
    Status PublishAutoSeq(const PublishAutoSeqRequest& request, PublishAutoSeqResponse* response);
    Status Fetch(const FetchRequest& request, FetchResponse* response);
    Status FetchVisible(uint64_t principal, uint64_t from_seq, uint32_t limit, bool only_my_recipient,
                        FetchResponse* response);

    Status CreateChannel(const CreateChannelRequest& request, CreateChannelResponse* response);
    Status GetChannel(const GetChannelRequest& request, GetChannelResponse* response);
    Status ListChannels(const ListChannelsRequest& request, ListChannelsResponse* response);
    Status AddMember(const AddMemberRequest& request, AddMemberResponse* response);
    Status RemoveMember(const RemoveMemberRequest& request, RemoveMemberResponse* response);

    Status AddToken(const AddTokenRequest& request, AddTokenResponse* response);
    Status DeleteToken(const DeleteTokenRequest& request, DeleteTokenResponse* response);
    Status ListTokens(const ListTokensRequest& request, ListTokensResponse* response);

    Result<uint64_t> MaxSeq() const;
    Status RecoverPending();

private:
    Status PublishCommitted(uint64_t principal,
                            uint64_t channel_id,
                            uint64_t seq,
                            const google::protobuf::RepeatedField<uint64_t>& recipients,
                            const std::string& payload,
                            uint64_t ts_ms);
    Status ValidatePayloadSize(const std::string& payload) const;

    Result<std::optional<ChannelInfo>> LoadChannel(uint64_t channel_id) const;
    bool CanRead(const ChannelInfo& channel, uint64_t principal) const;
    bool CanWrite(const ChannelInfo& channel, uint64_t principal) const;
    bool IsMember(const ChannelInfo& channel, uint64_t principal) const;
    Status ValidateRecipients(const ChannelInfo& channel,
                              const google::protobuf::RepeatedField<uint64_t>& recipients) const;
    bool HasRecipient(const Message& message, uint64_t principal) const;
    bool SameMessage(const Message& lhs, const Message& rhs) const;
    ChannelInfo SystemChannel() const;
    Status ValidateVisibility(Visibility visibility) const;
    Status ValidateFilter(ChannelFilter filter) const;

    std::unique_ptr<MetadataStore> metadata_;
    std::unique_ptr<MessageStore> message_store_;
    size_t max_payload_bytes_ = 0;
    mutable std::mutex write_mu_;
};

}  // namespace openevent

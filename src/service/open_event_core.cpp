#include "service/open_event_core.h"

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <sstream>
#include <unordered_set>

#include <sys/random.h>

namespace openevent {
namespace {

uint64_t NowMs()
{
    using namespace std::chrono;
    return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
}

Result<std::string> GenerateToken()
{
    uint8_t bytes[16] = {};
    size_t filled = 0;
    while (filled < sizeof(bytes)) {
        const ssize_t n = getrandom(bytes + filled, sizeof(bytes) - filled, 0);
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            return Status(grpc::StatusCode::INTERNAL, std::string("getrandom failed: ") + std::strerror(errno));
        }
        filled += static_cast<size_t>(n);
    }

    bytes[6] = static_cast<uint8_t>((bytes[6] & 0x0f) | 0x40);
    bytes[8] = static_cast<uint8_t>((bytes[8] & 0x3f) | 0x80);

    std::ostringstream oss;
    oss << std::hex << std::nouppercase;
    oss.fill('0');
    for (size_t i = 0; i < sizeof(bytes); ++i) {
        if (i == 4 || i == 6 || i == 8 || i == 10) {
            oss << '-';
        }
        oss.width(2);
        oss << static_cast<unsigned>(bytes[i]);
    }
    return oss.str();
}

template <typename Repeated>
bool Contains(const Repeated& values, uint64_t value)
{
    return std::find(values.begin(), values.end(), value) != values.end();
}

}  // namespace

OpenEventCore::OpenEventCore(std::unique_ptr<MetadataStore> metadata,
                             std::unique_ptr<MessageStore> message_store,
                             size_t max_payload_bytes)
    : metadata_(std::move(metadata)),
      message_store_(std::move(message_store)),
      max_payload_bytes_(max_payload_bytes)
{
}

Status OpenEventCore::Authenticate(uint64_t principal, const std::string& token) const
{
    if (token.empty()) {
        return Status(grpc::StatusCode::UNAUTHENTICATED, "token is required");
    }

    auto result = metadata_->GetPrincipalForToken(token);
    if (!result.ok()) {
        return result.status();
    }
    if (!result.value().has_value() || result.value().value() != principal) {
        return Status(grpc::StatusCode::UNAUTHENTICATED, "invalid token");
    }
    return Status::Ok();
}

Status OpenEventCore::GetStatus(const GetStatusRequest& request, GetStatusResponse* response)
{
    Status auth = Authenticate(request.principal(), request.token());
    if (!auth.ok()) {
        return auth;
    }

    auto max_seq = metadata_->GetMaxSeq();
    if (!max_seq.ok()) {
        return max_seq.status();
    }
    response->set_max_seq(max_seq.value());
    response->set_min_seq(max_seq.value() == 0 ? 0 : 1);
    return Status::Ok();
}

Status OpenEventCore::Publish(const PublishRequest& request, PublishResponse*)
{
    const uint64_t ts_ms = NowMs();

    Status auth = Authenticate(request.principal(), request.token());
    if (!auth.ok()) {
        return auth;
    }
    Status payload_status = ValidatePayloadSize(request.payload());
    if (!payload_status.ok()) {
        return payload_status;
    }

    std::lock_guard<std::mutex> lock(write_mu_);
    auto max_seq = metadata_->GetMaxSeq();
    if (!max_seq.ok()) {
        return max_seq.status();
    }
    if (request.seq() != max_seq.value() + 1) {
        return Status(grpc::StatusCode::ABORTED, "seq must equal max_seq + 1");
    }

    return PublishCommitted(request.principal(), request.channel_id(), request.seq(), request.recipients(),
                            request.payload(), ts_ms);
}

Status OpenEventCore::PublishAutoSeq(const PublishAutoSeqRequest& request, PublishAutoSeqResponse* response)
{
    const uint64_t ts_ms = NowMs();

    Status auth = Authenticate(request.principal(), request.token());
    if (!auth.ok()) {
        return auth;
    }
    Status payload_status = ValidatePayloadSize(request.payload());
    if (!payload_status.ok()) {
        return payload_status;
    }

    std::lock_guard<std::mutex> lock(write_mu_);
    auto max_seq = metadata_->GetMaxSeq();
    if (!max_seq.ok()) {
        return max_seq.status();
    }
    const uint64_t seq = max_seq.value() + 1;

    Status status = PublishCommitted(request.principal(), request.channel_id(), seq, request.recipients(),
                                     request.payload(), ts_ms);
    if (!status.ok()) {
        return status;
    }
    response->set_seq(seq);
    return Status::Ok();
}

Status OpenEventCore::PublishCommitted(uint64_t principal,
                                       uint64_t channel_id,
                                       uint64_t seq,
                                       const google::protobuf::RepeatedField<uint64_t>& recipients,
                                       const std::string& payload,
                                       uint64_t ts_ms)
{
    if (channel_id == 0) {
        return Status(grpc::StatusCode::PERMISSION_DENIED, "system channel is read-only");
    }
    Status payload_status = ValidatePayloadSize(payload);
    if (!payload_status.ok()) {
        return payload_status;
    }

    auto channel_result = LoadChannel(channel_id);
    if (!channel_result.ok()) {
        return channel_result.status();
    }
    if (!channel_result.value().has_value()) {
        return Status(grpc::StatusCode::NOT_FOUND, "channel not found");
    }
    const ChannelInfo& channel = channel_result.value().value();
    if (!CanWrite(channel, principal)) {
        return Status(grpc::StatusCode::PERMISSION_DENIED, "write permission denied");
    }
    Status recipients_status = ValidateRecipients(channel, recipients);
    if (!recipients_status.ok()) {
        return recipients_status;
    }

    Message message;
    message.set_seq(seq);
    message.set_channel_id(channel_id);
    message.set_principal(principal);
    for (uint64_t recipient : recipients) {
        message.add_recipients(recipient);
    }
    message.set_payload(payload);
    message.set_ts_ms(ts_ms);

    Status status = metadata_->PutPendingMessage(seq, message);
    if (!status.ok()) {
        return status;
    }

    auto append_result = message_store_->Append(message);
    if (!append_result.ok()) {
        return append_result.status();
    }

    status = metadata_->PutPendingOffset(seq, append_result.value());
    if (!status.ok()) {
        return status;
    }

    return metadata_->CommitMessage(seq, append_result.value(), channel_id, ts_ms);
}

Status OpenEventCore::ValidatePayloadSize(const std::string& payload) const
{
    if (payload.size() <= max_payload_bytes_) {
        return Status::Ok();
    }
    return Status(grpc::StatusCode::RESOURCE_EXHAUSTED,
                  "payload exceeds limits.max_payload_bytes (" + std::to_string(payload.size()) + " > " +
                      std::to_string(max_payload_bytes_) + ")");
}

Status OpenEventCore::Fetch(const FetchRequest& request, FetchResponse* response)
{
    Status auth = Authenticate(request.principal(), request.token());
    if (!auth.ok()) {
        return auth;
    }
    if (request.limit() == 0 || request.limit() > 1000) {
        return Status(grpc::StatusCode::INVALID_ARGUMENT, "limit must be in 1..1000");
    }

    return FetchVisible(request.principal(), request.from_seq(), request.limit(), request.only_my_recipient(),
                        response);
}

Status OpenEventCore::FetchVisible(uint64_t principal,
                                   uint64_t from_seq,
                                   uint32_t limit,
                                   bool only_my_recipient,
                                   FetchResponse* response)
{
    auto max_seq_result = metadata_->GetMaxSeq();
    if (!max_seq_result.ok()) {
        return max_seq_result.status();
    }
    const uint64_t max_seq = max_seq_result.value();
    response->clear_messages();

    if (from_seq == 0 || from_seq > max_seq) {
        response->set_has_more(false);
        response->set_next_seq(max_seq + 1);
        return Status::Ok();
    }

    uint64_t seq = from_seq;
    uint64_t next_seq = from_seq;
    while (seq <= max_seq && response->messages_size() < static_cast<int>(limit)) {
        auto offset_result = metadata_->GetOffsetForSeq(seq);
        if (!offset_result.ok()) {
            return offset_result.status();
        }
        if (!offset_result.value().has_value()) {
            ++seq;
            next_seq = seq;
            continue;
        }

        const uint64_t expected_offset = offset_result.value().value();
        auto fetch_result = message_store_->Fetch(expected_offset, 1);
        if (!fetch_result.ok()) {
            return fetch_result.status();
        }

        if (!fetch_result.value().records.empty()) {
            const StoredMessage& stored = fetch_result.value().records.front();
            if (stored.offset != expected_offset) {
                ++seq;
                next_seq = seq;
                continue;
            }
            auto seq_for_offset = metadata_->GetSeqForOffset(stored.offset);
            if (!seq_for_offset.ok()) {
                return seq_for_offset.status();
            }
            if (seq_for_offset.value().has_value() && seq_for_offset.value().value() == seq) {
                const Message& message = stored.message;
                auto channel_result = LoadChannel(message.channel_id());
                if (!channel_result.ok()) {
                    return channel_result.status();
                }
                if (channel_result.value().has_value() && CanRead(channel_result.value().value(), principal) &&
                    (!only_my_recipient || HasRecipient(message, principal))) {
                    *response->add_messages() = message;
                }
            }
        }

        ++seq;
        next_seq = seq;
    }

    response->set_next_seq(next_seq);
    response->set_has_more(next_seq <= max_seq);
    return Status::Ok();
}

Status OpenEventCore::CreateChannel(const CreateChannelRequest& request, CreateChannelResponse* response)
{
    Status auth = Authenticate(request.principal(), request.token());
    if (!auth.ok()) {
        return auth;
    }
    Status visibility_status = ValidateVisibility(request.visibility());
    if (!visibility_status.ok()) {
        return visibility_status;
    }

    std::lock_guard<std::mutex> lock(write_mu_);
    auto id_result = metadata_->AllocateChannelId();
    if (!id_result.ok()) {
        return id_result.status();
    }

    ChannelInfo channel;
    channel.set_channel_id(id_result.value());
    channel.set_name(request.name());
    channel.set_visibility(request.visibility());
    channel.set_protocol(request.protocol());
    channel.set_description(request.description());
    channel.set_creator(request.principal());

    std::unordered_set<uint64_t> seen;
    channel.add_members(request.principal());
    seen.insert(request.principal());
    for (uint64_t member : request.members()) {
        if (seen.insert(member).second) {
            channel.add_members(member);
        }
    }

    Status status = metadata_->PutChannel(channel);
    if (!status.ok()) {
        return status;
    }
    *response->mutable_channel() = channel;
    return Status::Ok();
}

Status OpenEventCore::GetChannel(const GetChannelRequest& request, GetChannelResponse* response)
{
    Status auth = Authenticate(request.principal(), request.token());
    if (!auth.ok()) {
        return auth;
    }

    auto channel_result = LoadChannel(request.channel_id());
    if (!channel_result.ok()) {
        return channel_result.status();
    }
    if (!channel_result.value().has_value()) {
        return Status(grpc::StatusCode::NOT_FOUND, "channel not found");
    }
    if (!CanRead(channel_result.value().value(), request.principal())) {
        return Status(grpc::StatusCode::PERMISSION_DENIED, "channel not visible");
    }

    *response->mutable_channel() = channel_result.value().value();
    return Status::Ok();
}

Status OpenEventCore::ListChannels(const ListChannelsRequest& request, ListChannelsResponse* response)
{
    Status auth = Authenticate(request.principal(), request.token());
    if (!auth.ok()) {
        return auth;
    }
    Status filter_status = ValidateFilter(request.filter());
    if (!filter_status.ok()) {
        return filter_status;
    }

    response->clear_channels();
    if (request.filter() == CHANNEL_FILTER_ALL) {
        *response->add_channels() = SystemChannel();
    }

    auto channels = metadata_->ListChannels();
    if (!channels.ok()) {
        return channels.status();
    }

    for (const auto& channel : channels.value()) {
        if (!CanRead(channel, request.principal())) {
            continue;
        }
        if (request.filter() == CHANNEL_FILTER_JOINED && !IsMember(channel, request.principal())) {
            continue;
        }
        if (request.filter() == CHANNEL_FILTER_OWNED &&
            (!channel.has_creator() || channel.creator() != request.principal())) {
            continue;
        }
        *response->add_channels() = channel;
    }
    return Status::Ok();
}

Status OpenEventCore::AddMember(const AddMemberRequest& request, AddMemberResponse*)
{
    Status auth = Authenticate(request.principal(), request.token());
    if (!auth.ok()) {
        return auth;
    }
    if (request.channel_id() == 0) {
        return Status(grpc::StatusCode::PERMISSION_DENIED, "system channel cannot be modified");
    }

    std::lock_guard<std::mutex> lock(write_mu_);
    auto channel_result = LoadChannel(request.channel_id());
    if (!channel_result.ok()) {
        return channel_result.status();
    }
    if (!channel_result.value().has_value()) {
        return Status(grpc::StatusCode::NOT_FOUND, "channel not found");
    }

    ChannelInfo channel = channel_result.value().value();
    if (!channel.has_creator() || channel.creator() != request.principal()) {
        return Status(grpc::StatusCode::PERMISSION_DENIED, "only creator can add members");
    }
    if (IsMember(channel, request.target_principal())) {
        return Status(grpc::StatusCode::ALREADY_EXISTS, "member already exists");
    }

    channel.add_members(request.target_principal());
    return metadata_->PutChannel(channel);
}

Status OpenEventCore::RemoveMember(const RemoveMemberRequest& request, RemoveMemberResponse*)
{
    Status auth = Authenticate(request.principal(), request.token());
    if (!auth.ok()) {
        return auth;
    }
    if (request.channel_id() == 0) {
        return Status(grpc::StatusCode::PERMISSION_DENIED, "system channel cannot be modified");
    }

    std::lock_guard<std::mutex> lock(write_mu_);
    auto channel_result = LoadChannel(request.channel_id());
    if (!channel_result.ok()) {
        return channel_result.status();
    }
    if (!channel_result.value().has_value()) {
        return Status(grpc::StatusCode::NOT_FOUND, "channel not found");
    }

    ChannelInfo channel = channel_result.value().value();
    if (!channel.has_creator() || channel.creator() != request.principal()) {
        return Status(grpc::StatusCode::PERMISSION_DENIED, "only creator can remove members");
    }
    if (request.target_principal() == channel.creator()) {
        return Status(grpc::StatusCode::PERMISSION_DENIED, "creator cannot be removed");
    }

    auto* members = channel.mutable_members();
    auto it = std::find(members->begin(), members->end(), request.target_principal());
    if (it != members->end()) {
        members->erase(it);
    }
    return metadata_->PutChannel(channel);
}

Status OpenEventCore::AddToken(const AddTokenRequest& request, AddTokenResponse* response)
{
    std::string token;
    for (int attempt = 0; attempt < 16; ++attempt) {
        auto token_result = GenerateToken();
        if (!token_result.ok()) {
            return token_result.status();
        }
        token = token_result.value();
        auto existing = metadata_->GetPrincipalForToken(token);
        if (!existing.ok()) {
            return existing.status();
        }
        if (!existing.value().has_value()) {
            break;
        }
        token.clear();
    }
    if (token.empty()) {
        return Status(grpc::StatusCode::INTERNAL, "failed to generate unique token");
    }

    Status status = metadata_->PutToken(token, request.target_principal());
    if (!status.ok()) {
        return status;
    }

    response->mutable_binding()->set_token(token);
    response->mutable_binding()->set_principal(request.target_principal());
    return Status::Ok();
}

Status OpenEventCore::DeleteToken(const DeleteTokenRequest& request, DeleteTokenResponse*)
{
    return metadata_->DeleteToken(request.target_token());
}

Status OpenEventCore::ListTokens(const ListTokensRequest&, ListTokensResponse* response)
{
    auto bindings = metadata_->ListTokens();
    if (!bindings.ok()) {
        return bindings.status();
    }

    response->clear_bindings();
    for (const auto& binding : bindings.value()) {
        auto* item = response->add_bindings();
        item->set_token(binding.token);
        item->set_principal(binding.principal);
    }
    return Status::Ok();
}

Result<uint64_t> OpenEventCore::MaxSeq() const
{
    return metadata_->GetMaxSeq();
}

Status OpenEventCore::RecoverPending()
{
    std::lock_guard<std::mutex> lock(write_mu_);

    auto pending_result = metadata_->ListPendingSeqs();
    if (!pending_result.ok()) {
        return pending_result.status();
    }

    for (uint64_t seq : pending_result.value()) {
        auto max_seq = metadata_->GetMaxSeq();
        if (!max_seq.ok()) {
            return max_seq.status();
        }
        if (seq != max_seq.value() + 1) {
            return Status(grpc::StatusCode::INTERNAL, "pending seq is not the next commit point");
        }

        auto message_result = metadata_->GetPendingMessage(seq);
        if (!message_result.ok()) {
            return message_result.status();
        }
        if (!message_result.value().has_value()) {
            return Status(grpc::StatusCode::INTERNAL, "pending message missing");
        }
        const Message& message = message_result.value().value();
        if (message.seq() != seq) {
            return Status(grpc::StatusCode::INTERNAL, "pending message seq mismatch");
        }

        uint64_t offset = 0;
        auto pending_offset = metadata_->GetPendingOffset(seq);
        if (!pending_offset.ok()) {
            return pending_offset.status();
        }
        bool needs_append = true;
        if (pending_offset.value().has_value()) {
            offset = pending_offset.value().value();
            auto mapped_seq = metadata_->GetSeqForOffset(offset);
            if (!mapped_seq.ok()) {
                return mapped_seq.status();
            }
            if (mapped_seq.value().has_value() && mapped_seq.value().value() != seq) {
                return Status(grpc::StatusCode::INTERNAL, "pending offset is mapped to another seq");
            }

            auto fetch_result = message_store_->Fetch(offset, 1);
            if (!fetch_result.ok()) {
                return fetch_result.status();
            }
            const bool exact_match = !fetch_result.value().records.empty() &&
                                     fetch_result.value().records.front().offset == offset &&
                                     SameMessage(fetch_result.value().records.front().message, message);
            if (mapped_seq.value().has_value() && !exact_match) {
                return Status(grpc::StatusCode::INTERNAL, "mapped pending offset message mismatch");
            }
            if (!exact_match) {
                needs_append = true;
            } else {
                needs_append = false;
            }
        }

        if (needs_append) {
            auto append_result = message_store_->Append(message);
            if (!append_result.ok()) {
                return append_result.status();
            }
            offset = append_result.value();

            Status status = metadata_->PutPendingOffset(seq, offset);
            if (!status.ok()) {
                return status;
            }
        }

        Status status = metadata_->CommitMessage(seq, offset, message.channel_id(), message.ts_ms());
        if (!status.ok()) {
            return status;
        }
    }

    return Status::Ok();
}

Result<std::optional<ChannelInfo>> OpenEventCore::LoadChannel(uint64_t channel_id) const
{
    if (channel_id == 0) {
        return std::optional<ChannelInfo>{SystemChannel()};
    }
    return metadata_->GetChannel(channel_id);
}

bool OpenEventCore::CanRead(const ChannelInfo& channel, uint64_t principal) const
{
    if (channel.channel_id() == 0) {
        return true;
    }
    if (channel.visibility() == VISIBILITY_PUBLIC || channel.visibility() == VISIBILITY_PROTECTED) {
        return true;
    }
    return IsMember(channel, principal);
}

bool OpenEventCore::CanWrite(const ChannelInfo& channel, uint64_t principal) const
{
    if (channel.channel_id() == 0) {
        return false;
    }
    if (channel.visibility() == VISIBILITY_PUBLIC) {
        return true;
    }
    return IsMember(channel, principal);
}

bool OpenEventCore::IsMember(const ChannelInfo& channel, uint64_t principal) const
{
    return Contains(channel.members(), principal);
}

Status OpenEventCore::ValidateRecipients(const ChannelInfo& channel,
                                         const google::protobuf::RepeatedField<uint64_t>& recipients) const
{
    for (uint64_t recipient : recipients) {
        if (!IsMember(channel, recipient)) {
            return Status(grpc::StatusCode::INVALID_ARGUMENT, "recipient must be channel member");
        }
    }
    return Status::Ok();
}

bool OpenEventCore::HasRecipient(const Message& message, uint64_t principal) const
{
    return Contains(message.recipients(), principal);
}

bool OpenEventCore::SameMessage(const Message& lhs, const Message& rhs) const
{
    if (lhs.seq() != rhs.seq() || lhs.channel_id() != rhs.channel_id() || lhs.principal() != rhs.principal() ||
        lhs.payload() != rhs.payload() || lhs.ts_ms() != rhs.ts_ms() || lhs.recipients_size() != rhs.recipients_size()) {
        return false;
    }
    for (int i = 0; i < lhs.recipients_size(); ++i) {
        if (lhs.recipients(i) != rhs.recipients(i)) {
            return false;
        }
    }
    return true;
}

ChannelInfo OpenEventCore::SystemChannel() const
{
    ChannelInfo channel;
    channel.set_channel_id(0);
    channel.set_visibility(VISIBILITY_PROTECTED);
    return channel;
}

Status OpenEventCore::ValidateVisibility(Visibility visibility) const
{
    if (visibility == VISIBILITY_PUBLIC || visibility == VISIBILITY_PROTECTED || visibility == VISIBILITY_PRIVATE) {
        return Status::Ok();
    }
    return Status(grpc::StatusCode::INVALID_ARGUMENT, "invalid visibility");
}

Status OpenEventCore::ValidateFilter(ChannelFilter filter) const
{
    if (filter == CHANNEL_FILTER_ALL || filter == CHANNEL_FILTER_JOINED || filter == CHANNEL_FILTER_OWNED) {
        return Status::Ok();
    }
    return Status(grpc::StatusCode::INVALID_ARGUMENT, "invalid channel filter");
}

}  // namespace openevent

#include "service/open_event_core.h"

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <iomanip>
#include <limits>
#include <sstream>
#include <unordered_set>
#include <utility>

#include <sys/random.h>

namespace openevent {
namespace {

constexpr size_t kResponseEnvelopeBytes = 1024 * 1024;
constexpr size_t kMaxObjectNameBytes = 255;
constexpr size_t kMaxObjectTypeBytes = 255;
constexpr size_t kMaxObjectDescriptionBytes = 4096;
constexpr size_t kMaxObjectBytes = 4 * 1024 * 1024;
constexpr int kMaxObjectKeys = 1024;

uint64_t NowMs()
{
    using namespace std::chrono;
    return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
}

Result<std::string> GenerateToken()
{
    unsigned char bytes[16];
    size_t filled = 0;
    while (filled < sizeof(bytes)) {
        ssize_t count = getrandom(bytes + filled, sizeof(bytes) - filled, 0);
        if (count < 0) {
            if (errno == EINTR) {
                continue;
            }
            return Status(grpc::StatusCode::INTERNAL, std::string("getrandom failed: ") + std::strerror(errno));
        }
        filled += static_cast<size_t>(count);
    }

    std::ostringstream oss;
    oss << std::hex << std::setfill('0');
    for (size_t i = 0; i < sizeof(bytes); ++i) {
        oss << std::setw(2) << static_cast<unsigned int>(bytes[i]);
        if (i == 3 || i == 5 || i == 7 || i == 9) {
            oss << '-';
        }
    }
    return oss.str();
}

Result<std::string> GenerateObjectToken()
{
    unsigned char bytes[32];
    size_t filled = 0;
    while (filled < sizeof(bytes)) {
        const ssize_t count = getrandom(bytes + filled, sizeof(bytes) - filled, 0);
        if (count < 0) {
            if (errno == EINTR) {
                continue;
            }
            return Status(grpc::StatusCode::INTERNAL,
                          std::string("getrandom failed: ") + std::strerror(errno));
        }
        filled += static_cast<size_t>(count);
    }

    constexpr char kBase64Url[] =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    std::string token;
    token.reserve(43);
    size_t index = 0;
    while (index + 3 <= sizeof(bytes)) {
        const uint32_t value = (static_cast<uint32_t>(bytes[index]) << 16) |
                               (static_cast<uint32_t>(bytes[index + 1]) << 8) |
                               static_cast<uint32_t>(bytes[index + 2]);
        token.push_back(kBase64Url[(value >> 18) & 0x3f]);
        token.push_back(kBase64Url[(value >> 12) & 0x3f]);
        token.push_back(kBase64Url[(value >> 6) & 0x3f]);
        token.push_back(kBase64Url[value & 0x3f]);
        index += 3;
    }
    const size_t remaining = sizeof(bytes) - index;
    if (remaining == 1) {
        const uint32_t value = static_cast<uint32_t>(bytes[index]) << 16;
        token.push_back(kBase64Url[(value >> 18) & 0x3f]);
        token.push_back(kBase64Url[(value >> 12) & 0x3f]);
    } else if (remaining == 2) {
        const uint32_t value = (static_cast<uint32_t>(bytes[index]) << 16) |
                               (static_cast<uint32_t>(bytes[index + 1]) << 8);
        token.push_back(kBase64Url[(value >> 18) & 0x3f]);
        token.push_back(kBase64Url[(value >> 12) & 0x3f]);
        token.push_back(kBase64Url[(value >> 6) & 0x3f]);
    }
    return token;
}

bool SameObject(const StoredObject& left, const StoredObject& right)
{
    return left.object_id == right.object_id && left.object_token == right.object_token &&
           left.creator_principal == right.creator_principal && left.name == right.name &&
           left.type == right.type && left.description == right.description &&
           left.nbytes == right.nbytes && left.sha256 == right.sha256;
}

std::string EncodeTokenPageToken(const std::string& token)
{
    constexpr char kHex[] = "0123456789abcdef";
    std::string encoded = "v1:";
    encoded.reserve(encoded.size() + token.size() * 2);
    for (unsigned char ch : token) {
        encoded.push_back(kHex[ch >> 4]);
        encoded.push_back(kHex[ch & 0x0f]);
    }
    return encoded;
}

int HexValue(char ch)
{
    if (ch >= '0' && ch <= '9') {
        return ch - '0';
    }
    if (ch >= 'a' && ch <= 'f') {
        return ch - 'a' + 10;
    }
    return -1;
}

Result<std::string> DecodeTokenPageToken(const std::string& page_token)
{
    if (page_token.empty()) {
        return std::string{};
    }

    constexpr size_t kGeneratedTokenSize = 36;
    constexpr char kPrefix[] = "v1:";
    constexpr size_t kPrefixSize = sizeof(kPrefix) - 1;
    if (page_token.size() != kPrefixSize + kGeneratedTokenSize * 2 ||
        page_token.compare(0, kPrefixSize, kPrefix) != 0) {
        return Status(grpc::StatusCode::INVALID_ARGUMENT, "invalid page_token");
    }

    std::string decoded;
    decoded.reserve(kGeneratedTokenSize);
    for (size_t i = kPrefixSize; i < page_token.size(); i += 2) {
        const int high = HexValue(page_token[i]);
        const int low = HexValue(page_token[i + 1]);
        if (high < 0 || low < 0) {
            return Status(grpc::StatusCode::INVALID_ARGUMENT, "invalid page_token");
        }
        decoded.push_back(static_cast<char>((high << 4) | low));
    }
    return decoded;
}

template <typename Repeated>
bool Contains(const Repeated& values, uint64_t value)
{
    return std::find(values.begin(), values.end(), value) != values.end();
}

}  // namespace

OpenEventCore::OpenEventCore(std::unique_ptr<UnifiedStorage> storage,
                             size_t max_payload_bytes,
                             uint64_t max_scan_records,
                             size_t response_soft_limit_bytes,
                             FatalErrorHandler fatal_error_handler)
    : storage_(std::move(storage)),
      max_payload_bytes_(max_payload_bytes),
      max_scan_records_(max_scan_records),
      response_soft_limit_bytes_(
          response_soft_limit_bytes == 0
              ? (max_payload_bytes > std::numeric_limits<size_t>::max() - kResponseEnvelopeBytes
                     ? std::numeric_limits<size_t>::max()
                     : max_payload_bytes + kResponseEnvelopeBytes)
              : response_soft_limit_bytes),
      fatal_error_handler_(std::move(fatal_error_handler))
{
}

Result<ReadSnapshot> OpenEventCore::CreateLinearizedSnapshot()
{
    std::lock_guard<std::mutex> lock(coordinator_mu_);
    return storage_->CreateSnapshot();
}

Status OpenEventCore::Authenticate(const ReadSnapshot& snapshot,
                                   uint64_t principal,
                                   const std::string& token) const
{
    if (token.empty()) {
        return Status(grpc::StatusCode::UNAUTHENTICATED, "token is required");
    }
    auto result = storage_->GetPrincipalForToken(snapshot, token);
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
    auto snapshot_result = CreateLinearizedSnapshot();
    if (!snapshot_result.ok()) {
        return snapshot_result.status();
    }
    ReadSnapshot snapshot = std::move(snapshot_result.value());
    Status auth = Authenticate(snapshot, request.principal(), request.token());
    if (!auth.ok()) {
        return auth;
    }
    auto max_seq = storage_->GetMaxSeq(snapshot);
    if (!max_seq.ok()) {
        return max_seq.status();
    }
    response->set_max_seq(max_seq.value());
    response->set_min_seq(max_seq.value() == 0 ? 0 : 1);
    return Status::Ok();
}

Status OpenEventCore::BuildPublishBatch(const ReadSnapshot& snapshot,
                                        uint64_t principal,
                                        uint64_t channel_id,
                                        uint64_t seq,
                                        const google::protobuf::RepeatedField<uint64_t>& recipients,
                                        const std::string& payload,
                                        const google::protobuf::RepeatedPtrField<ObjectKey>& object_keys,
                                        uint64_t ts_ms,
                                        rocksdb::WriteBatch* batch) const
{
    if (channel_id == 0) {
        return Status(grpc::StatusCode::PERMISSION_DENIED, "system channel is read-only");
    }
    auto channel_result = LoadChannel(snapshot, channel_id);
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
    for (const auto& object_key : object_keys) {
        auto object = LoadAuthorizedObject(snapshot, object_key.object_id(), object_key.object_token());
        if (!object.ok()) {
            return object.status();
        }
    }

    EventMessage message;
    message.set_seq(seq);
    message.set_channel_id(channel_id);
    message.set_principal(principal);
    for (uint64_t recipient : recipients) {
        message.add_recipients(recipient);
    }
    message.set_payload(payload);
    message.set_ts_ms(ts_ms);
    for (const auto& object_key : object_keys) {
        *message.add_object_keys() = object_key;
    }

    Status status = storage_->PutMessage(batch, message);
    if (!status.ok()) {
        return status;
    }
    return storage_->SetMaxSeq(batch, seq);
}

Status OpenEventCore::CommitBatch(rocksdb::WriteBatch* batch)
{
    Status status = storage_->Commit(batch);
    if (status.ok()) {
        commit_generation_.fetch_add(1, std::memory_order_release);
        commit_cv_.notify_all();
    }
    return status;
}

Status OpenEventCore::Publish(const PublishRequest& request, PublishResponse*)
{
    const uint64_t ts_ms = NowMs();
    Status payload_status = ValidatePayloadSize(request.payload());
    if (!payload_status.ok()) {
        return payload_status;
    }
    Status object_keys_status = ValidateObjectKeysShape(request.object_keys());
    if (!object_keys_status.ok()) {
        return object_keys_status;
    }

    std::lock_guard<std::mutex> lock(coordinator_mu_);
    auto snapshot_result = storage_->CreateSnapshot();
    if (!snapshot_result.ok()) {
        return snapshot_result.status();
    }
    ReadSnapshot snapshot = std::move(snapshot_result.value());
    Status auth = Authenticate(snapshot, request.principal(), request.token());
    if (!auth.ok()) {
        return auth;
    }
    auto max_seq = storage_->GetMaxSeq(snapshot);
    if (!max_seq.ok()) {
        return max_seq.status();
    }
    if (request.seq() != max_seq.value() + 1) {
        return Status(grpc::StatusCode::ABORTED, "seq must equal max_seq + 1");
    }

    rocksdb::WriteBatch batch;
    Status status = BuildPublishBatch(snapshot,
                                      request.principal(),
                                      request.channel_id(),
                                      request.seq(),
                                      request.recipients(),
                                      request.payload(),
                                      request.object_keys(),
                                      ts_ms,
                                      &batch);
    if (!status.ok()) {
        return status;
    }
    return CommitBatch(&batch);
}

Status OpenEventCore::PublishAutoSeq(const PublishAutoSeqRequest& request, PublishAutoSeqResponse* response)
{
    const uint64_t ts_ms = NowMs();
    Status payload_status = ValidatePayloadSize(request.payload());
    if (!payload_status.ok()) {
        return payload_status;
    }
    Status object_keys_status = ValidateObjectKeysShape(request.object_keys());
    if (!object_keys_status.ok()) {
        return object_keys_status;
    }

    std::lock_guard<std::mutex> lock(coordinator_mu_);
    auto snapshot_result = storage_->CreateSnapshot();
    if (!snapshot_result.ok()) {
        return snapshot_result.status();
    }
    ReadSnapshot snapshot = std::move(snapshot_result.value());
    Status auth = Authenticate(snapshot, request.principal(), request.token());
    if (!auth.ok()) {
        return auth;
    }
    auto max_seq = storage_->GetMaxSeq(snapshot);
    if (!max_seq.ok()) {
        return max_seq.status();
    }
    const uint64_t seq = max_seq.value() + 1;

    rocksdb::WriteBatch batch;
    Status status = BuildPublishBatch(snapshot,
                                      request.principal(),
                                      request.channel_id(),
                                      seq,
                                      request.recipients(),
                                      request.payload(),
                                      request.object_keys(),
                                      ts_ms,
                                      &batch);
    if (!status.ok()) {
        return status;
    }
    status = CommitBatch(&batch);
    if (!status.ok()) {
        return status;
    }
    response->set_seq(seq);
    return Status::Ok();
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

Status OpenEventCore::ValidateObjectKeysShape(
    const google::protobuf::RepeatedPtrField<ObjectKey>& object_keys) const
{
    if (object_keys.size() > kMaxObjectKeys) {
        return Status(grpc::StatusCode::INVALID_ARGUMENT, "object_keys must contain at most 1024 items");
    }
    for (const auto& object_key : object_keys) {
        if (object_key.object_id() == 0 || object_key.object_token().empty()) {
            return Status(grpc::StatusCode::INVALID_ARGUMENT,
                          "each ObjectKey requires nonzero object_id and nonempty object_token");
        }
    }
    return Status::Ok();
}

Result<StoredObject> OpenEventCore::LoadAuthorizedObject(const ReadSnapshot& snapshot,
                                                        uint64_t object_id,
                                                        const std::string& object_token) const
{
    if (object_id == 0 || object_token.empty()) {
        return Status(grpc::StatusCode::INVALID_ARGUMENT,
                      "object_id must be nonzero and object_token must be nonempty");
    }
    auto object = storage_->GetCommittedObject(snapshot, object_id);
    if (!object.ok()) {
        return object.status().code() == grpc::StatusCode::DATA_LOSS
                   ? FatalStorageError(object.status())
                   : object.status();
    }
    if (!object.value().has_value() ||
        !ObjectFileStore::ConstantTimeEquals(object.value()->object_token, object_token)) {
        return Status(grpc::StatusCode::NOT_FOUND, "object capability not found");
    }
    return object.value().value();
}

Status OpenEventCore::FatalStorageError(const Status& status) const
{
    bool expected = false;
    if (fatal_error_triggered_.compare_exchange_strong(expected, true, std::memory_order_acq_rel) &&
        fatal_error_handler_) {
        fatal_error_handler_(status);
    }
    return status;
}

Status OpenEventCore::CleanupPreparingObject(uint64_t object_id, const Status& result)
{
    Status file_status = storage_->CleanupObjectFiles(object_id);
    if (!file_status.ok()) {
        return file_status.code() == grpc::StatusCode::DATA_LOSS ? FatalStorageError(file_status) : file_status;
    }

    std::lock_guard<std::mutex> lock(coordinator_mu_);
    rocksdb::WriteBatch batch;
    storage_->DeletePreparingObject(&batch, object_id);
    Status cleanup_status = CommitBatch(&batch);
    return cleanup_status.ok() ? result : cleanup_status;
}

Status OpenEventCore::WriteObject(const WriteObjectRequest& request, WriteObjectResponse* response)
{
    if (request.name().empty() || request.name().size() > kMaxObjectNameBytes ||
        request.type().empty() || request.type().size() > kMaxObjectTypeBytes ||
        request.description().size() > kMaxObjectDescriptionBytes || request.data().empty() ||
        request.data().size() > kMaxObjectBytes) {
        return Status(grpc::StatusCode::INVALID_ARGUMENT, "invalid object metadata or data size");
    }

    auto object_token = GenerateObjectToken();
    if (!object_token.ok()) {
        return object_token.status();
    }
    auto sha256 = ObjectFileStore::Sha256(request.data());
    if (!sha256.ok()) {
        return sha256.status();
    }

    StoredObject object;
    object.object_token = std::move(object_token.value());
    object.creator_principal = request.principal();
    object.name = request.name();
    object.type = request.type();
    object.description = request.description();
    object.nbytes = request.data().size();
    object.sha256 = std::move(sha256.value());

    {
        std::lock_guard<std::mutex> lock(coordinator_mu_);
        auto snapshot_result = storage_->CreateSnapshot();
        if (!snapshot_result.ok()) {
            return snapshot_result.status();
        }
        ReadSnapshot snapshot = std::move(snapshot_result.value());
        Status auth = Authenticate(snapshot, request.principal(), request.token());
        if (!auth.ok()) {
            return auth;
        }
        auto next_object_id = storage_->GetNextObjectId(snapshot);
        if (!next_object_id.ok()) {
            return next_object_id.status();
        }
        if (next_object_id.value() == std::numeric_limits<uint64_t>::max()) {
            return Status(grpc::StatusCode::RESOURCE_EXHAUSTED, "object ID space exhausted");
        }
        object.object_id = next_object_id.value();

        rocksdb::WriteBatch batch;
        Status status = storage_->PutPreparingObject(&batch, object);
        if (!status.ok()) {
            return status;
        }
        status = storage_->SetNextObjectId(&batch, object.object_id + 1);
        if (!status.ok()) {
            return status;
        }
        status = CommitBatch(&batch);
        if (!status.ok()) {
            return status;
        }
    }

    Status file_status = storage_->WriteObjectFile(object.object_id, request.data());
    if (!file_status.ok()) {
        if (file_status.code() == grpc::StatusCode::DATA_LOSS) {
            return FatalStorageError(file_status);
        }
        return CleanupPreparingObject(object.object_id, file_status);
    }

    Status final_status = Status::Ok();
    bool commit_attempted = false;
    {
        std::lock_guard<std::mutex> lock(coordinator_mu_);
        auto snapshot_result = storage_->CreateSnapshot();
        if (!snapshot_result.ok()) {
            final_status = snapshot_result.status();
        } else {
            ReadSnapshot snapshot = std::move(snapshot_result.value());
            final_status = Authenticate(snapshot, request.principal(), request.token());
            if (final_status.ok()) {
                auto preparing = storage_->GetPreparingObject(snapshot, object.object_id);
                if (!preparing.ok()) {
                    if (preparing.status().code() == grpc::StatusCode::DATA_LOSS) {
                        return FatalStorageError(preparing.status());
                    }
                    final_status = preparing.status();
                } else if (!preparing.value().has_value() ||
                           !SameObject(preparing.value().value(), object)) {
                    return FatalStorageError(Status(
                        grpc::StatusCode::DATA_LOSS,
                        "preparing object metadata changed unexpectedly"));
                } else {
                    rocksdb::WriteBatch batch;
                    final_status = storage_->PutCommittedObject(&batch, object);
                    if (final_status.ok()) {
                        commit_attempted = true;
                        final_status = CommitBatch(&batch);
                    }
                    if (final_status.ok()) {
                        response->set_object_id(object.object_id);
                        response->set_object_token(object.object_token);
                        return Status::Ok();
                    }
                }
            }
        }
    }
    if (commit_attempted) {
        return final_status;
    }
    return CleanupPreparingObject(object.object_id, final_status);
}

Status OpenEventCore::GetObjectMetadata(const GetObjectMetadataRequest& request,
                                        GetObjectMetadataResponse* response)
{
    auto snapshot_result = CreateLinearizedSnapshot();
    if (!snapshot_result.ok()) {
        return snapshot_result.status();
    }
    ReadSnapshot snapshot = std::move(snapshot_result.value());
    auto object = LoadAuthorizedObject(snapshot, request.object_id(), request.object_token());
    if (!object.ok()) {
        return object.status();
    }
    response->set_name(object.value().name);
    response->set_type(object.value().type);
    response->set_description(object.value().description);
    response->set_nbytes(object.value().nbytes);
    return Status::Ok();
}

Status OpenEventCore::ReadObject(const ReadObjectRequest& request, ReadObjectResponse* response)
{
    if (request.nbytes() == 0) {
        return Status(grpc::StatusCode::INVALID_ARGUMENT, "nbytes must be greater than 0");
    }

    StoredObject object;
    {
        auto snapshot_result = CreateLinearizedSnapshot();
        if (!snapshot_result.ok()) {
            return snapshot_result.status();
        }
        ReadSnapshot snapshot = std::move(snapshot_result.value());
        auto loaded = LoadAuthorizedObject(snapshot, request.object_id(), request.object_token());
        if (!loaded.ok()) {
            return loaded.status();
        }
        object = std::move(loaded.value());
    }

    if (request.offset() > object.nbytes) {
        return Status(grpc::StatusCode::OUT_OF_RANGE, "offset exceeds object size");
    }
    auto data = storage_->ReadObjectFile(object);
    if (!data.ok()) {
        return data.status().code() == grpc::StatusCode::DATA_LOSS ? FatalStorageError(data.status())
                                                                   : data.status();
    }
    const uint64_t remaining = object.nbytes - request.offset();
    const size_t count = static_cast<size_t>(std::min(request.nbytes(), remaining));
    response->set_data(data.value().substr(static_cast<size_t>(request.offset()), count));
    return Status::Ok();
}

Status OpenEventCore::Fetch(const FetchRequest& request, FetchResponse* response)
{
    if (request.limit() == 0 || request.limit() > 1000) {
        return Status(grpc::StatusCode::INVALID_ARGUMENT, "limit must be in 1..1000");
    }
    auto snapshot_result = CreateLinearizedSnapshot();
    if (!snapshot_result.ok()) {
        return snapshot_result.status();
    }
    ReadSnapshot snapshot = std::move(snapshot_result.value());
    Status auth = Authenticate(snapshot, request.principal(), request.token());
    if (!auth.ok()) {
        return auth;
    }
    return FetchVisible(snapshot,
                        request.principal(),
                        request.from_seq(),
                        request.limit(),
                        request.only_my_recipient(),
                        request.channels(),
                        response);
}

Status OpenEventCore::FetchVisible(const ReadSnapshot& snapshot,
                                   uint64_t principal,
                                   uint64_t from_seq,
                                   uint32_t limit,
                                   bool only_my_recipient,
                                   const google::protobuf::RepeatedField<uint64_t>& channels,
                                   FetchResponse* response) const
{
    auto max_seq_result = storage_->GetMaxSeq(snapshot);
    if (!max_seq_result.ok()) {
        return max_seq_result.status();
    }
    const uint64_t max_seq = max_seq_result.value();
    response->clear_messages();
    response->set_last_seq(max_seq);
    if (from_seq == 0 || from_seq > max_seq) {
        response->set_next_seq(max_seq + 1);
        return Status::Ok();
    }

    size_t response_bytes = 0;
    auto next_seq = storage_->ScanMessages(
        snapshot,
        from_seq,
        max_seq,
        max_scan_records_,
        [&](const EventMessage& message) -> Result<MessageScanAction> {
            if (!channels.empty() && !Contains(channels, message.channel_id())) {
                return MessageScanAction::kContinue;
            }
            auto channel_result = LoadChannel(snapshot, message.channel_id());
            if (!channel_result.ok()) {
                return channel_result.status();
            }
            if (!channel_result.value().has_value() ||
                !CanRead(channel_result.value().value(), principal) ||
                (only_my_recipient && !HasRecipient(message, principal))) {
                return MessageScanAction::kContinue;
            }

            const size_t message_bytes = message.ByteSizeLong();
            if (response->messages_size() > 0 &&
                message_bytes > response_soft_limit_bytes_ - std::min(response_bytes, response_soft_limit_bytes_)) {
                return MessageScanAction::kStopBefore;
            }
            *response->add_messages() = message;
            response_bytes += message_bytes;
            if (response->messages_size() >= static_cast<int>(limit)) {
                return MessageScanAction::kStopAfter;
            }
            return MessageScanAction::kContinue;
        });
    if (!next_seq.ok()) {
        return next_seq.status();
    }
    response->set_next_seq(next_seq.value());
    return Status::Ok();
}

Status OpenEventCore::CreateChannel(const CreateChannelRequest& request, CreateChannelResponse* response)
{
    Status visibility_status = ValidateVisibility(request.visibility());
    if (!visibility_status.ok()) {
        return visibility_status;
    }

    std::lock_guard<std::mutex> lock(coordinator_mu_);
    auto snapshot_result = storage_->CreateSnapshot();
    if (!snapshot_result.ok()) {
        return snapshot_result.status();
    }
    ReadSnapshot snapshot = std::move(snapshot_result.value());
    Status auth = Authenticate(snapshot, request.principal(), request.token());
    if (!auth.ok()) {
        return auth;
    }
    auto next_channel_id = storage_->GetNextChannelId(snapshot);
    if (!next_channel_id.ok()) {
        return next_channel_id.status();
    }

    ChannelInfo channel;
    channel.set_channel_id(next_channel_id.value());
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

    rocksdb::WriteBatch batch;
    Status status = storage_->PutChannel(&batch, channel);
    if (!status.ok()) {
        return status;
    }
    status = storage_->SetNextChannelId(&batch, next_channel_id.value() + 1);
    if (!status.ok()) {
        return status;
    }
    status = CommitBatch(&batch);
    if (!status.ok()) {
        return status;
    }
    *response->mutable_channel() = channel;
    return Status::Ok();
}

Status OpenEventCore::GetChannel(const GetChannelRequest& request, GetChannelResponse* response)
{
    auto snapshot_result = CreateLinearizedSnapshot();
    if (!snapshot_result.ok()) {
        return snapshot_result.status();
    }
    ReadSnapshot snapshot = std::move(snapshot_result.value());
    Status auth = Authenticate(snapshot, request.principal(), request.token());
    if (!auth.ok()) {
        return auth;
    }
    auto channel_result = LoadChannel(snapshot, request.channel_id());
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
    Status filter_status = ValidateFilter(request.filter());
    if (!filter_status.ok()) {
        return filter_status;
    }
    auto snapshot_result = CreateLinearizedSnapshot();
    if (!snapshot_result.ok()) {
        return snapshot_result.status();
    }
    ReadSnapshot snapshot = std::move(snapshot_result.value());
    Status auth = Authenticate(snapshot, request.principal(), request.token());
    if (!auth.ok()) {
        return auth;
    }

    response->clear_channels();
    if (request.filter() == CHANNEL_FILTER_ALL) {
        *response->add_channels() = SystemChannel();
    }
    auto channels = storage_->ListChannels(snapshot);
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
    std::lock_guard<std::mutex> lock(coordinator_mu_);
    auto snapshot_result = storage_->CreateSnapshot();
    if (!snapshot_result.ok()) {
        return snapshot_result.status();
    }
    ReadSnapshot snapshot = std::move(snapshot_result.value());
    Status auth = Authenticate(snapshot, request.principal(), request.token());
    if (!auth.ok()) {
        return auth;
    }
    if (request.channel_id() == 0) {
        return Status(grpc::StatusCode::PERMISSION_DENIED, "system channel cannot be modified");
    }
    auto channel_result = LoadChannel(snapshot, request.channel_id());
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

    rocksdb::WriteBatch batch;
    Status status = storage_->PutChannel(&batch, channel);
    if (!status.ok()) {
        return status;
    }
    return CommitBatch(&batch);
}

Status OpenEventCore::RemoveMember(const RemoveMemberRequest& request, RemoveMemberResponse*)
{
    std::lock_guard<std::mutex> lock(coordinator_mu_);
    auto snapshot_result = storage_->CreateSnapshot();
    if (!snapshot_result.ok()) {
        return snapshot_result.status();
    }
    ReadSnapshot snapshot = std::move(snapshot_result.value());
    Status auth = Authenticate(snapshot, request.principal(), request.token());
    if (!auth.ok()) {
        return auth;
    }
    if (request.channel_id() == 0) {
        return Status(grpc::StatusCode::PERMISSION_DENIED, "system channel cannot be modified");
    }
    auto channel_result = LoadChannel(snapshot, request.channel_id());
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

    rocksdb::WriteBatch batch;
    Status status = storage_->PutChannel(&batch, channel);
    if (!status.ok()) {
        return status;
    }
    return CommitBatch(&batch);
}

Status OpenEventCore::AddToken(const AddTokenRequest& request, AddTokenResponse* response)
{
    std::lock_guard<std::mutex> lock(coordinator_mu_);
    auto snapshot_result = storage_->CreateSnapshot();
    if (!snapshot_result.ok()) {
        return snapshot_result.status();
    }
    ReadSnapshot snapshot = std::move(snapshot_result.value());

    std::string token;
    for (int attempt = 0; attempt < 16; ++attempt) {
        auto token_result = GenerateToken();
        if (!token_result.ok()) {
            return token_result.status();
        }
        token = token_result.value();
        auto existing = storage_->GetPrincipalForToken(snapshot, token);
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

    rocksdb::WriteBatch batch;
    Status status = storage_->PutToken(&batch, token, request.target_principal());
    if (!status.ok()) {
        return status;
    }
    status = CommitBatch(&batch);
    if (!status.ok()) {
        return status;
    }
    response->mutable_binding()->set_token(token);
    response->mutable_binding()->set_principal(request.target_principal());
    return Status::Ok();
}

Status OpenEventCore::DeleteToken(const DeleteTokenRequest& request, DeleteTokenResponse*)
{
    std::lock_guard<std::mutex> lock(coordinator_mu_);
    rocksdb::WriteBatch batch;
    storage_->DeleteToken(&batch, request.target_token());
    return CommitBatch(&batch);
}

Status OpenEventCore::ListTokens(const ListTokensRequest& request, ListTokensResponse* response)
{
    if (request.limit() == 0 || request.limit() > 1000) {
        return Status(grpc::StatusCode::INVALID_ARGUMENT, "limit must be in 1..1000");
    }
    auto start_after = DecodeTokenPageToken(request.page_token());
    if (!start_after.ok()) {
        return start_after.status();
    }
    auto snapshot_result = CreateLinearizedSnapshot();
    if (!snapshot_result.ok()) {
        return snapshot_result.status();
    }
    ReadSnapshot snapshot = std::move(snapshot_result.value());
    auto page = storage_->ListTokens(snapshot, start_after.value(), request.limit());
    if (!page.ok()) {
        return page.status();
    }

    response->clear_bindings();
    response->clear_next_page_token();
    for (const auto& binding : page.value().bindings) {
        auto* item = response->add_bindings();
        item->set_token(binding.token);
        item->set_principal(binding.principal);
    }
    if (page.value().has_more) {
        response->set_next_page_token(EncodeTokenPageToken(page.value().bindings.back().token));
    }
    return Status::Ok();
}

Status OpenEventCore::ListMessages(const ListMessagesRequest& request, ListMessagesResponse* response)
{
    if (request.limit() == 0 || request.limit() > 1000) {
        return Status(grpc::StatusCode::INVALID_ARGUMENT, "limit must be in 1..1000");
    }
    auto snapshot_result = CreateLinearizedSnapshot();
    if (!snapshot_result.ok()) {
        return snapshot_result.status();
    }
    ReadSnapshot snapshot = std::move(snapshot_result.value());
    return ListAllMessages(snapshot, request.from_seq(), request.limit(), response);
}

Status OpenEventCore::ListAllMessages(const ReadSnapshot& snapshot,
                                      uint64_t from_seq,
                                      uint32_t limit,
                                      ListMessagesResponse* response) const
{
    auto max_seq_result = storage_->GetMaxSeq(snapshot);
    if (!max_seq_result.ok()) {
        return max_seq_result.status();
    }
    const uint64_t max_seq = max_seq_result.value();
    response->clear_messages();
    response->set_last_seq(max_seq);
    const uint64_t start_seq = from_seq == 0 ? 1 : from_seq;
    if (start_seq > max_seq) {
        response->set_next_seq(max_seq + 1);
        return Status::Ok();
    }

    size_t response_bytes = 0;
    auto next_seq = storage_->ScanMessages(
        snapshot,
        start_seq,
        max_seq,
        max_scan_records_,
        [&](const EventMessage& message) -> Result<MessageScanAction> {
            const size_t message_bytes = message.ByteSizeLong();
            if (response->messages_size() > 0 &&
                message_bytes > response_soft_limit_bytes_ - std::min(response_bytes, response_soft_limit_bytes_)) {
                return MessageScanAction::kStopBefore;
            }
            *response->add_messages() = message;
            response_bytes += message_bytes;
            if (response->messages_size() >= static_cast<int>(limit)) {
                return MessageScanAction::kStopAfter;
            }
            return MessageScanAction::kContinue;
        });
    if (!next_seq.ok()) {
        return next_seq.status();
    }
    response->set_next_seq(next_seq.value());
    return Status::Ok();
}

Status OpenEventCore::GetSubscriptionMaxSeq(uint64_t principal,
                                            const std::string& token,
                                            uint64_t* max_seq) const
{
    auto snapshot_result = storage_->CreateSnapshot();
    if (!snapshot_result.ok()) {
        return snapshot_result.status();
    }
    ReadSnapshot snapshot = std::move(snapshot_result.value());
    Status auth = Authenticate(snapshot, principal, token);
    if (!auth.ok()) {
        return auth;
    }
    auto result = storage_->GetMaxSeq(snapshot);
    if (!result.ok()) {
        return result.status();
    }
    *max_seq = result.value();
    return Status::Ok();
}

Status OpenEventCore::FetchSubscriptionBatch(uint64_t principal,
                                             const std::string& token,
                                             uint64_t from_seq,
                                             uint32_t limit,
                                             bool only_my_recipient,
                                             FetchResponse* response) const
{
    auto snapshot_result = storage_->CreateSnapshot();
    if (!snapshot_result.ok()) {
        return snapshot_result.status();
    }
    ReadSnapshot snapshot = std::move(snapshot_result.value());
    Status auth = Authenticate(snapshot, principal, token);
    if (!auth.ok()) {
        return auth;
    }
    google::protobuf::RepeatedField<uint64_t> channels;
    return FetchVisible(snapshot, principal, from_seq, limit, only_my_recipient, channels, response);
}

uint64_t OpenEventCore::CommitGeneration() const
{
    return commit_generation_.load(std::memory_order_acquire);
}

void OpenEventCore::WaitForCommit(uint64_t observed_generation, std::chrono::milliseconds timeout) const
{
    std::unique_lock<std::mutex> lock(commit_wait_mu_);
    commit_cv_.wait_for(lock, timeout, [&]() { return CommitGeneration() != observed_generation; });
}

Result<uint64_t> OpenEventCore::MaxSeq() const
{
    auto snapshot_result = storage_->CreateSnapshot();
    if (!snapshot_result.ok()) {
        return snapshot_result.status();
    }
    ReadSnapshot snapshot = std::move(snapshot_result.value());
    return storage_->GetMaxSeq(snapshot);
}

Result<std::optional<ChannelInfo>> OpenEventCore::LoadChannel(const ReadSnapshot& snapshot,
                                                             uint64_t channel_id) const
{
    if (channel_id == 0) {
        return std::optional<ChannelInfo>{SystemChannel()};
    }
    return storage_->GetChannel(snapshot, channel_id);
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

bool OpenEventCore::HasRecipient(const EventMessage& message, uint64_t principal) const
{
    return Contains(message.recipients(), principal);
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
    if (visibility == VISIBILITY_PUBLIC || visibility == VISIBILITY_PROTECTED ||
        visibility == VISIBILITY_PRIVATE) {
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

#include "service/open_event_core.h"

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <iomanip>
#include <sstream>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include <sys/random.h>

#include <grpcpp/server_context.h>

#include "common/object_limits.h"
#include "common/message_limits.h"

namespace openevent {
namespace {

constexpr uint32_t kMaxUuidBatch = 1024;

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
    unsigned char bytes[kObjectTokenRandomBytes];
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
    token.reserve(kObjectTokenEncodedBytes);
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
           left.nbytes == right.nbytes;
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
              ? kMessagePageSoftBytes
              : response_soft_limit_bytes),
      fatal_error_handler_(std::move(fatal_error_handler))
{
    auto snapshot = storage_->CreateSnapshot();
    if (snapshot.ok()) {
        subscription_snapshot_ =
            std::make_shared<ReadSnapshot>(std::move(snapshot.value()));
        subscription_snapshot_version_ = 1;
        subscription_snapshot_status_ = Status::Ok();
    } else {
        subscription_snapshot_status_ = snapshot.status();
        if (snapshot.status().requires_server_exit()) {
            FatalStorageError(snapshot.status());
        }
    }
    if (!fatal_error_triggered_.load(std::memory_order_acquire)) {
        subscription_snapshot_thread_ =
            std::thread([this]() { RefreshSubscriptionSnapshots(); });
    }
}

OpenEventCore::~OpenEventCore()
{
    {
        std::lock_guard<std::mutex> lock(subscription_snapshot_mu_);
        stop_subscription_snapshot_thread_ = true;
    }
    subscription_snapshot_cv_.notify_all();
    if (subscription_snapshot_thread_.joinable()) {
        subscription_snapshot_thread_.join();
    }
    subscription_snapshot_.reset();
}

Status OpenEventCore::EnsureAvailable() const
{
    if (fatal_error_triggered_.load(std::memory_order_acquire)) {
        return Status(grpc::StatusCode::UNAVAILABLE,
                      "server is shutting down due to a fatal storage error");
    }
    return Status::Ok();
}

Status OpenEventCore::CheckRpcActive(const grpc::ServerContext* context) const
{
    if (context == nullptr) {
        return Status::Ok();
    }

    const auto deadline = context->deadline();
    if (deadline != std::chrono::system_clock::time_point::max() &&
        std::chrono::system_clock::now() >= deadline) {
        return Status(grpc::StatusCode::DEADLINE_EXCEEDED, "RPC deadline exceeded");
    }
    if (context->IsCancelled()) {
        return Status(grpc::StatusCode::CANCELLED, "RPC cancelled");
    }
    return Status::Ok();
}

Result<ReadSnapshot> OpenEventCore::CreateLinearizedSnapshot()
{
    std::lock_guard<std::mutex> lock(coordinator_mu_);
    return storage_->CreateSnapshot();
}

Result<OpenEventCore::SubscriptionSnapshot> OpenEventCore::AcquireSubscriptionSnapshot() const
{
    Status available = EnsureAvailable();
    if (!available.ok()) {
        return available;
    }
    const uint64_t required_generation = commit_generation_.load(std::memory_order_acquire);
    std::unique_lock<std::mutex> lock(subscription_snapshot_mu_);
    subscription_snapshot_cv_.wait(lock, [&]() {
        return stop_subscription_snapshot_thread_ ||
               fatal_error_triggered_.load(std::memory_order_acquire) ||
               subscription_snapshot_generation_ >= required_generation ||
               (!subscription_snapshot_status_.ok() &&
                subscription_snapshot_attempt_generation_ >= required_generation);
    });
    if (!subscription_snapshot_status_.ok() &&
        subscription_snapshot_attempt_generation_ >= required_generation) {
        return subscription_snapshot_status_;
    }
    if (!subscription_snapshot_) {
        if (fatal_error_triggered_.load(std::memory_order_acquire)) {
            return Status(grpc::StatusCode::UNAVAILABLE,
                          "subscription snapshot is unavailable after fatal storage error");
        }
        return Status(grpc::StatusCode::UNAVAILABLE, "subscription snapshot is unavailable");
    }
    return SubscriptionSnapshot{subscription_snapshot_, subscription_snapshot_version_};
}

void OpenEventCore::RefreshSubscriptionSnapshots()
{
    constexpr auto kRefreshInterval = std::chrono::milliseconds(100);
    auto next_refresh = std::chrono::steady_clock::now() + kRefreshInterval;
    std::unique_lock<std::mutex> lock(subscription_snapshot_mu_);
    while (!stop_subscription_snapshot_thread_) {
        if (subscription_snapshot_cv_.wait_until(lock, next_refresh, [&]() {
                return stop_subscription_snapshot_thread_ ||
                       fatal_error_triggered_.load(std::memory_order_acquire);
            })) {
            break;
        }
        next_refresh += kRefreshInterval;

        const uint64_t generation = commit_generation_.load(std::memory_order_acquire);
        if (subscription_snapshot_ && generation == subscription_snapshot_generation_) {
            continue;
        }

        lock.unlock();
        auto snapshot = storage_->CreateSnapshot();
        lock.lock();
        if (fatal_error_triggered_.load(std::memory_order_acquire)) {
            break;
        }
        subscription_snapshot_attempt_generation_ = generation;
        if (snapshot.ok()) {
            subscription_snapshot_ =
                std::make_shared<ReadSnapshot>(std::move(snapshot.value()));
            subscription_snapshot_generation_ = generation;
            ++subscription_snapshot_version_;
            subscription_snapshot_status_ = Status::Ok();
        } else {
            subscription_snapshot_status_ = snapshot.status();
        }
        subscription_snapshot_cv_.notify_all();

        if (!snapshot.ok() && snapshot.status().requires_server_exit()) {
            const Status fatal_status = snapshot.status();
            lock.unlock();
            FatalStorageError(fatal_status);
            lock.lock();
            break;
        }

        const auto now = std::chrono::steady_clock::now();
        if (next_refresh <= now) {
            next_refresh = now + kRefreshInterval;
        }
    }
}

Status OpenEventCore::Authenticate(const ReadSnapshot& snapshot,
                                   uint64_t principal,
                                   const std::string& token) const
{
    if (principal == 0) {
        return Status(grpc::StatusCode::INVALID_ARGUMENT, "principal must be greater than 0");
    }
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
    Status available = EnsureAvailable();
    if (!available.ok()) {
        return available;
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
    auto max_seq = storage_->GetMaxSeq(snapshot);
    if (!max_seq.ok()) {
        return max_seq.status();
    }
    response->set_max_seq(max_seq.value());
    response->set_min_seq(0);
    return Status::Ok();
}

Status OpenEventCore::AllocateUuids(const AllocateUuidsRequest& request,
                                    AllocateUuidsResponse* response)
{
    Status available = EnsureAvailable();
    if (!available.ok()) {
        return available;
    }
    if (request.count() == 0 || request.count() > kMaxUuidBatch) {
        return Status(grpc::StatusCode::INVALID_ARGUMENT,
                      "uuid allocation count must be in the range 1..1024");
    }

    std::lock_guard<std::mutex> lock(coordinator_mu_);
    auto snapshot_result = storage_->CreateSnapshot();
    if (!snapshot_result.ok()) {
        return snapshot_result.status();
    }
    ReadSnapshot snapshot = std::move(snapshot_result.value());
    auto next_uuid = storage_->GetNextUuid(snapshot);
    if (!next_uuid.ok()) {
        return next_uuid.status();
    }
    const uint64_t first_uuid = next_uuid.value();
    const uint64_t count = request.count();
    const uint64_t next_value = first_uuid + count;

    rocksdb::WriteBatch batch;
    Status status = storage_->SetNextUuid(&batch, next_value);
    if (!status.ok()) {
        return status;
    }
    status = CommitBatch(&batch);
    if (!status.ok()) {
        return status;
    }
    response->clear_uuids();
    for (uint64_t offset = 0; offset < count; ++offset) {
        response->add_uuids(first_uuid + offset);
    }
    return Status::Ok();
}

Status OpenEventCore::GetSeqByUuid(const GetSeqByUuidRequest& request,
                                   GetSeqByUuidResponse* response)
{
    Status available = EnsureAvailable();
    if (!available.ok()) {
        return available;
    }
    auto snapshot_result = CreateLinearizedSnapshot();
    if (!snapshot_result.ok()) {
        return snapshot_result.status();
    }
    ReadSnapshot snapshot = std::move(snapshot_result.value());
    auto seq = storage_->GetUsedUuidSeq(snapshot, request.uuid());
    if (!seq.ok()) {
        return seq.status();
    }
    if (!seq.value().has_value()) {
        return Status(grpc::StatusCode::NOT_FOUND, "uuid has not been consumed by a committed message");
    }
    response->set_seq(seq.value().value());
    return Status::Ok();
}

Status OpenEventCore::ValidateAvailableUuid(const ReadSnapshot& snapshot, uint64_t uuid) const
{
    if (uuid == 0) {
        return Status(grpc::StatusCode::INVALID_ARGUMENT, "uuid must be allocated by the server");
    }
    auto next_uuid = storage_->GetNextUuid(snapshot);
    if (!next_uuid.ok()) {
        return next_uuid.status();
    }
    if (uuid >= next_uuid.value()) {
        return Status(grpc::StatusCode::INVALID_ARGUMENT, "uuid was not allocated by the server");
    }
    auto uuid_seq = storage_->GetUsedUuidSeq(snapshot, uuid);
    if (!uuid_seq.ok()) {
        return uuid_seq.status();
    }
    if (uuid_seq.value().has_value()) {
        return Status(grpc::StatusCode::ALREADY_EXISTS, "uuid has already been used");
    }
    return Status::Ok();
}

Status OpenEventCore::BuildPublishBatch(const ReadSnapshot& snapshot,
                                        uint64_t principal,
                                        uint64_t channel_id,
                                        uint64_t seq,
                                        uint64_t uuid,
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
    std::vector<uint64_t> object_ids;
    object_ids.reserve(object_keys.size());
    std::unordered_map<uint64_t, size_t> object_indexes;
    object_indexes.reserve(object_keys.size());
    for (const auto& object_key : object_keys) {
        const bool inserted =
            object_indexes.emplace(object_key.object_id(), object_ids.size()).second;
        if (inserted) {
            object_ids.push_back(object_key.object_id());
        }
    }
    auto objects = storage_->GetCommittedObjects(snapshot, object_ids);
    if (!objects.ok()) {
        return objects.status().requires_server_exit() ||
                       objects.status().code() == grpc::StatusCode::DATA_LOSS
                   ? FatalStorageError(objects.status())
                   : objects.status();
    }
    for (const auto& object_key : object_keys) {
        const auto& object = objects.value()[object_indexes.at(object_key.object_id())];
        if (!object.has_value() ||
            !ObjectFileStore::ConstantTimeEquals(object->object_token, object_key.object_token())) {
            return Status(grpc::StatusCode::NOT_FOUND, "object capability not found");
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
    message.set_uuid(uuid);
    for (const auto& object_key : object_keys) {
        *message.add_object_keys() = object_key;
    }

    Status status = storage_->PutMessage(batch, message);
    if (!status.ok()) {
        return status;
    }
    status = storage_->PutUsedUuid(batch, uuid, seq);
    if (!status.ok()) {
        return status;
    }
    return storage_->SetMaxSeq(batch, seq);
}

Status OpenEventCore::CommitBatch(rocksdb::WriteBatch* batch)
{
    Status status = storage_->Commit(batch);
    if (!status.ok() && status.requires_server_exit()) {
        return FatalStorageError(status);
    }
    if (status.ok()) {
        commit_generation_.fetch_add(1, std::memory_order_release);
    }
    return status;
}

Status OpenEventCore::Publish(const PublishRequest& request,
                              PublishResponse*,
                              const grpc::ServerContext* context)
{
    Status available = EnsureAvailable();
    if (!available.ok()) {
        return available;
    }
    if (request.principal() == 0 || Contains(request.recipients(), uint64_t{0})) {
        return Status(grpc::StatusCode::INVALID_ARGUMENT, "principal and recipients must be greater than 0");
    }
    const uint64_t ts_ms = NowMs();

    std::lock_guard<std::mutex> lock(coordinator_mu_);
    Status rpc_status = CheckRpcActive(context);
    if (!rpc_status.ok()) {
        return rpc_status;
    }
    auto snapshot_result = storage_->CreateSnapshot();
    if (!snapshot_result.ok()) {
        return snapshot_result.status();
    }
    ReadSnapshot snapshot = std::move(snapshot_result.value());
    Status auth = Authenticate(snapshot, request.principal(), request.token());
    if (!auth.ok()) {
        return auth;
    }
    Status uuid_status = ValidateAvailableUuid(snapshot, request.uuid());
    if (!uuid_status.ok()) {
        return uuid_status;
    }
    Status payload_status = ValidatePayloadSize(request.payload());
    if (!payload_status.ok()) {
        return payload_status;
    }
    Status object_keys_status = ValidateObjectKeysShape(request.object_keys());
    if (!object_keys_status.ok()) {
        return object_keys_status;
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
                                      request.uuid(),
                                      request.recipients(),
                                      request.payload(),
                                      request.object_keys(),
                                      ts_ms,
                                      &batch);
    if (!status.ok()) {
        return status;
    }
    rpc_status = CheckRpcActive(context);
    if (!rpc_status.ok()) {
        return rpc_status;
    }
    return CommitBatch(&batch);
}

Status OpenEventCore::PublishAutoSeq(const PublishAutoSeqRequest& request,
                                     PublishAutoSeqResponse* response,
                                     const grpc::ServerContext* context)
{
    Status available = EnsureAvailable();
    if (!available.ok()) {
        return available;
    }
    if (request.principal() == 0 || Contains(request.recipients(), uint64_t{0})) {
        return Status(grpc::StatusCode::INVALID_ARGUMENT, "principal and recipients must be greater than 0");
    }
    const uint64_t ts_ms = NowMs();

    std::lock_guard<std::mutex> lock(coordinator_mu_);
    Status rpc_status = CheckRpcActive(context);
    if (!rpc_status.ok()) {
        return rpc_status;
    }
    auto snapshot_result = storage_->CreateSnapshot();
    if (!snapshot_result.ok()) {
        return snapshot_result.status();
    }
    ReadSnapshot snapshot = std::move(snapshot_result.value());
    Status auth = Authenticate(snapshot, request.principal(), request.token());
    if (!auth.ok()) {
        return auth;
    }
    Status uuid_status = ValidateAvailableUuid(snapshot, request.uuid());
    if (!uuid_status.ok()) {
        return uuid_status;
    }
    Status payload_status = ValidatePayloadSize(request.payload());
    if (!payload_status.ok()) {
        return payload_status;
    }
    Status object_keys_status = ValidateObjectKeysShape(request.object_keys());
    if (!object_keys_status.ok()) {
        return object_keys_status;
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
                                      request.uuid(),
                                      request.recipients(),
                                      request.payload(),
                                      request.object_keys(),
                                      ts_ms,
                                      &batch);
    if (!status.ok()) {
        return status;
    }
    rpc_status = CheckRpcActive(context);
    if (!rpc_status.ok()) {
        return rpc_status;
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
        return object.status().requires_server_exit() ||
                       object.status().code() == grpc::StatusCode::DATA_LOSS
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
    {
        std::lock_guard<std::mutex> lock(subscription_snapshot_mu_);
        if (!fatal_error_triggered_.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
            return status;
        }
    }
    subscription_snapshot_cv_.notify_all();
    if (fatal_error_handler_) {
        fatal_error_handler_(status);
    }
    return status;
}

Status OpenEventCore::HandleRpcStatus(const Status& status) const
{
    if (status.requires_server_exit() || status.code() == grpc::StatusCode::DATA_LOSS) {
        FatalStorageError(status);
    }
    return status;
}

Status OpenEventCore::CleanupPreparingObject(uint64_t object_id, const Status& result)
{
    Status file_status = storage_->CleanupObjectFiles(object_id);
    if (!file_status.ok()) {
        return file_status.requires_server_exit() ||
                       file_status.code() == grpc::StatusCode::DATA_LOSS
                   ? FatalStorageError(file_status)
                   : file_status;
    }

    std::lock_guard<std::mutex> lock(coordinator_mu_);
    rocksdb::WriteBatch batch;
    storage_->DeletePreparingObject(&batch, object_id);
    Status cleanup_status = CommitBatch(&batch);
    return cleanup_status.ok() ? result : cleanup_status;
}

Status OpenEventCore::WriteObject(const WriteObjectRequest& request, WriteObjectResponse* response)
{
    Status available = EnsureAvailable();
    if (!available.ok()) {
        return available;
    }
    if (request.principal() == 0) {
        return Status(grpc::StatusCode::INVALID_ARGUMENT, "principal must be greater than 0");
    }
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
    StoredObject object;
    object.object_token = std::move(object_token.value());
    object.creator_principal = request.principal();
    object.name = request.name();
    object.type = request.type();
    object.description = request.description();
    object.nbytes = request.data().size();

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
        if (file_status.requires_server_exit() ||
            file_status.code() == grpc::StatusCode::DATA_LOSS) {
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
                    if (preparing.status().requires_server_exit() ||
                        preparing.status().code() == grpc::StatusCode::DATA_LOSS) {
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
        return FatalStorageError(final_status);
    }
    if (final_status.requires_server_exit() ||
        final_status.code() == grpc::StatusCode::DATA_LOSS) {
        return FatalStorageError(final_status);
    }
    return CleanupPreparingObject(object.object_id, final_status);
}

Status OpenEventCore::GetObjectMetadata(const GetObjectMetadataRequest& request,
                                        GetObjectMetadataResponse* response)
{
    Status available = EnsureAvailable();
    if (!available.ok()) {
        return available;
    }
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
    Status available = EnsureAvailable();
    if (!available.ok()) {
        return available;
    }
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
    auto data = storage_->ReadObjectFile(object, request.offset(), request.nbytes());
    if (!data.ok()) {
        return data.status().requires_server_exit() ||
                       data.status().code() == grpc::StatusCode::DATA_LOSS
                   ? FatalStorageError(data.status())
                   : data.status();
    }
    response->set_data(std::move(data.value()));
    return Status::Ok();
}

Status OpenEventCore::Fetch(const FetchRequest& request, FetchResponse* response)
{
    Status available = EnsureAvailable();
    if (!available.ok()) {
        return available;
    }
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
    Status channel_status =
        ValidateReadableChannels(snapshot, request.principal(), request.channels());
    if (!channel_status.ok()) {
        return channel_status;
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
    if (from_seq > max_seq) {
        response->set_next_seq(max_seq + 1);
        return Status::Ok();
    }

    const std::unordered_set<uint64_t> requested_channels(channels.begin(), channels.end());
    std::unordered_map<uint64_t, bool> readable_channels;
    size_t response_bytes = 0;
    auto next_seq = storage_->ScanMessages(
        snapshot,
        from_seq,
        max_seq,
        max_scan_records_,
        [&](const EventMessage& message) -> Result<MessageScanAction> {
            auto readable = readable_channels.find(message.channel_id());
            if (readable == readable_channels.end()) {
                auto channel_result = LoadChannel(snapshot, message.channel_id());
                if (!channel_result.ok()) {
                    return channel_result.status();
                }
                if (!channel_result.value().has_value()) {
                    return Status(grpc::StatusCode::DATA_LOSS,
                                  "message references a missing channel");
                }
                const bool can_read = CanRead(channel_result.value().value(), principal);
                readable = readable_channels.emplace(message.channel_id(), can_read).first;
            }
            if (!requested_channels.empty() && !requested_channels.contains(message.channel_id())) {
                return MessageScanAction::kContinue;
            }
            if (!readable->second || (only_my_recipient && !HasRecipient(message, principal))) {
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
    Status available = EnsureAvailable();
    if (!available.ok()) {
        return available;
    }
    if (request.principal() == 0 || Contains(request.members(), uint64_t{0}) ||
        request.name().empty() || request.name().size() > 255 ||
        request.protocol().size() > 255 || request.description().size() > 4096 ||
        request.members_size() > 1024) {
        return Status(grpc::StatusCode::INVALID_ARGUMENT, "invalid Channel parameters");
    }
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
    Status available = EnsureAvailable();
    if (!available.ok()) {
        return available;
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
    Status available = EnsureAvailable();
    if (!available.ok()) {
        return available;
    }
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
    Status available = EnsureAvailable();
    if (!available.ok()) {
        return available;
    }
    if (request.principal() == 0 || request.target_principal() == 0) {
        return Status(grpc::StatusCode::INVALID_ARGUMENT, "principals must be greater than 0");
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
    Status available = EnsureAvailable();
    if (!available.ok()) {
        return available;
    }
    if (request.principal() == 0 || request.target_principal() == 0) {
        return Status(grpc::StatusCode::INVALID_ARGUMENT, "principals must be greater than 0");
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
    Status available = EnsureAvailable();
    if (!available.ok()) {
        return available;
    }
    if (request.target_principal() == 0) {
        return Status(grpc::StatusCode::INVALID_ARGUMENT, "target_principal must be greater than 0");
    }
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
    Status available = EnsureAvailable();
    if (!available.ok()) {
        return available;
    }
    if (request.target_token().empty()) {
        return Status(grpc::StatusCode::INVALID_ARGUMENT, "target_token must not be empty");
    }
    std::lock_guard<std::mutex> lock(coordinator_mu_);
    rocksdb::WriteBatch batch;
    storage_->DeleteToken(&batch, request.target_token());
    return CommitBatch(&batch);
}

Status OpenEventCore::ListMessages(const ListMessagesRequest& request, ListMessagesResponse* response)
{
    Status available = EnsureAvailable();
    if (!available.ok()) {
        return available;
    }
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
    const uint64_t start_seq = from_seq;
    if (start_seq > max_seq) {
        response->set_next_seq(max_seq + 1);
        return Status::Ok();
    }

    size_t response_bytes = 0;
    std::unordered_set<uint64_t> known_channels;
    auto next_seq = storage_->ScanMessages(
        snapshot,
        start_seq,
        max_seq,
        max_scan_records_,
        [&](const EventMessage& message) -> Result<MessageScanAction> {
            if (!known_channels.contains(message.channel_id())) {
                auto channel = LoadChannel(snapshot, message.channel_id());
                if (!channel.ok()) {
                    return channel.status();
                }
                if (!channel.value().has_value()) {
                    return Status(grpc::StatusCode::DATA_LOSS,
                                  "message references a missing channel");
                }
                known_channels.insert(message.channel_id());
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

Status OpenEventCore::GetSubscriptionMaxSeq(
    uint64_t principal,
    const std::string& token,
    const google::protobuf::RepeatedField<uint64_t>& channels,
    uint64_t* max_seq) const
{
    Status available = EnsureAvailable();
    if (!available.ok()) {
        return available;
    }
    auto snapshot_result = AcquireSubscriptionSnapshot();
    if (!snapshot_result.ok()) {
        return snapshot_result.status();
    }
    const ReadSnapshot& snapshot = *snapshot_result.value().snapshot;
    Status auth = Authenticate(snapshot, principal, token);
    if (!auth.ok()) {
        return auth;
    }
    Status channel_status = ValidateReadableChannels(snapshot, principal, channels);
    if (!channel_status.ok()) {
        return channel_status;
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
                                             const google::protobuf::RepeatedField<uint64_t>& channels,
                                             FetchResponse* response,
                                             uint64_t* snapshot_version) const
{
    auto snapshot_result = AcquireSubscriptionSnapshot();
    if (!snapshot_result.ok()) {
        return snapshot_result.status();
    }
    const ReadSnapshot& snapshot = *snapshot_result.value().snapshot;
    Status auth = Authenticate(snapshot, principal, token);
    if (!auth.ok()) {
        return auth;
    }
    Status channel_status = ValidateReadableChannels(snapshot, principal, channels);
    if (!channel_status.ok()) {
        return channel_status;
    }
    Status status = FetchVisible(snapshot, principal, from_seq, limit, only_my_recipient, channels, response);
    if (status.ok() && snapshot_version != nullptr) {
        *snapshot_version = snapshot_result.value().version;
    }
    return status;
}

uint64_t OpenEventCore::SubscriptionSnapshotVersion() const
{
    std::lock_guard<std::mutex> lock(subscription_snapshot_mu_);
    return subscription_snapshot_version_;
}

bool OpenEventCore::WaitForSubscriptionSnapshot(uint64_t observed_version,
                                                std::chrono::milliseconds timeout) const
{
    std::unique_lock<std::mutex> lock(subscription_snapshot_mu_);
    subscription_snapshot_cv_.wait_for(lock, timeout, [&]() {
        return stop_subscription_snapshot_thread_ ||
               fatal_error_triggered_.load(std::memory_order_acquire) ||
               subscription_snapshot_version_ != observed_version ||
               (!subscription_snapshot_status_.ok() &&
                subscription_snapshot_attempt_generation_ >
                    subscription_snapshot_generation_);
    });
    return fatal_error_triggered_.load(std::memory_order_acquire) ||
           subscription_snapshot_version_ != observed_version ||
           (!subscription_snapshot_status_.ok() &&
            subscription_snapshot_attempt_generation_ >
                subscription_snapshot_generation_);
}

Result<uint64_t> OpenEventCore::MaxSeq() const
{
    Status available = EnsureAvailable();
    if (!available.ok()) {
        return available;
    }
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

Status OpenEventCore::ValidateReadableChannels(
    const ReadSnapshot& snapshot,
    uint64_t principal,
    const google::protobuf::RepeatedField<uint64_t>& channels) const
{
    std::unordered_set<uint64_t> validated;
    for (uint64_t channel_id : channels) {
        if (!validated.insert(channel_id).second) {
            continue;
        }
        auto channel_result = LoadChannel(snapshot, channel_id);
        if (!channel_result.ok()) {
            return channel_result.status();
        }
        if (!channel_result.value().has_value()) {
            return Status(grpc::StatusCode::NOT_FOUND, "channel not found");
        }
        if (!CanRead(channel_result.value().value(), principal)) {
            return Status(grpc::StatusCode::PERMISSION_DENIED, "channel not visible");
        }
    }
    return Status::Ok();
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
    if (recipients.empty()) {
        return Status::Ok();
    }
    const std::unordered_set<uint64_t> members(channel.members().begin(), channel.members().end());
    for (uint64_t recipient : recipients) {
        if (!members.contains(recipient)) {
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
    channel.set_name("system");
    channel.set_protocol("system.v1");
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

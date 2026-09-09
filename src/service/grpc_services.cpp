#include "service/grpc_services.h"

#include <chrono>

namespace openevent {
namespace {

grpc::Status ToGrpcStatus(const std::shared_ptr<OpenEventCore>& core, const Status& status)
{
    return core->HandleRpcStatus(status).ToGrpc();
}

}  // namespace

EventServiceImpl::EventServiceImpl(std::shared_ptr<OpenEventCore> core) : core_(std::move(core)) {}

grpc::Status EventServiceImpl::GetStatus(grpc::ServerContext*,
                                         const GetStatusRequest* request,
                                         GetStatusResponse* response)
{
    return ToGrpcStatus(core_, core_->GetStatus(*request, response));
}

grpc::Status EventServiceImpl::AllocateUuids(grpc::ServerContext*,
                                             const AllocateUuidsRequest* request,
                                             AllocateUuidsResponse* response)
{
    return ToGrpcStatus(core_, core_->AllocateUuids(*request, response));
}

grpc::Status EventServiceImpl::GetSeqByUuid(grpc::ServerContext*,
                                            const GetSeqByUuidRequest* request,
                                            GetSeqByUuidResponse* response)
{
    return ToGrpcStatus(core_, core_->GetSeqByUuid(*request, response));
}

grpc::Status EventServiceImpl::Publish(grpc::ServerContext* context,
                                       const PublishRequest* request,
                                       PublishResponse* response)
{
    return ToGrpcStatus(core_, core_->Publish(*request, response, context));
}

grpc::Status EventServiceImpl::PublishAutoSeq(grpc::ServerContext* context,
                                              const PublishAutoSeqRequest* request,
                                              PublishAutoSeqResponse* response)
{
    return ToGrpcStatus(core_, core_->PublishAutoSeq(*request, response, context));
}

grpc::Status EventServiceImpl::Fetch(grpc::ServerContext*, const FetchRequest* request, FetchResponse* response)
{
    return ToGrpcStatus(core_, core_->Fetch(*request, response));
}

bool EventServiceImpl::RegisterSubscription(grpc::ServerContext* context)
{
    std::lock_guard<std::mutex> lock(subscriptions_mu_);
    if (subscriptions_stopping_) {
        return false;
    }
    subscriptions_.insert(context);
    return true;
}

void EventServiceImpl::UnregisterSubscription(grpc::ServerContext* context)
{
    std::lock_guard<std::mutex> lock(subscriptions_mu_);
    subscriptions_.erase(context);
}

void EventServiceImpl::StopSubscriptions()
{
    std::lock_guard<std::mutex> lock(subscriptions_mu_);
    subscriptions_stopping_ = true;
    for (grpc::ServerContext* context : subscriptions_) {
        context->TryCancel();
    }
}

grpc::Status EventServiceImpl::Subscribe(grpc::ServerContext* context,
                                         const SubscribeRequest* request,
                                         grpc::ServerWriter<SubscribeResponse>* writer)
{
    if (!RegisterSubscription(context)) {
        return grpc::Status(grpc::StatusCode::UNAVAILABLE, "server is shutting down");
    }

    grpc::Status status = RunSubscription(context, request, writer);
    UnregisterSubscription(context);
    return status;
}

grpc::Status EventServiceImpl::RunSubscription(grpc::ServerContext* context,
                                               const SubscribeRequest* request,
                                               grpc::ServerWriter<SubscribeResponse>* writer)
{
    uint64_t max_seq = 0;
    Status start_status = core_->GetSubscriptionMaxSeq(
        request->principal(), request->token(), request->channels(), &max_seq);
    if (!start_status.ok()) {
        return ToGrpcStatus(core_, start_status);
    }

    uint64_t next_seq = request->from_seq();
    if (next_seq > max_seq) {
        return grpc::Status(grpc::StatusCode::OUT_OF_RANGE, "from_seq exceeds max_seq");
    }

    // Send acceptance only after authentication, Channel and start validation.
    // Keep it nonempty so SDKs can distinguish acceptance from trailers-only rejection.
    context->AddInitialMetadata("openevent-subscription", "accepted");
    writer->SendInitialMetadata();

    while (!context->IsCancelled()) {
        FetchResponse batch;
        uint64_t snapshot_version = 0;
        Status status = core_->FetchSubscriptionBatch(request->principal(),
                                                      request->token(),
                                                      next_seq,
                                                      100,
                                                      request->only_my_recipient(),
                                                      request->channels(),
                                                      &batch,
                                                      &snapshot_version);
        if (!status.ok()) {
            return ToGrpcStatus(core_, status);
        }

        for (const auto& message : batch.messages()) {
            SubscribeResponse response;
            *response.mutable_message() = message;
            if (!writer->Write(response)) {
                return grpc::Status::OK;
            }
        }

        next_seq = batch.next_seq();
        if (batch.next_seq() > batch.last_seq()) {
            while (!context->IsCancelled() &&
                   !core_->WaitForSubscriptionSnapshot(snapshot_version,
                                                       std::chrono::milliseconds(100))) {
            }
        }
    }

    return grpc::Status::OK;
}

ObjectStorageServiceImpl::ObjectStorageServiceImpl(std::shared_ptr<OpenEventCore> core)
    : core_(std::move(core))
{
}

grpc::Status ObjectStorageServiceImpl::WriteObject(grpc::ServerContext*,
                                                   const WriteObjectRequest* request,
                                                   WriteObjectResponse* response)
{
    return ToGrpcStatus(core_, core_->WriteObject(*request, response));
}

grpc::Status ObjectStorageServiceImpl::GetObjectMetadata(
    grpc::ServerContext*,
    const GetObjectMetadataRequest* request,
    GetObjectMetadataResponse* response)
{
    return ToGrpcStatus(core_, core_->GetObjectMetadata(*request, response));
}

grpc::Status ObjectStorageServiceImpl::ReadObject(grpc::ServerContext*,
                                                  const ReadObjectRequest* request,
                                                  ReadObjectResponse* response)
{
    return ToGrpcStatus(core_, core_->ReadObject(*request, response));
}

ChannelServiceImpl::ChannelServiceImpl(std::shared_ptr<OpenEventCore> core) : core_(std::move(core)) {}

grpc::Status ChannelServiceImpl::CreateChannel(grpc::ServerContext*,
                                               const CreateChannelRequest* request,
                                               CreateChannelResponse* response)
{
    return ToGrpcStatus(core_, core_->CreateChannel(*request, response));
}

grpc::Status ChannelServiceImpl::GetChannel(grpc::ServerContext*,
                                            const GetChannelRequest* request,
                                            GetChannelResponse* response)
{
    return ToGrpcStatus(core_, core_->GetChannel(*request, response));
}

grpc::Status ChannelServiceImpl::ListChannels(grpc::ServerContext*,
                                              const ListChannelsRequest* request,
                                              ListChannelsResponse* response)
{
    return ToGrpcStatus(core_, core_->ListChannels(*request, response));
}

grpc::Status ChannelServiceImpl::AddMember(grpc::ServerContext*,
                                           const AddMemberRequest* request,
                                           AddMemberResponse* response)
{
    return ToGrpcStatus(core_, core_->AddMember(*request, response));
}

grpc::Status ChannelServiceImpl::RemoveMember(grpc::ServerContext*,
                                              const RemoveMemberRequest* request,
                                              RemoveMemberResponse* response)
{
    return ToGrpcStatus(core_, core_->RemoveMember(*request, response));
}

AdminServiceImpl::AdminServiceImpl(std::shared_ptr<OpenEventCore> core) : core_(std::move(core)) {}

grpc::Status AdminServiceImpl::AddToken(grpc::ServerContext*, const AddTokenRequest* request, AddTokenResponse* response)
{
    return ToGrpcStatus(core_, core_->AddToken(*request, response));
}

grpc::Status AdminServiceImpl::DeleteToken(grpc::ServerContext*,
                                           const DeleteTokenRequest* request,
                                           DeleteTokenResponse* response)
{
    return ToGrpcStatus(core_, core_->DeleteToken(*request, response));
}

grpc::Status AdminServiceImpl::ListMessages(grpc::ServerContext*,
                                            const ListMessagesRequest* request,
                                            ListMessagesResponse* response)
{
    return ToGrpcStatus(core_, core_->ListMessages(*request, response));
}

}  // namespace openevent

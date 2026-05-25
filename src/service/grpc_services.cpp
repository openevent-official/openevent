#include "service/grpc_services.h"

#include <chrono>
#include <thread>

namespace openevent {
namespace {

grpc::Status ToGrpcStatus(const Status& status)
{
    return status.ToGrpc();
}

}  // namespace

EventServiceImpl::EventServiceImpl(std::shared_ptr<OpenEventCore> core) : core_(std::move(core)) {}

grpc::Status EventServiceImpl::GetStatus(grpc::ServerContext*,
                                         const GetStatusRequest* request,
                                         GetStatusResponse* response)
{
    return ToGrpcStatus(core_->GetStatus(*request, response));
}

grpc::Status EventServiceImpl::Publish(grpc::ServerContext*,
                                       const PublishRequest* request,
                                       PublishResponse* response)
{
    return ToGrpcStatus(core_->Publish(*request, response));
}

grpc::Status EventServiceImpl::PublishAutoSeq(grpc::ServerContext*,
                                              const PublishAutoSeqRequest* request,
                                              PublishAutoSeqResponse* response)
{
    return ToGrpcStatus(core_->PublishAutoSeq(*request, response));
}

grpc::Status EventServiceImpl::Fetch(grpc::ServerContext*, const FetchRequest* request, FetchResponse* response)
{
    return ToGrpcStatus(core_->Fetch(*request, response));
}

grpc::Status EventServiceImpl::Subscribe(grpc::ServerContext* context,
                                         const SubscribeRequest* request,
                                         grpc::ServerWriter<SubscribeResponse>* writer)
{
    Status auth = core_->Authenticate(request->principal(), request->token());
    if (!auth.ok()) {
        return ToGrpcStatus(auth);
    }

    auto max_seq = core_->MaxSeq();
    if (!max_seq.ok()) {
        return ToGrpcStatus(max_seq.status());
    }

    uint64_t next_seq = request->from_seq();
    if (next_seq == 0) {
        next_seq = max_seq.value() + 1;
    } else if (next_seq > max_seq.value()) {
        SubscribeResponse response;
        response.set_next_seq(max_seq.value() + 1);
        writer->Write(response);
        return grpc::Status::OK;
    }

    while (!context->IsCancelled()) {
        FetchResponse batch;
        Status status = core_->FetchVisible(request->principal(), next_seq, 100, request->only_my_recipient(), &batch);
        if (!status.ok()) {
            return ToGrpcStatus(status);
        }

        for (const auto& message : batch.messages()) {
            SubscribeResponse response;
            *response.mutable_message() = message;
            if (!writer->Write(response)) {
                return grpc::Status::OK;
            }
        }

        next_seq = batch.next_seq();
        if (!batch.has_more()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    }

    return grpc::Status::OK;
}

ChannelServiceImpl::ChannelServiceImpl(std::shared_ptr<OpenEventCore> core) : core_(std::move(core)) {}

grpc::Status ChannelServiceImpl::CreateChannel(grpc::ServerContext*,
                                               const CreateChannelRequest* request,
                                               CreateChannelResponse* response)
{
    return ToGrpcStatus(core_->CreateChannel(*request, response));
}

grpc::Status ChannelServiceImpl::GetChannel(grpc::ServerContext*,
                                            const GetChannelRequest* request,
                                            GetChannelResponse* response)
{
    return ToGrpcStatus(core_->GetChannel(*request, response));
}

grpc::Status ChannelServiceImpl::ListChannels(grpc::ServerContext*,
                                              const ListChannelsRequest* request,
                                              ListChannelsResponse* response)
{
    return ToGrpcStatus(core_->ListChannels(*request, response));
}

grpc::Status ChannelServiceImpl::AddMember(grpc::ServerContext*,
                                           const AddMemberRequest* request,
                                           AddMemberResponse* response)
{
    return ToGrpcStatus(core_->AddMember(*request, response));
}

grpc::Status ChannelServiceImpl::RemoveMember(grpc::ServerContext*,
                                              const RemoveMemberRequest* request,
                                              RemoveMemberResponse* response)
{
    return ToGrpcStatus(core_->RemoveMember(*request, response));
}

AdminServiceImpl::AdminServiceImpl(std::shared_ptr<OpenEventCore> core) : core_(std::move(core)) {}

grpc::Status AdminServiceImpl::AddToken(grpc::ServerContext*, const AddTokenRequest* request, AddTokenResponse* response)
{
    return ToGrpcStatus(core_->AddToken(*request, response));
}

grpc::Status AdminServiceImpl::DeleteToken(grpc::ServerContext*,
                                           const DeleteTokenRequest* request,
                                           DeleteTokenResponse* response)
{
    return ToGrpcStatus(core_->DeleteToken(*request, response));
}

grpc::Status AdminServiceImpl::ListTokens(grpc::ServerContext*,
                                          const ListTokensRequest* request,
                                          ListTokensResponse* response)
{
    return ToGrpcStatus(core_->ListTokens(*request, response));
}

}  // namespace openevent

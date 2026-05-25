#pragma once

#include <memory>

#include <grpcpp/grpcpp.h>

#include "openevent.grpc.pb.h"
#include "service/open_event_core.h"

namespace openevent {

class EventServiceImpl final : public ::openevent::EventService::Service {
public:
    explicit EventServiceImpl(std::shared_ptr<OpenEventCore> core);

    grpc::Status GetStatus(grpc::ServerContext* context,
                           const GetStatusRequest* request,
                           GetStatusResponse* response) override;
    grpc::Status Publish(grpc::ServerContext* context,
                         const PublishRequest* request,
                         PublishResponse* response) override;
    grpc::Status PublishAutoSeq(grpc::ServerContext* context,
                                const PublishAutoSeqRequest* request,
                                PublishAutoSeqResponse* response) override;
    grpc::Status Fetch(grpc::ServerContext* context,
                       const FetchRequest* request,
                       FetchResponse* response) override;
    grpc::Status Subscribe(grpc::ServerContext* context,
                           const SubscribeRequest* request,
                           grpc::ServerWriter<SubscribeResponse>* writer) override;

private:
    std::shared_ptr<OpenEventCore> core_;
};

class ChannelServiceImpl final : public ::openevent::ChannelService::Service {
public:
    explicit ChannelServiceImpl(std::shared_ptr<OpenEventCore> core);

    grpc::Status CreateChannel(grpc::ServerContext* context,
                               const CreateChannelRequest* request,
                               CreateChannelResponse* response) override;
    grpc::Status GetChannel(grpc::ServerContext* context,
                            const GetChannelRequest* request,
                            GetChannelResponse* response) override;
    grpc::Status ListChannels(grpc::ServerContext* context,
                              const ListChannelsRequest* request,
                              ListChannelsResponse* response) override;
    grpc::Status AddMember(grpc::ServerContext* context,
                           const AddMemberRequest* request,
                           AddMemberResponse* response) override;
    grpc::Status RemoveMember(grpc::ServerContext* context,
                              const RemoveMemberRequest* request,
                              RemoveMemberResponse* response) override;

private:
    std::shared_ptr<OpenEventCore> core_;
};

class AdminServiceImpl final : public ::openevent::AdminService::Service {
public:
    explicit AdminServiceImpl(std::shared_ptr<OpenEventCore> core);

    grpc::Status AddToken(grpc::ServerContext* context,
                          const AddTokenRequest* request,
                          AddTokenResponse* response) override;
    grpc::Status DeleteToken(grpc::ServerContext* context,
                             const DeleteTokenRequest* request,
                             DeleteTokenResponse* response) override;
    grpc::Status ListTokens(grpc::ServerContext* context,
                            const ListTokensRequest* request,
                            ListTokensResponse* response) override;

private:
    std::shared_ptr<OpenEventCore> core_;
};

}  // namespace openevent

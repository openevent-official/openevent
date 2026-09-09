#pragma once

#include <memory>
#include <mutex>
#include <unordered_set>

#include <grpcpp/grpcpp.h>

#include "admin.grpc.pb.h"
#include "openevent.grpc.pb.h"
#include "service/open_event_core.h"

namespace openevent {

class EventServiceImpl final : public ::openevent::EventService::Service {
public:
    explicit EventServiceImpl(std::shared_ptr<OpenEventCore> core);

    grpc::Status GetStatus(grpc::ServerContext* context,
                           const GetStatusRequest* request,
                           GetStatusResponse* response) override;
    grpc::Status AllocateUuids(grpc::ServerContext* context,
                               const AllocateUuidsRequest* request,
                               AllocateUuidsResponse* response) override;
    grpc::Status GetSeqByUuid(grpc::ServerContext* context,
                              const GetSeqByUuidRequest* request,
                              GetSeqByUuidResponse* response) override;
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

    // Ends all active subscription streams as part of server shutdown.
    void StopSubscriptions();

private:
    bool RegisterSubscription(grpc::ServerContext* context);
    void UnregisterSubscription(grpc::ServerContext* context);
    grpc::Status RunSubscription(grpc::ServerContext* context,
                                 const SubscribeRequest* request,
                                 grpc::ServerWriter<SubscribeResponse>* writer);

    std::shared_ptr<OpenEventCore> core_;
    std::mutex subscriptions_mu_;
    std::unordered_set<grpc::ServerContext*> subscriptions_;
    bool subscriptions_stopping_ = false;
};

class ObjectStorageServiceImpl final : public ::openevent::ObjectStorageService::Service {
public:
    explicit ObjectStorageServiceImpl(std::shared_ptr<OpenEventCore> core);

    grpc::Status WriteObject(grpc::ServerContext* context,
                             const WriteObjectRequest* request,
                             WriteObjectResponse* response) override;
    grpc::Status GetObjectMetadata(grpc::ServerContext* context,
                                   const GetObjectMetadataRequest* request,
                                   GetObjectMetadataResponse* response) override;
    grpc::Status ReadObject(grpc::ServerContext* context,
                            const ReadObjectRequest* request,
                            ReadObjectResponse* response) override;

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
    grpc::Status ListMessages(grpc::ServerContext* context,
                              const ListMessagesRequest* request,
                              ListMessagesResponse* response) override;

private:
    std::shared_ptr<OpenEventCore> core_;
};

}  // namespace openevent

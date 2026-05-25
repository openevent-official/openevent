#include <iostream>
#include <memory>
#include <string>
#include <thread>

#include <grpcpp/grpcpp.h>

#include "server/server_config.h"
#include "service/grpc_services.h"
#include "service/open_event_core.h"
#include "storage/metadata_store.h"
#include "storage/rocksdb_message_store.h"

namespace {

void PrintUsage(const char* program)
{
    std::cerr << "usage: " << program << " <config.yaml>\n";
}

}  // namespace

int main(int argc, char** argv)
{
    if (argc != 2) {
        PrintUsage(argv[0]);
        return 1;
    }

    auto config_result = openevent::LoadServerConfig(argv[1]);
    if (!config_result.ok()) {
        std::cerr << config_result.status().message() << "\n";
        return 1;
    }
    const openevent::ServerConfig config = config_result.value();

    auto metadata_result = openevent::MetadataStore::Open(config.metadata_path);
    if (!metadata_result.ok()) {
        std::cerr << metadata_result.status().message() << "\n";
        return 1;
    }

    auto store_result = openevent::RocksDBMessageStore::Open(config.message_store_path);
    if (!store_result.ok()) {
        std::cerr << store_result.status().message() << "\n";
        return 1;
    }

    auto core = std::make_shared<openevent::OpenEventCore>(
        std::move(metadata_result.value()), std::move(store_result.value()), config.max_payload_bytes);
    openevent::Status recover_status = core->RecoverPending();
    if (!recover_status.ok()) {
        std::cerr << "recover pending failed: " << recover_status.message() << "\n";
        return 1;
    }

    openevent::EventServiceImpl event_service(core);
    openevent::ChannelServiceImpl channel_service(core);
    openevent::AdminServiceImpl admin_service(core);

    grpc::ServerBuilder public_builder;
    public_builder.AddListeningPort(config.grpc_listen_addr, grpc::InsecureServerCredentials());
    public_builder.RegisterService(&event_service);
    public_builder.RegisterService(&channel_service);
    std::unique_ptr<grpc::Server> public_server(public_builder.BuildAndStart());
    if (!public_server) {
        std::cerr << "failed to start public server at " << config.grpc_listen_addr << "\n";
        return 1;
    }

    grpc::ServerBuilder admin_builder;
    admin_builder.AddListeningPort(config.admin_listen_addr, grpc::InsecureServerCredentials());
    admin_builder.RegisterService(&admin_service);
    std::unique_ptr<grpc::Server> admin_server(admin_builder.BuildAndStart());
    if (!admin_server) {
        std::cerr << "failed to start admin server at " << config.admin_listen_addr << "\n";
        return 1;
    }

    std::cout << "OpenEvent public gRPC listening on " << config.grpc_listen_addr << "\n";
    std::cout << "OpenEvent admin gRPC listening on " << config.admin_listen_addr << "\n";

    std::thread admin_thread([&admin_server]() { admin_server->Wait(); });
    public_server->Wait();
    admin_server->Shutdown();
    admin_thread.join();
    return 0;
}

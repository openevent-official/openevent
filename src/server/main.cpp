#include <atomic>
#include <csignal>
#include <iostream>
#include <memory>
#include <pthread.h>
#include <thread>
#include <utility>

#include <grpcpp/grpcpp.h>

#include "server/server_config.h"
#include "common/message_limits.h"
#include "service/grpc_services.h"
#include "service/open_event_core.h"
#include "storage/unified_storage.h"

namespace {

constexpr int kSubscribeKeepaliveTimeMs = 30 * 1000;
constexpr int kSubscribeKeepaliveTimeoutMs = 10 * 1000;
constexpr int kMinimumClientPingIntervalMs = 20 * 1000;

void ConfigurePublicServerKeepalive(grpc::ServerBuilder* builder)
{
    builder->AddChannelArgument("grpc.keepalive_time_ms", kSubscribeKeepaliveTimeMs);
    builder->AddChannelArgument("grpc.keepalive_timeout_ms", kSubscribeKeepaliveTimeoutMs);
    builder->AddChannelArgument("grpc.http2.ping_timeout_ms", kSubscribeKeepaliveTimeoutMs);
    builder->AddChannelArgument("grpc.keepalive_permit_without_calls", 1);
    builder->AddChannelArgument("grpc.http2.min_ping_interval_without_data_ms",
                                kMinimumClientPingIntervalMs);
    builder->AddChannelArgument("grpc.http2.max_pings_without_data", 0);
    builder->AddChannelArgument("grpc.http2.max_ping_strikes", 2);
}

int RunServer(const openevent::ServerConfig& config, std::atomic<bool>& fatal_storage_error)
{
    sigset_t shutdown_signals;
    sigemptyset(&shutdown_signals);
    sigaddset(&shutdown_signals, SIGINT);
    sigaddset(&shutdown_signals, SIGTERM);
    if (pthread_sigmask(SIG_BLOCK, &shutdown_signals, nullptr) != 0) {
        std::cerr << "failed to block shutdown signals\n";
        return 1;
    }

    std::cout << "OpenEvent data directory: " << config.storage_path << std::endl;
    auto storage_result = openevent::UnifiedStorage::Open(config.storage_path);
    if (!storage_result.ok()) {
        std::cerr << storage_result.status().message() << "\n";
        return 1;
    }

    const pthread_t main_thread = pthread_self();
    const auto on_fatal_error = [main_thread, &fatal_storage_error](const openevent::Status& status) {
        fatal_storage_error.store(true);
        std::cerr << status.FatalLogMessage() << "\n";
        // This stays pending for the main thread even if it has not reached sigwait yet.
        pthread_kill(main_thread, SIGTERM);
    };
    auto core = std::make_shared<openevent::OpenEventCore>(
        std::move(storage_result.value()), config.max_payload_bytes, 10000, 0, on_fatal_error);
    if (fatal_storage_error.load()) {
        return 1;
    }

    openevent::EventServiceImpl event_service(core);
    openevent::ObjectStorageServiceImpl object_storage_service(core);
    openevent::ChannelServiceImpl channel_service(core);
    openevent::AdminServiceImpl admin_service(core);

    const int grpc_message_limit = openevent::kGrpcMessageBytes;

    grpc::ServerBuilder admin_builder;
    admin_builder.SetMaxReceiveMessageSize(grpc_message_limit);
    admin_builder.SetMaxSendMessageSize(grpc_message_limit);
    admin_builder.AddListeningPort(config.admin_listen_addr, grpc::InsecureServerCredentials());
    admin_builder.RegisterService(&admin_service);
    std::unique_ptr<grpc::Server> admin_server(admin_builder.BuildAndStart());
    if (!admin_server) {
        std::cerr << "failed to start admin server at " << config.admin_listen_addr << "\n";
        return 1;
    }

    grpc::ServerBuilder public_builder;
    public_builder.SetMaxReceiveMessageSize(grpc_message_limit);
    public_builder.SetMaxSendMessageSize(grpc_message_limit);
    ConfigurePublicServerKeepalive(&public_builder);
    public_builder.AddListeningPort(config.grpc_listen_addr, grpc::InsecureServerCredentials());
    public_builder.RegisterService(&event_service);
    public_builder.RegisterService(&object_storage_service);
    public_builder.RegisterService(&channel_service);
    std::unique_ptr<grpc::Server> public_server(public_builder.BuildAndStart());
    if (!public_server) {
        std::cerr << "failed to start public server at " << config.grpc_listen_addr << "\n";
        admin_server->Shutdown();
        return 1;
    }

    std::cout << "OpenEvent public gRPC listening on " << config.grpc_listen_addr << '\n'
              << "OpenEvent admin gRPC listening on " << config.admin_listen_addr << std::endl;

    int signal_number = 0;
    const int wait_error = sigwait(&shutdown_signals, &signal_number);
    if (wait_error != 0) {
        std::cerr << "failed to wait for shutdown signal: " << wait_error << "\n";
    } else if (fatal_storage_error.load()) {
        std::cerr << "stopping servers after fatal storage error and waiting for in-flight requests\n";
    } else {
        std::cerr << "received shutdown signal " << signal_number
                  << ", waiting for in-flight requests\n";
    }

    event_service.StopSubscriptions();
    std::thread public_shutdown([&public_server]() { public_server->Shutdown(); });
    admin_server->Shutdown();
    public_shutdown.join();
    return wait_error == 0 ? 0 : 1;
}

}  // namespace

int main(int argc, char** argv)
{
    if (argc != 2) {
        std::cerr << "usage: " << argv[0] << " <config.yaml>\n";
        return 1;
    }
    auto config = openevent::LoadServerConfig(argv[1]);
    if (!config.ok()) {
        std::cerr << config.status().message() << "\n";
        return 1;
    }
    std::atomic<bool> fatal_storage_error{false};
    const int result = RunServer(config.value(), fatal_storage_error);
    return fatal_storage_error.load() ? 1 : result;
}

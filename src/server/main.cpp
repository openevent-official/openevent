#include <algorithm>
#include <atomic>
#include <chrono>
#include <csignal>
#include <iostream>
#include <limits>
#include <memory>
#include <pthread.h>
#include <string>
#include <thread>
#include <unistd.h>

#include <grpcpp/grpcpp.h>

#include "server/server_config.h"
#include "service/grpc_services.h"
#include "service/open_event_core.h"
#include "storage/unified_storage.h"

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

    sigset_t shutdown_signals;
    sigemptyset(&shutdown_signals);
    sigaddset(&shutdown_signals, SIGINT);
    sigaddset(&shutdown_signals, SIGTERM);
    if (pthread_sigmask(SIG_BLOCK, &shutdown_signals, nullptr) != 0) {
        std::cerr << "failed to block shutdown signals\n";
        return 1;
    }

    auto storage_result = openevent::UnifiedStorage::Open(config.storage_path);
    if (!storage_result.ok()) {
        std::cerr << storage_result.status().message() << "\n";
        return 1;
    }

    std::atomic<bool> fatal_storage_error{false};
    auto core = std::make_shared<openevent::OpenEventCore>(std::move(storage_result.value()),
                                                           config.max_payload_bytes,
                                                           10000,
                                                           0,
                                                           [&fatal_storage_error](const openevent::Status& status) {
                                                               fatal_storage_error.store(
                                                                   true, std::memory_order_release);
                                                               std::cerr << "fatal storage error: "
                                                                         << status.message() << "\n";
                                                               ::kill(::getpid(), SIGTERM);
                                                           });

    openevent::EventServiceImpl event_service(core);
    openevent::ObjectStorageServiceImpl object_storage_service(core);
    openevent::ChannelServiceImpl channel_service(core);
    openevent::AdminServiceImpl admin_service(core);

    constexpr size_t kMaxObjectBytes = 4 * 1024 * 1024;
    constexpr size_t kGrpcEnvelopeBytes = 2 * 1024 * 1024;
    const size_t max_grpc_size = static_cast<size_t>(std::numeric_limits<int>::max());
    const size_t largest_body = std::max(config.max_payload_bytes, kMaxObjectBytes);
    const size_t configured_grpc_size = largest_body > max_grpc_size - kGrpcEnvelopeBytes
                                            ? max_grpc_size
                                            : largest_body + kGrpcEnvelopeBytes;
    const int grpc_message_limit = static_cast<int>(configured_grpc_size);

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
    public_builder.AddListeningPort(config.grpc_listen_addr, grpc::InsecureServerCredentials());
    public_builder.RegisterService(&event_service);
    public_builder.RegisterService(&object_storage_service);
    public_builder.RegisterService(&channel_service);
    std::unique_ptr<grpc::Server> public_server(public_builder.BuildAndStart());
    if (!public_server) {
        std::cerr << "failed to start public server at " << config.grpc_listen_addr << "\n";
        const auto deadline = std::chrono::system_clock::now() +
                              std::chrono::seconds(config.shutdown_grace_seconds);
        admin_server->Shutdown(deadline);
        return 1;
    }

    std::cout << "OpenEvent public gRPC listening on " << config.grpc_listen_addr << "\n";
    std::cout << "OpenEvent admin gRPC listening on " << config.admin_listen_addr << "\n";

    std::atomic<bool> shutdown_initiated{false};
    std::thread admin_thread([&admin_server]() { admin_server->Wait(); });
    std::thread shutdown_thread([&]() {
        int signal_number = 0;
        if (sigwait(&shutdown_signals, &signal_number) != 0) {
            return;
        }

        const auto deadline = std::chrono::system_clock::now() +
                              std::chrono::seconds(config.shutdown_grace_seconds);
        shutdown_initiated.store(true, std::memory_order_release);
        std::cerr << "received shutdown signal " << signal_number << ", draining requests\n";

        std::thread public_shutdown([&]() { public_server->Shutdown(deadline); });
        admin_server->Shutdown(deadline);
        public_shutdown.join();
    });

    public_server->Wait();
    if (!shutdown_initiated.load(std::memory_order_acquire)) {
        admin_server->Shutdown();
    }
    admin_thread.join();
    shutdown_thread.join();
    return fatal_storage_error.load(std::memory_order_acquire) ? 1 : 0;
}

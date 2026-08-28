#include <algorithm>
#include <atomic>
#include <csignal>
#include <condition_variable>
#include <iostream>
#include <limits>
#include <memory>
#include <mutex>
#include <pthread.h>
#include <string>
#include <thread>
#include <unistd.h>

#include <grpcpp/grpcpp.h>

#include "server/server_config.h"
#include "common/object_limits.h"
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
    builder->AddChannelArgument("grpc.http2.min_ping_interval_without_data_ms",
                                kMinimumClientPingIntervalMs);
    builder->AddChannelArgument("grpc.http2.max_pings_without_data", 0);
    builder->AddChannelArgument("grpc.http2.max_ping_strikes", 2);
}

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
    std::condition_variable fatal_storage_error_cv;
    std::mutex fatal_storage_error_mu;
    auto core = std::make_shared<openevent::OpenEventCore>(std::move(storage_result.value()),
                                                           config.max_payload_bytes,
                                                           10000,
                                                           0,
                                                           [&](const openevent::Status& status) {
                                                               {
                                                                   std::lock_guard<std::mutex> lock(
                                                                       fatal_storage_error_mu);
                                                                   fatal_storage_error.store(
                                                                       true, std::memory_order_release);
                                                               }
                                                               std::cerr << "fatal storage error: "
                                                                         << status.message() << "\n";
                                                               fatal_storage_error_cv.notify_all();
                                                           });
    if (fatal_storage_error.load(std::memory_order_acquire)) {
        return 1;
    }

    openevent::EventServiceImpl event_service(core);
    openevent::ObjectStorageServiceImpl object_storage_service(core);
    openevent::ChannelServiceImpl channel_service(core);
    openevent::AdminServiceImpl admin_service(core);

    constexpr size_t kGrpcEnvelopeBytes = 2 * 1024 * 1024;
    const size_t max_grpc_size = static_cast<size_t>(std::numeric_limits<int>::max());
    const size_t largest_body = std::max(config.max_payload_bytes, openevent::kMaxObjectBytes);
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

    std::cout << "OpenEvent public gRPC listening on " << config.grpc_listen_addr << "\n";
    std::cout << "OpenEvent admin gRPC listening on " << config.admin_listen_addr << "\n";

    std::atomic<bool> shutdown_initiated{false};
    std::atomic<bool> shutdown_signal_received{false};
    std::atomic<bool> stop_shutdown_threads{false};
    const auto shutdown_servers = [&]() {
        event_service.StopSubscriptions();
        std::thread public_shutdown([&]() { public_server->Shutdown(); });
        admin_server->Shutdown();
        public_shutdown.join();
    };
    std::thread admin_thread([&admin_server]() { admin_server->Wait(); });
    std::thread shutdown_thread([&]() {
        int signal_number = 0;
        if (sigwait(&shutdown_signals, &signal_number) != 0) {
            return;
        }

        shutdown_signal_received.store(true, std::memory_order_release);
        const bool fatal = fatal_storage_error.load(std::memory_order_acquire);
        const bool first_shutdown =
            !shutdown_initiated.exchange(true, std::memory_order_acq_rel);
        if (fatal) {
            if (first_shutdown) {
                std::cerr << "stopping servers after fatal storage error and waiting for in-flight requests\n";
            }
        } else if (first_shutdown) {
            std::cerr << "received shutdown signal " << signal_number
                      << ", waiting for in-flight requests\n";
        }
        if (first_shutdown) {
            shutdown_servers();
        }
    });
    std::thread fatal_shutdown_thread([&]() {
        std::unique_lock<std::mutex> lock(fatal_storage_error_mu);
        fatal_storage_error_cv.wait(lock, [&]() {
            return fatal_storage_error.load(std::memory_order_acquire) ||
                   stop_shutdown_threads.load(std::memory_order_acquire);
        });
        if (!fatal_storage_error.load(std::memory_order_acquire)) {
            return;
        }
        lock.unlock();

        if (shutdown_initiated.exchange(true, std::memory_order_acq_rel)) {
            return;
        }
        std::cerr << "stopping servers after fatal storage error and waiting for in-flight requests\n";
        shutdown_servers();
    });

    public_server->Wait();
    if (!shutdown_initiated.load(std::memory_order_acquire)) {
        event_service.StopSubscriptions();
        admin_server->Shutdown();
    }
    {
        std::lock_guard<std::mutex> lock(fatal_storage_error_mu);
        stop_shutdown_threads.store(true, std::memory_order_release);
    }
    fatal_storage_error_cv.notify_all();
    if (!shutdown_signal_received.load(std::memory_order_acquire)) {
        ::kill(::getpid(), SIGTERM);
    }
    admin_thread.join();
    shutdown_thread.join();
    fatal_shutdown_thread.join();
    return fatal_storage_error.load(std::memory_order_acquire) ? 1 : 0;
}

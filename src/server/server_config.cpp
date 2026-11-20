#include "server/server_config.h"

#include <arpa/inet.h>
#include <netdb.h>

#include <charconv>
#include <filesystem>
#include <string_view>

#include <yaml-cpp/yaml.h>

namespace openevent {
namespace {

template <typename T>
void AssignIfPresent(const YAML::Node& node, const char* key, T* out)
{
    if (node && node[key]) {
        *out = node[key].as<T>();
    }
}

bool HasFixedListenPort(std::string_view address)
{
    if (address.find('\0') != std::string_view::npos) {
        return false;
    }
    if (address.starts_with("dns:")) {
        if (!address.starts_with("dns:///")) {
            return false;
        }
        address.remove_prefix(7);
    }
    if (address.starts_with("unix:") || address.starts_with("unix-abstract:")) {
        return address.find(':') + 1 < address.size();
    }

    if (address.starts_with("external:")) {
        return false;
    }
    const size_t colon = address.rfind(':');
    if (colon == std::string_view::npos || colon == 0 || colon + 1 == address.size()) {
        return false;
    }
    const auto host = address.substr(0, colon);
    if (host.starts_with('[') && host.ends_with(']')) {
        in6_addr ipv6{};
        const std::string literal(host.substr(1, host.size() - 2));
        if (inet_pton(AF_INET6, literal.c_str(), &ipv6) != 1) {
            return false;
        }
    } else if (host.find_first_not_of("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-._")
               != std::string_view::npos) {
        return false;
    }

    const std::string service(address.substr(colon + 1));
    if (service.find_first_not_of("0123456789") == std::string::npos) {
        uint16_t port = 0;
        const auto parsed = std::from_chars(service.data(), service.data() + service.size(), port);
        return parsed.ec == std::errc{} && port != 0;
    }
    if (service.starts_with('-') ||
        service.find_first_not_of("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-")
        != std::string::npos) {
        return false;
    }
    const auto* named = getservbyname(service.c_str(), "tcp");
    // gRPC also knows these two names when the system service database lacks them.
    return named ? named->s_port != 0 : service == "http" || service == "https";
}

}  // namespace

Status ValidateServerConfig(const ServerConfig& config)
{
    if (config.grpc_listen_addr.empty()) {
        return Status(grpc::StatusCode::INVALID_ARGUMENT, "grpc.listen_addr must not be empty");
    }
    if (config.admin_listen_addr.empty()) {
        return Status(grpc::StatusCode::INVALID_ARGUMENT, "admin.listen_addr must not be empty");
    }
    if (!HasFixedListenPort(config.grpc_listen_addr)) {
        return Status(grpc::StatusCode::INVALID_ARGUMENT, "grpc.listen_addr requires a fixed TCP port (1-65535)");
    }
    if (!HasFixedListenPort(config.admin_listen_addr)) {
        return Status(grpc::StatusCode::INVALID_ARGUMENT, "admin.listen_addr requires a fixed TCP port (1-65535)");
    }
    if (config.grpc_listen_addr == config.admin_listen_addr) {
        return Status(grpc::StatusCode::INVALID_ARGUMENT,
                      "grpc.listen_addr and admin.listen_addr must be different");
    }
    if (config.storage_path.empty()) {
        return Status(grpc::StatusCode::INVALID_ARGUMENT, "storage.path must not be empty");
    }
    if (config.max_payload_bytes == 0) {
        return Status(grpc::StatusCode::INVALID_ARGUMENT, "limits.max_payload_bytes must be greater than 0");
    }
    if (config.max_payload_bytes > kMaxPayloadBytesLimit) {
        return Status(grpc::StatusCode::INVALID_ARGUMENT,
                      "limits.max_payload_bytes must not exceed 62914560 (60 MiB)");
    }
    return Status::Ok();
}

Result<ServerConfig> LoadServerConfig(const std::string& path)
{
    if (path.empty()) {
        return Status(grpc::StatusCode::INVALID_ARGUMENT, "config path must be provided");
    }

    std::error_code ec;
    if (!std::filesystem::exists(path, ec)) {
        if (ec) {
            return Status(grpc::StatusCode::INVALID_ARGUMENT, "stat config file: " + ec.message());
        }
        return Status(grpc::StatusCode::INVALID_ARGUMENT, "config file does not exist: " + path);
    }
    if (!std::filesystem::is_regular_file(path, ec)) {
        if (ec) {
            return Status(grpc::StatusCode::INVALID_ARGUMENT, "stat config file: " + ec.message());
        }
        return Status(grpc::StatusCode::INVALID_ARGUMENT, "config path is not a regular file: " + path);
    }

    ServerConfig config;
    try {
        YAML::Node root = YAML::LoadFile(path);
        AssignIfPresent(root["grpc"], "listen_addr", &config.grpc_listen_addr);
        AssignIfPresent(root["admin"], "listen_addr", &config.admin_listen_addr);
        AssignIfPresent(root["storage"], "path", &config.storage_path);
        AssignIfPresent(root["limits"], "max_payload_bytes", &config.max_payload_bytes);
    } catch (const YAML::Exception& e) {
        return Status(grpc::StatusCode::INVALID_ARGUMENT, "load config file: " + std::string(e.what()));
    }

    Status status = ValidateServerConfig(config);
    if (!status.ok()) {
        return status;
    }
    config.storage_path = std::filesystem::absolute(config.storage_path, ec).string();
    if (ec) {
        return Status(grpc::StatusCode::UNAVAILABLE, "resolve storage.path: " + ec.message());
    }
    return config;
}

}  // namespace openevent

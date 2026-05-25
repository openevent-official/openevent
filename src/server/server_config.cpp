#include "server/server_config.h"

#include <filesystem>

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

}  // namespace

Status ValidateServerConfig(const ServerConfig& config)
{
    if (config.grpc_listen_addr.empty()) {
        return Status(grpc::StatusCode::INVALID_ARGUMENT, "grpc.listen_addr must not be empty");
    }
    if (config.admin_listen_addr.empty()) {
        return Status(grpc::StatusCode::INVALID_ARGUMENT, "admin.listen_addr must not be empty");
    }
    if (config.metadata_path.empty()) {
        return Status(grpc::StatusCode::INVALID_ARGUMENT, "storage.metadata_path must not be empty");
    }
    if (config.message_store_path.empty()) {
        return Status(grpc::StatusCode::INVALID_ARGUMENT, "store.rocksdb.path must not be empty");
    }
    if (config.max_payload_bytes == 0) {
        return Status(grpc::StatusCode::INVALID_ARGUMENT, "limits.max_payload_bytes must be greater than 0");
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
        AssignIfPresent(root["storage"], "metadata_path", &config.metadata_path);
        AssignIfPresent(root["limits"], "max_payload_bytes", &config.max_payload_bytes);
        AssignIfPresent(root["log"], "level", &config.log_level);
        if (root["store"] && root["store"]["rocksdb"]) {
            AssignIfPresent(root["store"]["rocksdb"], "path", &config.message_store_path);
        }
    } catch (const YAML::Exception& e) {
        return Status(grpc::StatusCode::INVALID_ARGUMENT, "load config file: " + std::string(e.what()));
    }

    Status status = ValidateServerConfig(config);
    if (!status.ok()) {
        return status;
    }
    return config;
}

}  // namespace openevent

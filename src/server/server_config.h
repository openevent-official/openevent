#pragma once

#include <cstddef>
#include <string>

#include "common/status.h"

namespace openevent {

struct ServerConfig {
    std::string grpc_listen_addr = "0.0.0.0:9527";
    std::string admin_listen_addr = "127.0.0.1:9528";
    std::string metadata_path;
    std::string message_store_path;
    size_t max_payload_bytes = 16777216;
    std::string log_level = "info";
};

Result<ServerConfig> LoadServerConfig(const std::string& path);
Status ValidateServerConfig(const ServerConfig& config);

}  // namespace openevent

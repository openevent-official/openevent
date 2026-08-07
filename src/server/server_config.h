#pragma once

#include <cstddef>
#include <cstdint>
#include <string>

#include "common/status.h"

namespace openevent {

struct ServerConfig {
    std::string grpc_listen_addr = "0.0.0.0:9527";
    std::string admin_listen_addr = "127.0.0.1:9528";
    std::string storage_path;
    size_t max_payload_bytes = 16777216;
};

Result<ServerConfig> LoadServerConfig(const std::string& path);
Status ValidateServerConfig(const ServerConfig& config);

}  // namespace openevent

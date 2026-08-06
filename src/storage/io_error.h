#pragma once

#include <cerrno>
#include <cstring>
#include <string>

#include "common/status.h"

namespace openevent {

inline std::string ErrnoMessage(const std::string& operation, int error)
{
    return operation + ": " + std::strerror(error);
}

inline Status ObjectIoStatus(const std::string& operation, int error)
{
    if (error == ENOSPC || error == EDQUOT) {
        return Status(grpc::StatusCode::RESOURCE_EXHAUSTED, ErrnoMessage(operation, error));
    }
    return Status(grpc::StatusCode::UNAVAILABLE, ErrnoMessage(operation, error));
}

}  // namespace openevent

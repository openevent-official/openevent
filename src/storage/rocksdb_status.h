#pragma once

#include <string>

#include <rocksdb/status.h>

#include "common/status.h"

namespace openevent {

inline Status RocksToStatus(const rocksdb::Status& status, const std::string& operation)
{
    if (status.ok()) {
        return Status::Ok();
    }
    const auto code = status.IsCorruption() ? grpc::StatusCode::DATA_LOSS
                                           : grpc::StatusCode::UNAVAILABLE;
    // RocksDB's free-form error text can include database keys or values.
    const rocksdb::Status classification(status.code(), status.subcode(), status.severity(), rocksdb::Slice());
    const std::string category = classification.ToString();
    return Status::Fatal(code, operation + ": " + category)
        .WithStorageDiagnostic(operation, category);
}

}  // namespace openevent

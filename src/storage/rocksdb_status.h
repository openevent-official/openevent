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
    // Reconstruct from error enums only: RocksDB's free-form state can contain
    // database keys, credentials or payload and must not enter the diagnostic.
    const rocksdb::Status diagnostic(status.code(), status.subcode(), status.severity(), rocksdb::Slice());
    return Status::Fatal(code, operation + ": " + status.ToString())
        .WithStorageDiagnostic(operation, diagnostic.ToString());
}

}  // namespace openevent

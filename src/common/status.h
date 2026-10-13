#pragma once

#include <grpcpp/support/status.h>

#include <string>
#include <utility>

namespace openevent {

class Status {
public:
    Status() = default;
    Status(grpc::StatusCode code, std::string message)
        : code_(code), message_(std::move(message)) {}

    static Status Fatal(grpc::StatusCode code, std::string message)
    {
        Status status(code, std::move(message));
        status.requires_server_exit_ = true;
        return status;
    }

    static Status Ok() { return {}; }

    bool ok() const { return code_ == grpc::StatusCode::OK; }
    grpc::StatusCode code() const { return code_; }
    const std::string& message() const { return message_; }
    bool requires_server_exit() const { return requires_server_exit_; }

    // Only supply server-controlled operation names and error descriptions here.
    // message_ may contain storage keys or record bytes and must never be logged.
    Status WithStorageDiagnostic(std::string operation, std::string error) const
    {
        Status status = *this;
        status.storage_diagnostic_ = "operation=" + std::move(operation) +
                                     "; error=" + std::move(error);
        return status;
    }

    static Status StorageCorruption(std::string operation, std::string error)
    {
        return Status(grpc::StatusCode::DATA_LOSS, error)
            .WithStorageDiagnostic(std::move(operation), std::move(error));
    }

    std::string FatalLogMessage() const
    {
        return "fatal storage error: " +
               (storage_diagnostic_.empty() ? "operation=storage; error=unclassified failure"
                                            : storage_diagnostic_) +
               "; grpc_code=" + std::to_string(static_cast<int>(code_));
    }

    grpc::Status ToGrpc() const
    {
        if (ok()) {
            return grpc::Status::OK;
        }
        return grpc::Status(code_, message_);
    }

private:
    grpc::StatusCode code_ = grpc::StatusCode::OK;
    std::string message_;
    bool requires_server_exit_ = false;
    std::string storage_diagnostic_;
};

template <typename T>
class Result {
public:
    Result(T value) : value_(std::move(value)), status_(Status::Ok()) {}
    Result(Status status) : status_(std::move(status)) {}

    bool ok() const { return status_.ok(); }
    const Status& status() const { return status_; }
    T& value() { return value_; }
    const T& value() const { return value_; }

private:
    T value_{};
    Status status_;
};

}  // namespace openevent

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

    static Status Ok() { return {}; }

    bool ok() const { return code_ == grpc::StatusCode::OK; }
    grpc::StatusCode code() const { return code_; }
    const std::string& message() const { return message_; }

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

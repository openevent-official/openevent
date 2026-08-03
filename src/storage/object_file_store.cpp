#include "storage/object_file_store.h"

#include <algorithm>
#include <cerrno>
#include <climits>
#include <cstring>
#include <string>

#include <fcntl.h>
#include <linux/fs.h>
#include <openssl/crypto.h>
#include <openssl/evp.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <unistd.h>

namespace openevent {
namespace {

constexpr uint64_t kMaxObjectBytes = 4ULL * 1024 * 1024;
constexpr size_t kSha256Bytes = 32;

std::string ErrnoMessage(const std::string& operation, int error)
{
    return operation + ": " + std::strerror(error);
}

Status IoStatus(const std::string& operation, int error)
{
    if (error == ENOSPC || error == EDQUOT) {
        return Status(grpc::StatusCode::RESOURCE_EXHAUSTED, ErrnoMessage(operation, error));
    }
    return Status(grpc::StatusCode::UNAVAILABLE, ErrnoMessage(operation, error));
}

Status CorruptionStatus(const std::string& message)
{
    return Status(grpc::StatusCode::DATA_LOSS, message);
}

std::string FinalName(uint64_t object_id)
{
    return std::to_string(object_id);
}

std::string TemporaryName(uint64_t object_id)
{
    return ".tmp." + std::to_string(object_id);
}

Status CloseFile(int fd, const std::string& description)
{
    if (::close(fd) == 0) {
        return Status::Ok();
    }
    return IoStatus("close " + description, errno);
}

Status UnlinkKnownPath(int directory_fd, const std::string& name)
{
    if (::unlinkat(directory_fd, name.c_str(), 0) == 0 || errno == ENOENT) {
        return Status::Ok();
    }
    const int error = errno;
    if (error == EISDIR || error == EPERM) {
        return CorruptionStatus("object path is not a removable file: " + name);
    }
    return IoStatus("remove object path " + name, error);
}

}  // namespace

ObjectFileStore::ObjectFileStore(int directory_fd) : directory_fd_(directory_fd) {}

ObjectFileStore::~ObjectFileStore()
{
    if (directory_fd_ >= 0) {
        ::close(directory_fd_);
    }
}

Result<std::unique_ptr<ObjectFileStore>> ObjectFileStore::Open(const std::string& path)
{
    const int fd = ::open(path.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW);
    if (fd < 0) {
        return IoStatus("open object directory", errno);
    }
    return std::unique_ptr<ObjectFileStore>(new ObjectFileStore(fd));
}

Result<std::string> ObjectFileStore::Sha256(const std::string& data)
{
    EVP_MD_CTX* raw_context = EVP_MD_CTX_new();
    if (raw_context == nullptr) {
        return Status(grpc::StatusCode::INTERNAL, "allocate SHA-256 context failed");
    }
    std::unique_ptr<EVP_MD_CTX, decltype(&EVP_MD_CTX_free)> context(raw_context, EVP_MD_CTX_free);
    unsigned char digest[EVP_MAX_MD_SIZE];
    unsigned int digest_size = 0;
    if (EVP_DigestInit_ex(context.get(), EVP_sha256(), nullptr) != 1 ||
        EVP_DigestUpdate(context.get(), data.data(), data.size()) != 1 ||
        EVP_DigestFinal_ex(context.get(), digest, &digest_size) != 1 || digest_size != kSha256Bytes) {
        return Status(grpc::StatusCode::INTERNAL, "compute SHA-256 failed");
    }
    return std::string(reinterpret_cast<const char*>(digest), digest_size);
}

bool ObjectFileStore::ConstantTimeEquals(const std::string& left, const std::string& right)
{
    if (left.size() != right.size()) {
        return false;
    }
    return CRYPTO_memcmp(left.data(), right.data(), left.size()) == 0;
}

Status ObjectFileStore::Write(uint64_t object_id, const std::string& data) const
{
    const std::string temporary = TemporaryName(object_id);
    const std::string final = FinalName(object_id);
    int fd = ::openat(directory_fd_,
                      temporary.c_str(),
                      O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC | O_NOFOLLOW,
                      0600);
    if (fd < 0) {
        const int error = errno;
        if (error == EEXIST) {
            return CorruptionStatus("unexpected existing temporary object file: " + temporary);
        }
        return IoStatus("create temporary object file", error);
    }

    size_t written = 0;
    while (written < data.size()) {
        const size_t remaining = data.size() - written;
        const size_t count = std::min(remaining, static_cast<size_t>(SSIZE_MAX));
        const ssize_t result = ::write(fd, data.data() + written, count);
        if (result < 0) {
            const int error = errno;
            if (error == EINTR) {
                continue;
            }
            ::close(fd);
            return IoStatus("write temporary object file", error);
        }
        if (result == 0) {
            ::close(fd);
            return Status(grpc::StatusCode::UNAVAILABLE, "write temporary object file returned zero");
        }
        written += static_cast<size_t>(result);
    }

    if (::fsync(fd) != 0) {
        const int error = errno;
        ::close(fd);
        return IoStatus("fsync temporary object file", error);
    }
    Status close_status = CloseFile(fd, "temporary object file");
    if (!close_status.ok()) {
        return close_status;
    }

    if (::syscall(SYS_renameat2,
                  directory_fd_,
                  temporary.c_str(),
                  directory_fd_,
                  final.c_str(),
                  RENAME_NOREPLACE) != 0) {
        const int error = errno;
        if (error == EEXIST) {
            return CorruptionStatus("unexpected existing final object file: " + final);
        }
        return IoStatus("rename temporary object file", error);
    }
    if (::fsync(directory_fd_) != 0) {
        return IoStatus("fsync object directory", errno);
    }
    return Status::Ok();
}

Status ObjectFileStore::Cleanup(uint64_t object_id) const
{
    Status temporary_status = UnlinkKnownPath(directory_fd_, TemporaryName(object_id));
    if (!temporary_status.ok()) {
        return temporary_status;
    }
    Status final_status = UnlinkKnownPath(directory_fd_, FinalName(object_id));
    if (!final_status.ok()) {
        return final_status;
    }
    if (::fsync(directory_fd_) != 0) {
        return IoStatus("fsync object directory after cleanup", errno);
    }
    return Status::Ok();
}

Result<std::string> ObjectFileStore::ReadAndValidate(uint64_t object_id,
                                                     uint64_t expected_size,
                                                     const std::string& expected_sha256) const
{
    if (expected_size == 0 || expected_size > kMaxObjectBytes || expected_sha256.size() != kSha256Bytes) {
        return CorruptionStatus("invalid committed object metadata");
    }

    const std::string final = FinalName(object_id);
    int fd = -1;
    do {
        fd = ::openat(directory_fd_, final.c_str(), O_RDONLY | O_CLOEXEC | O_NOFOLLOW);
    } while (fd < 0 && errno == EINTR);
    if (fd < 0) {
        const int error = errno;
        if (error == ENOENT || error == ELOOP) {
            return CorruptionStatus(ErrnoMessage("open committed object file " + final, error));
        }
        return IoStatus("open committed object file " + final, error);
    }

    struct stat file_stat {};
    if (::fstat(fd, &file_stat) != 0) {
        const int error = errno;
        ::close(fd);
        return IoStatus("stat committed object file " + final, error);
    }
    if (!S_ISREG(file_stat.st_mode) || file_stat.st_size < 0 ||
        static_cast<uint64_t>(file_stat.st_size) != expected_size) {
        ::close(fd);
        return CorruptionStatus("committed object file type or size mismatch: " + final);
    }

    std::string data(static_cast<size_t>(expected_size), '\0');
    size_t read_bytes = 0;
    while (read_bytes < data.size()) {
        const ssize_t result = ::read(fd, data.data() + read_bytes, data.size() - read_bytes);
        if (result < 0) {
            const int error = errno;
            if (error == EINTR) {
                continue;
            }
            ::close(fd);
            return IoStatus("read committed object file " + final, error);
        }
        if (result == 0) {
            ::close(fd);
            return CorruptionStatus("committed object file is truncated: " + final);
        }
        read_bytes += static_cast<size_t>(result);
    }
    Status close_status = CloseFile(fd, "committed object file");
    if (!close_status.ok()) {
        return close_status;
    }

    auto digest = Sha256(data);
    if (!digest.ok()) {
        return digest.status();
    }
    if (CRYPTO_memcmp(digest.value().data(), expected_sha256.data(), kSha256Bytes) != 0) {
        return CorruptionStatus("committed object file SHA-256 mismatch: " + final);
    }
    return data;
}

}  // namespace openevent

#include "storage/object_file_store.h"

#include <algorithm>
#include <climits>
#include <string>
#include <utility>

#include <fcntl.h>
#include <openssl/crypto.h>
#include <sys/stat.h>
#include <unistd.h>

#include "storage/io_error.h"

namespace openevent {
namespace {

Status CloseFile(int fd, const std::string& description)
{
    if (::close(fd) == 0) {
        return Status::Ok();
    }
    return ObjectIoStatus("close " + description, errno);
}

Status UnlinkKnownPath(int directory_fd, const std::string& name)
{
    if (::unlinkat(directory_fd, name.c_str(), 0) == 0 || errno == ENOENT) {
        return Status::Ok();
    }
    const int error = errno;
    if (error == EISDIR || error == EPERM) {
        return Status::StorageCorruption("remove object path", "object path is not a removable file: " + name);
    }
    return ObjectIoStatus("remove object path " + name, error);
}

}  // namespace

ObjectFileStore::ObjectFileStore(int directory_fd, StorageFaultInjector fault_injector)
    : directory_fd_(directory_fd), fault_injector_(std::move(fault_injector))
{
}

ObjectFileStore::~ObjectFileStore()
{
    if (directory_fd_ >= 0) {
        ::close(directory_fd_);
    }
}

Result<std::unique_ptr<ObjectFileStore>> ObjectFileStore::Open(
    const std::string& path,
    StorageFaultInjector fault_injector)
{
    const int fd = ::open(path.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW);
    if (fd < 0) {
        return ObjectIoStatus("open object directory", errno);
    }
    return std::unique_ptr<ObjectFileStore>(new ObjectFileStore(fd, std::move(fault_injector)));
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
    const std::string name = std::to_string(object_id);
    const int fd = ::openat(directory_fd_, name.c_str(),
                            O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC | O_NOFOLLOW, 0600);
    if (fd < 0) {
        const int error = errno;
        if (error == EEXIST) {
            return Status::StorageCorruption("create object file", "unexpected existing object file: " + name);
        }
        return ObjectIoStatus("create object file", error);
    }

    Status injected = InjectStorageFault(fault_injector_, StorageFaultPoint::kAfterObjectFileCreate);
    if (!injected.ok()) {
        ::close(fd);
        return injected;
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
            return ObjectIoStatus("write object file", error);
        }
        if (result == 0) {
            ::close(fd);
            return Status(grpc::StatusCode::UNAVAILABLE, "write object file returned zero");
        }
        written += static_cast<size_t>(result);
    }

    injected = InjectStorageFault(fault_injector_, StorageFaultPoint::kAfterObjectFileWrite);
    if (!injected.ok()) {
        ::close(fd);
        return injected;
    }
    if (::fsync(fd) != 0) {
        const int error = errno;
        ::close(fd);
        return ObjectIoStatus("fsync object file", error);
    }
    Status close_status = CloseFile(fd, "object file");
    if (!close_status.ok()) {
        return close_status;
    }

    if (::fsync(directory_fd_) != 0) {
        return ObjectIoStatus("fsync object directory", errno);
    }
    return Status::Ok();
}

Status ObjectFileStore::Cleanup(uint64_t object_id) const
{
    return Cleanup(std::vector<uint64_t>{object_id});
}

Status ObjectFileStore::Cleanup(const std::vector<uint64_t>& object_ids) const
{
    for (uint64_t object_id : object_ids) {
        Status status = UnlinkKnownPath(directory_fd_, std::to_string(object_id));
        if (!status.ok()) {
            return status;
        }
    }
    if (object_ids.empty()) {
        return Status::Ok();
    }
    if (::fsync(directory_fd_) != 0) {
        return ObjectIoStatus("fsync object directory after cleanup", errno);
    }
    return Status::Ok();
}

Result<std::string> ObjectFileStore::ReadRange(uint64_t object_id,
                                               uint64_t expected_size,
                                               uint64_t offset,
                                               uint64_t nbytes) const
{
    const std::string final = std::to_string(object_id);
    struct stat file_stat {};
    int fd = -1;
    do {
        fd = ::openat(directory_fd_, final.c_str(), O_RDONLY | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK);
    } while (fd < 0 && errno == EINTR);
    if (fd < 0) {
        const int error = errno;
        if (error == ENOENT || error == ELOOP) {
            return Status::StorageCorruption("open committed object file " + final,
                                             ErrnoMessage("open committed object file " + final, error));
        }
        if (::fstatat(directory_fd_, final.c_str(), &file_stat, AT_SYMLINK_NOFOLLOW) == 0 &&
            !S_ISREG(file_stat.st_mode)) {
            return Status::StorageCorruption("stat committed object file",
                                             "committed object file is not a regular file: " + final);
        }
        return ObjectIoStatus("open committed object file " + final, error);
    }

    if (::fstat(fd, &file_stat) != 0) {
        const int error = errno;
        ::close(fd);
        return ObjectIoStatus("stat committed object file " + final, error);
    }
    if (!S_ISREG(file_stat.st_mode) || file_stat.st_size < 0 ||
        static_cast<uint64_t>(file_stat.st_size) != expected_size) {
        ::close(fd);
        return Status::StorageCorruption("stat committed object file",
                                         "committed object file type or size mismatch: " + final);
    }

    const uint64_t remaining = expected_size - offset;
    const size_t count = static_cast<size_t>(std::min(nbytes, remaining));
    std::string data(count, '\0');
    size_t read_bytes = 0;
    while (read_bytes < data.size()) {
        const ssize_t result = ::pread(fd,
                                       data.data() + read_bytes,
                                       data.size() - read_bytes,
                                       static_cast<off_t>(offset + read_bytes));
        if (result < 0) {
            const int error = errno;
            if (error == EINTR) {
                continue;
            }
            ::close(fd);
            return ObjectIoStatus("read committed object file range " + final, error);
        }
        if (result == 0) {
            ::close(fd);
            return Status::StorageCorruption("read committed object file",
                                             "committed object file is truncated: " + final);
        }
        read_bytes += static_cast<size_t>(result);
    }
    Status close_status = CloseFile(fd, "committed object file");
    if (!close_status.ok()) {
        return close_status;
    }

    return data;
}

}  // namespace openevent

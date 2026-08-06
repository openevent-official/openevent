#pragma once

#include <functional>

#include "common/status.h"

namespace openevent {

enum class StorageFaultPoint {
    kBeforeCommit,
    kAfterCommit,
    kAfterTemporaryFileCreate,
    kBeforeTemporaryFileWrite,
    kAfterTemporaryFileWrite,
    kBeforeFileFsync,
    kAfterFileFsync,
    kBeforeRename,
    kAfterRename,
    kBeforeDirectoryFsync,
    kAfterDirectoryFsync,
};

using StorageFaultInjector = std::function<Status(StorageFaultPoint)>;

inline Status InjectStorageFault(const StorageFaultInjector& injector, StorageFaultPoint point)
{
    return injector ? injector(point) : Status::Ok();
}

}  // namespace openevent

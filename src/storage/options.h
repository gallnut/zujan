#pragma once

#include "snapshot.h"

namespace zujan
{
namespace storage
{

struct ReadOptions
{
    bool            verify_checksums = false;
    bool            fill_cache = true;
    const Snapshot* snapshot = nullptr;
};

struct WriteOptions
{
    bool sync = false;
};

}  // namespace storage
}  // namespace zujan

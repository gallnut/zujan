#pragma once
#include <cstddef>
#include <cstdint>

namespace zujan
{
namespace storage
{

// MurmurHash3 (32-bit version) inspired hash function
// Used for internal data structures and bloom filters.
uint32_t Hash(const char *data, size_t n, uint32_t seed);

}  // namespace storage
}  // namespace zujan

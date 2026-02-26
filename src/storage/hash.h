#pragma once
#include <cstddef>
#include <cstdint>

namespace zujan
{
namespace storage
{

/**
 * @brief MurmurHash3 (32-bit version) inspired hash function
 * Used for internal data structures and bloom filters.
 *
 * @param data Pointer to the data to hash
 * @param n Length of the data in bytes
 * @param seed Hash seed
 * @return uint32_t Computed hash value
 */
uint32_t Hash(const char *data, size_t n, uint32_t seed);

}  // namespace storage
}  // namespace zujan

#pragma once

#include <cstdint>
#include <memory>
#include <string_view>

namespace zujan
{
namespace storage
{

class Cache;

/**
 * @brief Create a new cache with a fixed size capacity. This implementation of Cache uses a least-recently-used
 * eviction policy.
 * @param capacity The maximum capacity of the cache
 * @return std::shared_ptr<Cache> A shared pointer to the newly created cache
 */
std::shared_ptr<Cache> NewLRUCache(size_t capacity);

/**
 * @brief A Cache is an interface that maps keys to values. It has internal
 * synchronization and may be safely accessed concurrently from
 * multiple threads. It may automatically evict entries to make room
 * for new entries. Values have a specified charge against the cache
 * capacity.
 */
class Cache
{
public:
    Cache() = default;

    Cache(const Cache &) = delete;
    Cache &operator=(const Cache &) = delete;

    virtual ~Cache();

    /**
     * @brief Opaque handle to an entry stored in the cache.
     */
    struct Handle
    {
    };

    /**
     * @brief Insert a mapping from key->value into the cache and assign it
     * the specified charge against the total cache capacity.
     * When the inserted entry is no longer needed, the key and
     * value will be passed to "deleter".
     *
     * @param key The key to insert
     * @param value The value to insert
     * @param charge The charge against the total cache capacity
     * @param deleter Function to call when the entry is no longer needed
     * @return Handle* A handle that corresponds to the mapping. The caller must call this->Release(handle) when the
     * returned mapping is no longer needed.
     */
    virtual Handle *Insert(std::string_view key, void *value, size_t charge,
                           void (*deleter)(std::string_view key, void *value)) = 0;

    /**
     * @brief If the cache has no mapping for "key", returns nullptr.
     * Else return a handle that corresponds to the mapping.
     *
     * @param key The key to lookup
     * @return Handle* A handle that corresponds to the mapping. The caller must call this->Release(handle) when the
     * returned mapping is no longer needed.
     */
    virtual Handle *Lookup(std::string_view key) = 0;

    /**
     * @brief Release a mapping returned by a previous Lookup().
     * REQUIRES: handle must not have been released yet.
     * REQUIRES: handle must have been returned by a method on *this.
     *
     * @param handle The handle to release
     */
    virtual void Release(Handle *handle) = 0;

    /**
     * @brief Return the value encapsulated in a handle returned by a successful Lookup().
     * REQUIRES: handle must not have been released yet.
     * REQUIRES: handle must have been returned by a method on *this.
     *
     * @param handle The handle for which to retrieve the value
     * @return void* The encapsulated value
     */
    virtual void *Value(Handle *handle) = 0;

    /**
     * @brief If the cache contains entry for key, erase it. Note that the
     * underlying entry will be kept around until all existing handles
     * to it have been released.
     *
     * @param key The key to erase
     */
    virtual void Erase(std::string_view key) = 0;

    /**
     * @brief Return a new numeric id. May be used by multiple clients who are
     * sharing the same cache to partition the key space.
     *
     * @return uint64_t A new numeric id
     */
    virtual uint64_t NewId() = 0;

    /**
     * @brief Remove all cache entries that are not currently in use.
     */
    virtual void Prune() {}

    /**
     * @brief Return an estimate of the combined charges of all elements stored in the cache
     *
     * @return size_t Combined charges
     */
    virtual size_t TotalCharge() const = 0;
};

}  // namespace storage
}  // namespace zujan

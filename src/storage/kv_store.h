#pragma once

#include <expected>
#include <optional>
#include <string>

#include "status.h"

namespace zujan
{
namespace storage
{

/**
 * @brief Abstract Key-Value Store interface for the storage engine
 * Designed to support our custom LSM-Tree or other standard engines (e.g., RocksDB)
 */
class KVStore
{
public:
    virtual ~KVStore() = default;

    /**
     * @brief Put a key-value pair into the store
     * @param key The key to insert
     * @param value The value to insert
     * @return std::expected<void, Error> Success or error status
     */
    virtual std::expected<void, Error> Put(const std::string &key, const std::string &value) noexcept = 0;

    /**
     * @brief Get the value associated with a key
     * @param key The key to lookup
     * @return std::expected<std::optional<std::string>, Error> Value if found, std::nullopt if not, or error status
     */
    virtual std::expected<std::optional<std::string>, Error> Get(const std::string &key) noexcept = 0;

    /**
     * @brief Delete a key from the store
     * @param key The key to delete
     * @return std::expected<void, Error> Success or error status
     */
    virtual std::expected<void, Error> Delete(const std::string &key) noexcept = 0;
};

}  // namespace storage
}  // namespace zujan

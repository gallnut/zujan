#pragma once

#include <atomic>
#include <optional>
#include <string>
#include <string_view>

#include "arena.h"
#include "skiplist.h"

namespace zujan
{
namespace storage
{

enum ValueType : unsigned char
{
    kTypeDeletion = 0x0,
    kTypeValue = 0x1
};

class MemTable
{
public:
    MemTable();
    ~MemTable() = default;

    MemTable(const MemTable &) = delete;
    MemTable &operator=(const MemTable &) = delete;

    /**
     * @brief Add an entry into memtable that maps key to value.
     * @param key The key to insert
     * @param value The value to insert
     */
    void Put(std::string_view key, std::string_view value);

    /**
     * @brief If memtable contains a value for key, return it.
     * @param key The key to lookup
     * @return std::optional<std::string> Value if found, std::nullopt otherwise
     */
    std::optional<std::string> Get(std::string_view key) const;

    /**
     * @brief Add a tombstone for key.
     * @param key The key to mark as deleted
     */
    void Delete(std::string_view key);

    /**
     * @brief Estimate the memory size of the MemTable
     * @return size_t Estimated size in bytes
     */
    size_t EstimateSize() const;

private:
    struct KeyComparator
    {
        int operator()(const char *a, const char *b) const;
    };

    typedef SkipList<const char *, KeyComparator> Table;

    KeyComparator         comparator_;
    Arena                 arena_;
    Table                 table_;
    std::atomic<uint64_t> sequence_;
};

}  // namespace storage
}  // namespace zujan

#pragma once

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
     */
    void Add(uint64_t seq, ValueType type, std::string_view key, std::string_view value);

    /**
     * @brief Get a value
     * @param key Key
     * @param value Extracted value
     * @param deleted true if key was found but deleted
     * @param seq Max sequence number for snapshot isolation
     * @return true if key was found (either value or deletion)
     */
    bool Get(std::string_view key, std::string &value, bool *deleted, uint64_t seq = 0xffffffffffffffffull);

    /**
     * @brief If memtable contains a value for key, return it.
     */
    std::optional<std::string> Get(std::string_view key) const;

    /**
     * @brief Estimate the memory size of the MemTable
     * @return size_t Estimated size in bytes
     */
    size_t EstimateSize() const;

    /**
     * @brief Writes all entries in the MemTable to a TableBuilder
     * @param builder The TableBuilder to append to
     */
    void WriteToBuilder(class TableBuilder *builder) const;

private:
    struct KeyComparator
    {
        int operator()(const char *a, const char *b) const;
    };

    typedef SkipList<const char *, KeyComparator> Table;

    KeyComparator comparator_;
    Arena         arena_;
    Table         table_;
};

}  // namespace storage
}  // namespace zujan

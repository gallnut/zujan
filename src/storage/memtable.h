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

    // Add an entry into memtable that maps key to value.
    void Put(std::string_view key, std::string_view value);

    // If memtable contains a value for key, return it.
    std::optional<std::string> Get(std::string_view key) const;

    // Add a tombstone for key.
    void Delete(std::string_view key);

    size_t EstimateSize() const;

    // No clear method, as Arena is freed on destruction. Creates a new MemTable
    // instead.

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

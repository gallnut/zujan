#pragma once

#include <cstdint>
#include <string_view>

#include "write_batch.h"

namespace zujan
{
namespace storage
{

class MemTable;

class WriteBatchInternal
{
public:
    static void     SetCount(WriteBatch* b, uint32_t n);
    static void     SetSequence(WriteBatch* b, uint64_t seq);
    static uint32_t Count(const WriteBatch* b);
    static uint64_t Sequence(const WriteBatch* b);
    static void     Append(WriteBatch* dst, const WriteBatch* src);
    static void     InsertInto(const WriteBatch* b, MemTable* memtable);

    static std::string_view Contents(const WriteBatch* b);
    static void             SetContents(WriteBatch* b, std::string_view contents);
};

}  // namespace storage
}  // namespace zujan

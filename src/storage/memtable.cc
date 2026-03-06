#include "memtable.h"

#include <cstring>

#include "coding.h"
#include "table_builder.h"

namespace zujan
{
namespace storage
{

int MemTable::KeyComparator::operator()(const char *a, const char *b) const
{
    // Extract internal key lengths
    uint32_t    a_len, b_len;
    const char *a_ptr = GetVarint32Ptr(a, a + 5, &a_len);
    const char *b_ptr = GetVarint32Ptr(b, b + 5, &b_len);

    // Compare user keys
    uint32_t a_user_len = a_len - 8;
    uint32_t b_user_len = b_len - 8;
    uint32_t min_len = (a_user_len < b_user_len) ? a_user_len : b_user_len;
    int      r = std::memcmp(a_ptr, b_ptr, min_len);
    if (r == 0)
    {
        if (a_user_len < b_user_len)
            r = -1;
        else if (a_user_len > b_user_len)
            r = 1;
        else
        {
            // User keys are equal, compare sequence numbers in descending order
            uint64_t a_seq = DecodeFixed64(a_ptr + a_user_len);
            uint64_t b_seq = DecodeFixed64(b_ptr + b_user_len);
            if (a_seq > b_seq)
                r = -1;
            else if (a_seq < b_seq)
                r = 1;
            else
                r = 0;
        }
    }
    return r;
}

MemTable::MemTable() : comparator_(), arena_(), table_(comparator_, &arena_) {}

size_t MemTable::EstimateSize() const { return arena_.MemoryUsage(); }

void MemTable::Add(uint64_t seq, ValueType type, std::string_view key, std::string_view value)
{
    uint32_t     key_size = key.size();
    uint32_t     val_size = value.size();
    uint32_t     internal_key_size = key_size + 8;
    const size_t encoded_len = 5 + internal_key_size + 5 + val_size;

    char *buf = arena_.Allocate(encoded_len);
    char *p = EncodeVarint32(buf, internal_key_size);
    std::memcpy(p, key.data(), key_size);
    p += key_size;
    EncodeFixed64(p, (seq << 8) | type);
    p += 8;
    p = EncodeVarint32(p, val_size);
    if (val_size > 0)
    {
        std::memcpy(p, value.data(), val_size);
    }
    p += val_size;

    table_.Insert(buf);
}

std::optional<std::string> MemTable::Get(std::string_view key) const
{
    std::string val;
    bool        deleted = false;
    if (const_cast<MemTable *>(this)->Get(key, val, &deleted))
    {
        if (deleted) return std::nullopt;
        return val;
    }
    return std::nullopt;
}

bool MemTable::Get(std::string_view key, std::string &value, bool *deleted, uint64_t seq)
{
    // Ensure accurate snapshot by creating a lookup key that requests up to `seq`
    std::string lookup_key_buf(key.length() + 8, '\0');
    uint64_t    tag = (seq << 8) | static_cast<uint8_t>(kTypeValue);

    std::memcpy(lookup_key_buf.data(), key.data(), key.length());
    EncodeFixed64(&lookup_key_buf[key.length()], tag);

    uint32_t     internal_key_size = key.length() + 8;
    const size_t encoded_len = 5 + internal_key_size;
    char        *lookup_key = static_cast<char *>(alloca(encoded_len));
    char        *p = EncodeVarint32(lookup_key, internal_key_size);
    std::memcpy(p, lookup_key_buf.data(), internal_key_size);

    Table::Iterator iter(&table_);
    iter.Seek(lookup_key);

    if (iter.Valid())
    {
        const char *entry = iter.key();

        uint32_t    entry_internal_key_size;
        const char *key_ptr = GetVarint32Ptr(entry, entry + 5, &entry_internal_key_size);

        if (entry_internal_key_size < 8) return false;

        const char *user_key_ptr = key_ptr;
        size_t      user_key_len = entry_internal_key_size - 8;

        if (std::string_view(user_key_ptr, user_key_len) == key)
        {
            uint64_t  entry_tag = DecodeFixed64(user_key_ptr + user_key_len);
            ValueType type = static_cast<ValueType>(entry_tag & 0xff);

            if (type == kTypeValue)
            {
                uint32_t    value_size;
                const char *val_ptr = GetVarint32Ptr(key_ptr + entry_internal_key_size,
                                                     key_ptr + entry_internal_key_size + 5, &value_size);
                value.assign(val_ptr, value_size);
                if (deleted) *deleted = false;
                return true;
            }
            else if (type == kTypeDeletion)
            {
                if (deleted) *deleted = true;
                return true;
            }
        }
    }

    return false;
}

void MemTable::WriteToBuilder(TableBuilder *builder) const
{
    Table::Iterator iter(&table_);
    iter.SeekToFirst();
    std::string last_user_key = "";
    bool        first = true;

    while (iter.Valid())
    {
        const char      *entry = iter.key();
        uint32_t         internal_key_size;
        const char      *key_ptr = GetVarint32Ptr(entry, entry + 5, &internal_key_size);
        uint32_t         user_key_size = internal_key_size - 8;
        std::string_view user_key(key_ptr, user_key_size);

        if (first || user_key != last_user_key)
        {
            uint64_t  tag = DecodeFixed64(key_ptr + user_key_size);
            ValueType type = static_cast<ValueType>(tag & 0xff);

            const char *val_ptr = key_ptr + internal_key_size;
            uint32_t    val_length;
            val_ptr = GetVarint32Ptr(val_ptr, val_ptr + 5, &val_length);
            std::string_view actual_value(val_ptr, val_length);

            std::string encoded_value;
            encoded_value.push_back(static_cast<char>(type));
            encoded_value.append(actual_value);

            builder->Add(user_key, encoded_value);

            last_user_key = std::string(user_key);
            first = false;
        }
        iter.Next();
    }
}

}  // namespace storage
}  // namespace zujan

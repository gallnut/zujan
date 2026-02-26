#include "memtable.h"
#include "coding.h"
#include <cstring>

namespace zujan {
namespace storage {

int MemTable::KeyComparator::operator()(const char *a, const char *b) const {
  // Extract internal key lengths
  uint32_t a_len, b_len;
  const char *a_ptr = GetVarint32Ptr(a, a + 5, &a_len);
  const char *b_ptr = GetVarint32Ptr(b, b + 5, &b_len);

  // Compare user keys
  uint32_t a_user_len = a_len - 8;
  uint32_t b_user_len = b_len - 8;
  uint32_t min_len = (a_user_len < b_user_len) ? a_user_len : b_user_len;
  int r = std::memcmp(a_ptr, b_ptr, min_len);
  if (r == 0) {
    if (a_user_len < b_user_len)
      r = -1;
    else if (a_user_len > b_user_len)
      r = 1;
    else {
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

MemTable::MemTable()
    : comparator_(), arena_(), table_(comparator_, &arena_), sequence_(1) {}

size_t MemTable::EstimateSize() const { return arena_.MemoryUsage(); }

void MemTable::Put(std::string_view key, std::string_view value) {
  uint64_t seq = sequence_.fetch_add(1, std::memory_order_relaxed);
  uint32_t key_size = key.size();
  uint32_t val_size = value.size();
  uint32_t internal_key_size = key_size + 8; // sequence(7 bytes) + type(1 byte)
  const size_t encoded_len =
      5 + internal_key_size + 5 + val_size; // Max 5 bytes for varints

  char *buf = arena_.Allocate(encoded_len);
  char *p = EncodeVarint32(buf, internal_key_size);
  std::memcpy(p, key.data(), key_size);
  p += key_size;
  EncodeFixed64(p, (seq << 8) | kTypeValue);
  p += 8;
  p = EncodeVarint32(p, val_size);
  std::memcpy(p, value.data(), val_size);
  p += val_size;

  // Note: we allocated more than needed, but Arena can't shrink. It's fine for
  // small varint overhead.
  table_.Insert(buf);
}

void MemTable::Delete(std::string_view key) {
  uint64_t seq = sequence_.fetch_add(1, std::memory_order_relaxed);
  uint32_t key_size = key.size();
  uint32_t internal_key_size = key_size + 8;
  const size_t encoded_len = 5 + internal_key_size + 5; // value size is 0

  char *buf = arena_.Allocate(encoded_len);
  char *p = EncodeVarint32(buf, internal_key_size);
  std::memcpy(p, key.data(), key_size);
  p += key_size;
  EncodeFixed64(p, (seq << 8) | kTypeDeletion);
  p += 8;
  p = EncodeVarint32(p, 0); // 0 value size

  table_.Insert(buf);
}

std::optional<std::string> MemTable::Get(std::string_view key) const {
  uint32_t key_size = key.size();
  uint32_t internal_key_size = key_size + 8;
  const size_t encoded_len = 5 + internal_key_size;
  char *lookup_key = static_cast<char *>(alloca(encoded_len));

  char *p = EncodeVarint32(lookup_key, internal_key_size);
  std::memcpy(p, key.data(), key_size);
  p += key_size;
  // Maximum sequence number to seek to the latest version of this user key
  EncodeFixed64(p, (0xffffffffffffffffull << 8) | kTypeValue);

  Table::Iterator iter(&table_);
  iter.Seek(lookup_key);

  if (iter.Valid()) {
    const char *entry = iter.key();
    uint32_t entry_internal_key_size;
    const char *entry_key_ptr =
        GetVarint32Ptr(entry, entry + 5, &entry_internal_key_size);
    uint32_t entry_user_key_size = entry_internal_key_size - 8;

    // Check if user key matches
    if (entry_user_key_size == key_size &&
        std::memcmp(entry_key_ptr, key.data(), key_size) == 0) {
      uint64_t tag = DecodeFixed64(entry_key_ptr + entry_user_key_size);
      ValueType type = static_cast<ValueType>(tag & 0xff);
      if (type == kTypeValue) {
        const char *val_ptr = entry_key_ptr + entry_internal_key_size;
        uint32_t val_length;
        val_ptr = GetVarint32Ptr(val_ptr, val_ptr + 5, &val_length);
        return std::string(val_ptr, val_length);
      } else {
        return std::nullopt; // Deleted
      }
    }
  }
  return std::nullopt;
}

} // namespace storage
} // namespace zujan

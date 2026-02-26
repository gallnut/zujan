#include "block.h"
#include "coding.h"
#include <cassert>

namespace zujan {
namespace storage {

inline uint32_t Block::NumRestarts() const {
  assert(size_ >= sizeof(uint32_t));
  return DecodeFixed32(data_ + size_ - sizeof(uint32_t));
}

Block::Block(std::string_view contents)
    : data_(contents.data()), size_(contents.size()) {
  if (size_ < sizeof(uint32_t)) {
    size_ = 0; // Error marker
  } else {
    size_t max_restarts_allowed = (size_ - sizeof(uint32_t)) / sizeof(uint32_t);
    if (NumRestarts() > max_restarts_allowed) {
      size_ = 0;
    } else {
      restart_offset_ = size_ - (1 + NumRestarts()) * sizeof(uint32_t);
    }
  }
}

Block::Iterator *Block::NewIterator() const {
  if (size_ < sizeof(uint32_t)) {
    // Empty iterator
    return new Iterator(data_, 0, 0);
  }
  const uint32_t num_restarts = NumRestarts();
  return new Iterator(data_, restart_offset_, num_restarts);
}

Block::Iterator::Iterator(const char *data, uint32_t restarts,
                          uint32_t num_restarts)
    : data_(data), restarts_(restarts), num_restarts_(num_restarts),
      current_(restarts_), restart_index_(num_restarts_) {}

void Block::Iterator::Next() {
  assert(Valid());
  ParseNextKey();
}

void Block::Iterator::SeekToRestartPoint(uint32_t index) {
  key_.clear();
  restart_index_ = index;
  uint32_t offset = DecodeFixed32(data_ + restarts_ + index * sizeof(uint32_t));
  current_ = offset;
  ParseNextKey();
}

void Block::Iterator::ParseNextKey() {
  current_ += value_.size();
  if (current_ >= restarts_) {
    // End of block
    current_ = restarts_;
    restart_index_ = num_restarts_;
    return;
  }

  uint32_t shared, non_shared, value_length;
  const char *p = data_ + current_;
  const char *limit = data_ + restarts_;

  p = GetVarint32PtrFallback(p, limit, &shared);
  p = GetVarint32PtrFallback(p, limit, &non_shared);
  p = GetVarint32PtrFallback(p, limit, &value_length);

  key_.resize(shared);
  key_.append(p, non_shared);

  value_ = std::string_view(p + non_shared, value_length);

  while (restart_index_ + 1 < num_restarts_ &&
         DecodeFixed32(data_ + restarts_ +
                       (restart_index_ + 1) * sizeof(uint32_t)) <= current_) {
    ++restart_index_;
  }
}

void Block::Iterator::Seek(std::string_view target) {
  // Binary search in restart array
  uint32_t left = 0;
  uint32_t right = num_restarts_ - 1;
  while (left < right) {
    uint32_t mid = (left + right + 1) / 2;
    uint32_t region_offset =
        DecodeFixed32(data_ + restarts_ + mid * sizeof(uint32_t));
    uint32_t shared, non_shared, value_length;
    const char *key_ptr = GetVarint32PtrFallback(data_ + region_offset,
                                                 data_ + restarts_, &shared);
    key_ptr = GetVarint32PtrFallback(key_ptr, data_ + restarts_, &non_shared);
    key_ptr = GetVarint32PtrFallback(key_ptr, data_ + restarts_, &value_length);

    std::string_view mid_key(key_ptr, non_shared);
    if (mid_key < target) {
      left = mid;
    } else {
      right = mid - 1;
    }
  }

  SeekToRestartPoint(left);
  // Linear search for first key >= target
  while (Valid() && key_ < target) {
    ParseNextKey();
  }
}

} // namespace storage
} // namespace zujan

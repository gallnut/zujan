#pragma once

#include <atomic>
#include <cstddef>
#include <vector>

namespace zujan {
namespace storage {

// Arena provides a fast, fragmentation-free memory allocator for small
// allocations. It allocates memory in large blocks and doles out small chunks.
// It is not thread-safe for concurrent allocations, which is aligned with our
// single-writer SkipList design. The memory is freed all at once when the Arena
// is destroyed.
class Arena {
public:
  Arena();
  ~Arena();

  // No copying allowed
  Arena(const Arena &) = delete;
  Arena &operator=(const Arena &) = delete;

  // Return a pointer to a newly allocated memory block of "bytes" bytes.
  char *Allocate(size_t bytes);

  // Allocate memory with the normal alignment guarantees provided by malloc
  char *AllocateAligned(size_t bytes);

  // Returns an estimate of the total memory usage of data allocated
  // by the arena.
  size_t MemoryUsage() const {
    return memory_usage_.load(std::memory_order_relaxed);
  }

private:
  char *AllocateFallback(size_t bytes);
  char *AllocateNewBlock(size_t block_bytes);

  // Allocation state
  char *alloc_ptr_;
  size_t alloc_bytes_remaining_;

  // Array of new[] allocated memory blocks
  std::vector<char *> blocks_;

  // Total memory usage of the arena
  std::atomic<size_t> memory_usage_;
};

inline char *Arena::Allocate(size_t bytes) {
  // Typical fast path: there is enough room in the current block
  if (bytes <= alloc_bytes_remaining_) {
    char *result = alloc_ptr_;
    alloc_ptr_ += bytes;
    alloc_bytes_remaining_ -= bytes;
    return result;
  }
  return AllocateFallback(bytes);
}

} // namespace storage
} // namespace zujan

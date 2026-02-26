#pragma once

#include "io.h"
#include <expected>
#include <string>
#include <sys/types.h>

namespace zujan {
namespace storage {

class MemTable;

// Write-Ahead Log for durability using io_uring
class WAL {
public:
  explicit WAL(IOContext &io_ctx, const std::string &filepath);
  ~WAL();

  std::expected<void, Error> AppendPut(const std::string &key,
                                       const std::string &value) noexcept;
  std::expected<void, Error> AppendDelete(const std::string &key) noexcept;

  // Recovery routine to reconstruct MemTable on startup
  std::expected<void, Error> Recover(MemTable &memtable) noexcept;

  std::expected<void, Error> Sync() noexcept;

private:
  IOContext &io_ctx_;
  std::string filepath_;
  int fd_{-1};
  off_t current_offset_{0};
};

} // namespace storage
} // namespace zujan

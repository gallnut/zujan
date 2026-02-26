#pragma once

#include "io.h"
#include <span>

namespace zujan {
namespace storage {

// Fallback IO implementation for systems without io_uring
class PosixIOContext : public IOContext {
public:
  PosixIOContext() = default;
  ~PosixIOContext() override = default;

  PosixIOContext(const PosixIOContext &) = delete;
  PosixIOContext &operator=(const PosixIOContext &) = delete;

  std::expected<void, Error> Init() noexcept override;

  std::expected<int, Error> ReadAligned(int fd, std::span<char> buf,
                                        off_t offset) noexcept override;
  std::expected<int, Error> WriteAligned(int fd, std::span<const char> buf,
                                         off_t offset) noexcept override;

  std::future<std::expected<int, Error>>
  ReadAsync(int fd, std::span<char> buf, off_t offset) noexcept override;
  std::future<std::expected<int, Error>>
  WriteAsync(int fd, std::span<const char> buf, off_t offset) noexcept override;
};

} // namespace storage
} // namespace zujan

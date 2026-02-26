#include "wal.h"
#include "memtable.h"
#include <fcntl.h>
#include <unistd.h>
#include <vector>

namespace zujan {
namespace storage {

enum class EntryType : uint8_t { PUT = 0, DELETE = 1 };

WAL::WAL(IOContext &io_ctx, const std::string &filepath)
    : io_ctx_(io_ctx), filepath_(filepath) {
  fd_ = ::open(filepath_.c_str(), O_CREAT | O_RDWR | O_APPEND, 0644);
  if (fd_ >= 0) {
    // Get current size to append
    current_offset_ = ::lseek(fd_, 0, SEEK_END);
  }
}

WAL::~WAL() {
  if (fd_ >= 0) {
    ::close(fd_);
  }
}

std::expected<void, Error> WAL::AppendPut(const std::string &key,
                                          const std::string &value) noexcept {
  if (fd_ < 0)
    return std::unexpected(
        Error{ErrorCode::SystemError, "bad file descriptor"});

  std::vector<char> buffer;
  EntryType type = EntryType::PUT;
  uint32_t klen = key.size();
  uint32_t vlen = value.size();

  buffer.push_back(static_cast<char>(type));
  buffer.insert(buffer.end(), reinterpret_cast<char *>(&klen),
                reinterpret_cast<char *>(&klen) + 4);
  buffer.insert(buffer.end(), key.begin(), key.end());
  buffer.insert(buffer.end(), reinterpret_cast<char *>(&vlen),
                reinterpret_cast<char *>(&vlen) + 4);
  buffer.insert(buffer.end(), value.begin(), value.end());

  auto res = io_ctx_.WriteAligned(fd_, buffer, -1);
  if (!res)
    return std::unexpected(res.error());

  return {};
}

std::expected<void, Error> WAL::AppendDelete(const std::string &key) noexcept {
  if (fd_ < 0)
    return std::unexpected(
        Error{ErrorCode::SystemError, "bad file descriptor"});

  std::vector<char> buffer;
  EntryType type = EntryType::DELETE;
  uint32_t klen = key.size();

  buffer.push_back(static_cast<char>(type));
  buffer.insert(buffer.end(), reinterpret_cast<char *>(&klen),
                reinterpret_cast<char *>(&klen) + 4);
  buffer.insert(buffer.end(), key.begin(), key.end());

  auto res = io_ctx_.WriteAligned(fd_, buffer, -1);
  if (!res)
    return std::unexpected(res.error());

  return {};
}

std::expected<void, Error> WAL::Recover(MemTable &memtable) noexcept {
  if (fd_ < 0)
    return {};

  ::lseek(fd_, 0, SEEK_SET);

  while (true) {
    char type_char;
    ssize_t n = ::read(fd_, &type_char, 1);
    if (n <= 0)
      break;

    EntryType type = static_cast<EntryType>(type_char);
    uint32_t klen = 0;
    n = ::read(fd_, &klen, 4);
    if (n < 4)
      break;

    std::string key(klen, '\0');
    n = ::read(fd_, key.data(), klen);
    if (n < klen)
      break;

    if (type == EntryType::PUT) {
      uint32_t vlen = 0;
      n = ::read(fd_, &vlen, 4);
      if (n < 4)
        break;

      std::string value(vlen, '\0');
      n = ::read(fd_, value.data(), vlen);
      if (n < vlen)
        break;

      memtable.Put(key, value);
    } else if (type == EntryType::DELETE) {
      memtable.Delete(key);
    }
  }

  current_offset_ = ::lseek(fd_, 0, SEEK_END);
  return {};
}

std::expected<void, Error> WAL::Sync() noexcept {
  if (fd_ >= 0) {
    ::fdatasync(fd_);
  }
  return {};
}

} // namespace storage
} // namespace zujan

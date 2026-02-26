#pragma once

#include "status.h"
#include <expected>
#include <optional>
#include <string>

namespace zujan {
namespace storage {

// Abstract Key-Value Store interface for the storage engine
// Designed to support our custom LSM-Tree or other standard engines (e.g.,
// RocksDB)
class KVStore {
public:
  virtual ~KVStore() = default;

  virtual std::expected<void, Error> Put(const std::string &key,
                                         const std::string &value) noexcept = 0;
  virtual std::expected<std::optional<std::string>, Error>
  Get(const std::string &key) noexcept = 0;
  virtual std::expected<void, Error>
  Delete(const std::string &key) noexcept = 0;
};

} // namespace storage
} // namespace zujan

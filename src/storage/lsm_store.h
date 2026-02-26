#pragma once

#include "io.h"
#include "kv_store.h"
#include "memtable.h"
#include "sstable.h"
#include "wal.h"
#include <memory>

namespace zujan {
namespace storage {

class LSMStore : public KVStore {
public:
  static std::expected<std::unique_ptr<LSMStore>, Error> Open() noexcept;

  LSMStore();
  ~LSMStore() override;

  std::expected<void, Error> Init() noexcept;

  std::expected<void, Error> Put(const std::string &key,
                                 const std::string &value) noexcept override;
  std::expected<std::optional<std::string>, Error>
  Get(const std::string &key) noexcept override;
  std::expected<void, Error> Delete(const std::string &key) noexcept override;

private:
  std::unique_ptr<IOContext> io_ctx_;
  TableBuilderOptions table_options_;
  std::unique_ptr<MemTable> memtable_;
  std::unique_ptr<WAL> wal_;
  std::unique_ptr<SSTableManager> sst_manager_;
};

} // namespace storage
} // namespace zujan

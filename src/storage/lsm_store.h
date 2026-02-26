#pragma once

#include <memory>

#include "io.h"
#include "kv_store.h"
#include "memtable.h"
#include "sstable.h"
#include "wal.h"

namespace zujan
{
namespace storage
{

/**
 * @brief Log-Structured Merge-Tree (LSM-Tree) based key-value store implementation
 */
class LSMStore : public KVStore
{
public:
    /**
     * @brief Open or create an LSMStore at the specified directory
     * @param dir The directory path for the store
     * @return std::expected<std::unique_ptr<LSMStore>, Error> The opened store or error
     */
    static std::expected<std::unique_ptr<LSMStore>, Error> Open(const std::string &dir = ".") noexcept;

    explicit LSMStore(const std::string &dir);
    ~LSMStore() override;

    std::expected<void, Error> Init() noexcept;

    std::expected<void, Error> Put(const std::string &key, const std::string &value) noexcept override;
    std::expected<std::optional<std::string>, Error> Get(const std::string &key) noexcept override;
    std::expected<void, Error>                       Delete(const std::string &key) noexcept override;

private:
    std::string                     dir_;
    std::unique_ptr<IOContext>      io_ctx_;
    TableBuilderOptions             table_options_;
    std::unique_ptr<MemTable>       memtable_;
    std::unique_ptr<WAL>            wal_;
    std::unique_ptr<SSTableManager> sst_manager_;
};

}  // namespace storage
}  // namespace zujan

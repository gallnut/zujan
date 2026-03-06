#pragma once

#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <thread>

#include "io.h"
#include "kv_store.h"
#include "memtable.h"
#include "sstable.h"
#include "version_set.h"
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
    std::expected<std::optional<std::string>, Error> Get(const ReadOptions &options,
                                                         const std::string &key) noexcept override;
    std::expected<void, Error>                       Delete(const std::string &key) noexcept override;
    std::expected<void, Error> Write(const WriteOptions &options, WriteBatch *updates) noexcept override;

    const Snapshot *GetSnapshot() override;
    void            ReleaseSnapshot(const Snapshot *snapshot) override;

private:
    std::string                     dir_;
    std::unique_ptr<IOContext>      io_ctx_;
    TableBuilderOptions             table_options_;
    std::unique_ptr<MemTable>       memtable_;
    std::unique_ptr<MemTable>       imm_;  // Immutable memtable waiting for flush
    std::unique_ptr<WAL>            wal_;
    std::unique_ptr<SSTableManager> sst_manager_;

    // Background Compaction
    std::mutex              mutex_;
    std::condition_variable bg_cv_;
    std::thread             bg_thread_;
    std::atomic<bool>       stop_bg_{false};
    bool                    bg_compaction_scheduled_{false};

    size_t memtable_size_limit_ = 4 * 1024 * 1024;  // 4MB

    std::unique_ptr<VersionSet> versions_;
    SnapshotList                snapshots_;
    uint64_t                    logfile_number_ = 0;
    uint64_t                    imm_logfile_number_ = 0;

    void BGWork();
    void BackgroundCall();
    void CompactMemTable();
    void DoCompaction();
    void RecoverWAL(uint64_t log_number);
};

}  // namespace storage
}  // namespace zujan

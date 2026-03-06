#pragma once

#include <expected>
#include <map>
#include <memory>
#include <shared_mutex>
#include <string>
#include <vector>

#include "block.h"
#include "filter_block.h"
#include "io.h"
#include "options.h"
#include "table_builder.h"

namespace zujan
{
namespace storage
{

struct LookupResult
{
    bool        found = false;
    bool        deleted = false;
    std::string value;
};

class SSTable
{
public:
    /**
     * @brief Attempt to open the table that is stored in the file with the specified size.
     *
     * @param options The table builder options
     * @param io_ctx The IO context
     * @param filepath The path to the SSTable file
     * @return std::expected<std::unique_ptr<SSTable>, Error> The opened SSTable or error
     */
    static std::expected<std::unique_ptr<SSTable>, Error> Open(const TableBuilderOptions &options, IOContext &io_ctx,
                                                               const std::string &filepath);

    ~SSTable();

    /**
     * @brief Searches for a key within this SSTable
     *
     * @param options Read options
     * @param key The key to search for
     * @return std::expected<LookupResult, Error> Lookup result or error
     */
    std::expected<LookupResult, Error> Get(const ReadOptions &options, std::string_view key, uint64_t seq) noexcept;

    void DumpToMap(std::map<std::string, LookupResult> &out_map) const;

private:
    SSTable(IOContext &io_ctx, int fd, uint64_t file_size, const TableBuilderOptions &options);

    std::expected<void, Error>                   ReadFooterAndIndex();
    std::expected<std::unique_ptr<Block>, Error> ReadBlock(uint64_t offset, uint64_t size) const;

    IOContext          &io_ctx_;
    int                 fd_;
    uint64_t            file_size_;
    TableBuilderOptions options_;
    uint64_t            cache_id_;

    std::string                        filter_data_;
    std::unique_ptr<FilterBlockReader> filter_;
    std::unique_ptr<Block>             index_block_;

    // No copying
    SSTable(const SSTable &) = delete;
    void operator=(const SSTable &) = delete;
};

/**
 * @brief Manages levels, overlaps, and caching for SSTables
 */
class SSTableManager
{
public:
    explicit SSTableManager(IOContext &io_ctx, const TableBuilderOptions &opt);
    ~SSTableManager() = default;

    std::expected<LookupResult, Error> Get(const ReadOptions &options, std::string_view key, uint64_t seq) noexcept;

    void AddSSTable(int level, std::string filepath);

    // Compaction support
    std::vector<SSTable *> GetLevelSSTables(int level);
    void                   ReplaceLevelSSTables(int level, const std::vector<SSTable *> &old_tables,
                                                std::vector<std::string> new_filepaths);

private:
    mutable std::shared_mutex rw_lock_;
    IOContext                &io_ctx_;
    TableBuilderOptions       options_;

    // In a real LSM, we store metadata. Here we store loaded tables for
    // simplicity.
    std::vector<std::vector<std::unique_ptr<SSTable>>> levels_;
};

}  // namespace storage
}  // namespace zujan

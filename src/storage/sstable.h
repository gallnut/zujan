#pragma once

#include <expected>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "block.h"
#include "filter_block.h"
#include "io.h"
#include "table_builder.h"

namespace zujan
{
namespace storage
{

struct ReadOptions
{
    bool verify_checksums = false;
    bool fill_cache = true;
};

class SSTable
{
public:
    // Attempt to open the table that is stored in the file with the specified
    // size. We don't have file size easily accessible here without stat, so we
    // can require it or figure it out inside Open. For simplicity we let Open
    // figure it out or pass it in.
    static std::expected<std::unique_ptr<SSTable>, Error> Open(const TableBuilderOptions &options, IOContext &io_ctx,
                                                               const std::string &filepath);

    ~SSTable();

    // Searches for a key within this SSTable
    std::expected<std::optional<std::string>, Error> Get(const ReadOptions &options, std::string_view key) noexcept;

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

// Manages levels, overlaps, and caching for SSTables
class SSTableManager
{
public:
    explicit SSTableManager(IOContext &io_ctx, const TableBuilderOptions &opt);
    ~SSTableManager() = default;

    std::expected<std::optional<std::string>, Error> Get(const ReadOptions &options, std::string_view key) noexcept;

    void AddSSTable(int level, std::string filepath);

private:
    IOContext          &io_ctx_;
    TableBuilderOptions options_;

    // In a real LSM, we store metadata. Here we store loaded tables for
    // simplicity.
    std::vector<std::vector<std::unique_ptr<SSTable>>> levels_;
};

}  // namespace storage
}  // namespace zujan

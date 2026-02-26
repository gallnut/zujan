#pragma once

#include <cstdint>
#include <expected>
#include <memory>
#include <string_view>

#include "bloom_filter.h"
#include "cache.h"
#include "io.h"
#include "status.h"

namespace zujan
{
namespace storage
{

struct TableBuilderOptions
{
    std::shared_ptr<const BloomFilterPolicy> filter_policy{nullptr};
    std::shared_ptr<Cache>                   block_cache{nullptr};
    int                                      block_restart_interval = 16;
    size_t                                   block_size = 4096;
};

/**
 * @brief A TableBuilder is used to construct a Table (SSTable) file.
 */
class TableBuilder
{
public:
    /**
     * @brief Create a builder that will append to the given IOContext and file descriptor.
     *
     * @param options Table builder options
     * @param io_ctx IO context to use
     * @param fd File descriptor to write to
     */
    TableBuilder(const TableBuilderOptions &options, IOContext &io_ctx, int fd);
    ~TableBuilder();

    /**
     * @brief Change the options used by this builder.
     * REQUIRES: No keys have been added yet.
     *
     * @param options The new options
     */
    void ChangeOptions(const TableBuilderOptions &options);

    /**
     * @brief Add key,value to the table being constructed.
     * REQUIRES: key is after any previously added key
     *
     * @param key The key to add
     * @param value The value to add
     */
    void Add(std::string_view key, std::string_view value);

    /**
     * @brief Advanced operation: flush any buffered key/value pairs to file.
     * Can be used to ensure that two adjacent entries never live in
     * the same data block.
     */
    void Flush();

    /**
     * @brief Finish building the table. Stops using the file passed to the
     * constructor after this function returns.
     * REQUIRES: Finish(), Abandon() have not been called
     *
     * @return std::expected<void, Error> Success or error status
     */
    std::expected<void, Error> Finish();

    /**
     * @brief Number of calls to Add() so far.
     * @return uint64_t The number of entries
     */
    uint64_t NumEntries() const;

    /**
     * @brief Size of the file generated so far.
     * @return uint64_t The file size in bytes
     */
    uint64_t FileSize() const;

private:
    struct Rep;
    std::unique_ptr<Rep> rep_;

    // No copying allowed
    TableBuilder(const TableBuilder &);
    void operator=(const TableBuilder &);
};

}  // namespace storage
}  // namespace zujan

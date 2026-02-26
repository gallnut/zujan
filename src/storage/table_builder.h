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

// A TableBuilder is used to construct a Table (SSTable) file.
class TableBuilder
{
public:
    // Create a builder that will append to the given IOContext and file
    // descriptor.
    TableBuilder(const TableBuilderOptions &options, IOContext &io_ctx, int fd);
    ~TableBuilder();

    // Change the options used by this builder.
    // REQUIRES: No keys have been added yet.
    void ChangeOptions(const TableBuilderOptions &options);

    // Add key,value to the table being constructed.
    // REQUIRES: key is after any previously added key
    void Add(std::string_view key, std::string_view value);

    // Advanced operation: flush any buffered key/value pairs to file.
    // Can be used to ensure that two adjacent entries never live in
    // the same data block.
    void Flush();

    // Finish building the table. Stops using the file passed to the
    // constructor after this function returns.
    // REQUIRES: Finish(), Abandon() have not been called
    std::expected<void, Error> Finish();

    // Number of calls to Add() so far.
    uint64_t NumEntries() const;

    // Size of the file generated so far.
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

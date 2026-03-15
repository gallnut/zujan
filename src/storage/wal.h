#pragma once

#include <sys/types.h>

#include <expected>
#include <string>

#include "io.h"

namespace zujan
{
namespace storage
{

class MemTable;
class WriteBatch;

/**
 * @brief Write-Ahead Log for durability using system I/O
 */
class WAL
{
public:
    /**
     * @brief Construct a new WAL object
     * @param io_ctx IO context for disk operations
     * @param filepath Path to the WAL file
     */
    explicit WAL(IOContext &io_ctx, const std::string &filepath);
    ~WAL();

    /**
     * @brief Append a WriteBatch to the WAL
     * @param batch The batch to put
     * @return std::expected<void, Error> Success or error status
     */
    std::expected<void, Error> Append(const WriteBatch &batch) noexcept;

    /**
     * @brief Recovery routine to reconstruct MemTable on startup
     * @param memtable The memory table to recover into
     * @return std::expected<void, Error> Success or error status
     */
    std::expected<uint64_t, Error> Recover(MemTable &memtable) noexcept;

    /**
     * @brief Synchronize the WAL to disk
     * @return std::expected<void, Error> Success or error status
     */
    std::expected<void, Error> Sync() noexcept;

private:
    IOContext  &io_ctx_;
    std::string filepath_;
    int         fd_{-1};
    off_t       current_offset_{0};
};

}  // namespace storage
}  // namespace zujan

#pragma once

#include <cstdint>
#include <expected>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "storage/io.h"
#include "zujan.pb.h"

namespace zujan
{
namespace consensus
{

/**
 * @brief A dedicated persistent log for Raft entries, separate from the LSM WAL.
 * It stores zujan::proto::LogEntry items contiguously.
 */
struct LogMeta
{
    uint64_t                     offset;
    uint32_t                     length;
    uint64_t                     term;
    proto::LogEntry::CommandType type;
};
class RaftLog
{
public:
    static std::expected<std::unique_ptr<RaftLog>, storage::Error> Open(storage::IOContext &io_ctx,
                                                                        const std::string  &filepath);

    ~RaftLog();

    RaftLog(const RaftLog &) = delete;
    RaftLog &operator=(const RaftLog &) = delete;

    /**
     * @brief Append a single entry to the log.
     * Assumes the entry's index is strictly the next sequential index.
     * @param entry The entry to append
     * @return std::expected<void, storage::Error> Success or error status
     */
    std::expected<void, storage::Error> Append(const proto::LogEntry &entry);

    /**
     * @brief Append multiple entries.
     * @param entries The entries to append
     * @return std::expected<void, storage::Error> Success or error status
     */
    std::expected<void, storage::Error> Append(const std::vector<proto::LogEntry> &entries);

    /**
     * @brief Retrieve an entry at a given 1-based index (Raft index starts at 1)
     * @param index The index to retrieve
     * @return std::expected<proto::LogEntry, storage::Error> The retrieved entry or error status
     */
    std::expected<proto::LogEntry, storage::Error> Get(uint64_t index);

    /**
     * @brief Get the entries from [start_index, end_index) (exclusive end)
     * @param start_index The inclusive start index
     * @param end_index The exclusive end index
     * @return std::expected<std::vector<proto::LogEntry>, storage::Error> The entries or error status
     */
    std::expected<std::vector<proto::LogEntry>, storage::Error> GetRange(uint64_t start_index, uint64_t end_index);

    /**
     * @brief Truncate the log starting from `index` and delete all subsequent entries.
     * Useful when conflicting entries are found during AppendEntries.
     * @param index The index to truncate from
     * @return std::expected<void, storage::Error> Success or error status
     */
    std::expected<void, storage::Error> TruncateFrom(uint64_t index);

    /**
     * @brief Compact the log by discarding all entries BEFORE `up_to_index`.
     * Keeps the entry at `up_to_index` as the new base (often representing the snapshot).
     * @param up_to_index The index up to which to compact
     * @return std::expected<void, storage::Error> Success or error status
     */
    std::expected<void, storage::Error> Compact(uint64_t up_to_index);

    uint64_t FirstIndex() const;

    uint64_t LastIndex() const;
    uint64_t LastTerm() const;

private:
    RaftLog(storage::IOContext &io_ctx, int fd, const std::string &filepath);

    std::expected<void, storage::Error> Recover();

    storage::IOContext &io_ctx_;
    int                 fd_;
    std::string         filepath_;
    uint64_t            file_size_{0};

    // In-memory cache of log metadata. Since a Raft log typically gets snapshot
    // and compacted, keeping it in memory is standard (e.g. etcd/raft does this
    // up to limits). The first valid Raft index is 1. We keep a dummy entry at
    // index 0 for simplicity.
    std::vector<LogMeta> meta_;

    mutable std::mutex mutex_;
};

}  // namespace consensus
}  // namespace zujan

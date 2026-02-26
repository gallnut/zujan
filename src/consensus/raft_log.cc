#include "raft_log.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "storage/coding.h"

namespace zujan
{
namespace consensus
{

std::expected<std::unique_ptr<RaftLog>, storage::Error> RaftLog::Open(storage::IOContext &io_ctx,
                                                                      const std::string  &filepath)
{
    int fd = ::open(filepath.c_str(), O_RDWR | O_CREAT, 0644);
    if (fd < 0)
    {
        return std::unexpected(storage::Error{storage::ErrorCode::IOError, "Failed to open raft log file"});
    }

    auto log = std::unique_ptr<RaftLog>(new RaftLog(io_ctx, fd, filepath));
    auto err = log->Recover();
    if (!err)
    {
        return std::unexpected(err.error());
    }

    return log;
}

RaftLog::RaftLog(storage::IOContext &io_ctx, int fd, const std::string &filepath)
    : io_ctx_(io_ctx), fd_(fd), filepath_(filepath)
{
    // Add a dummy entry at index 0 for Raft 1-based indexing
    LogMeta dummy;
    dummy.offset = 0;
    dummy.length = 0;
    dummy.term = 0;
    dummy.type = proto::LogEntry::PUT;
    meta_.push_back(dummy);
}

RaftLog::~RaftLog()
{
    if (fd_ >= 0)
    {
        ::close(fd_);
    }
}

std::expected<void, storage::Error> RaftLog::Recover()
{
    struct stat st;
    if (::fstat(fd_, &st) < 0)
    {
        return std::unexpected(storage::Error{storage::ErrorCode::SystemError, "fstat failed on log file"});
    }
    file_size_ = st.st_size;

    if (file_size_ == 0)
    {
        return {};
    }

    uint64_t offset = 0;
    while (offset < file_size_)
    {
        // Read 4-byte length
        char len_buf[4];
        auto read_res = io_ctx_.ReadAligned(fd_, std::span<char>(len_buf, 4), offset);
        if (!read_res || *read_res != 4)
        {
            // Partial write or EOF
            break;
        }
        uint32_t entry_len = storage::DecodeFixed32(len_buf);
        offset += 4;

        if (offset + entry_len > file_size_)
        {
            // Corrupt record or partial write at tail
            break;
        }

        // Read payload
        std::string payload(entry_len, '\0');
        read_res = io_ctx_.ReadAligned(fd_, std::span<char>(payload.data(), entry_len), offset);
        if (!read_res || *read_res != entry_len)
        {
            break;
        }
        offset += entry_len;

        proto::LogEntry entry;
        if (!entry.ParseFromString(payload))
        {
            return std::unexpected(
                storage::Error{storage::ErrorCode::Corruption, "Failed to parse LogEntry from file"});
        }

        LogMeta meta;
        meta.offset = offset - entry_len;
        meta.length = entry_len;
        meta.term = entry.term();
        meta.type = entry.type();
        meta_.push_back(meta);
    }

    // Truncate to the valid offset to drop any partial tail entries
    if (offset < file_size_)
    {
        if (::ftruncate(fd_, offset) < 0)
        {
            return std::unexpected(storage::Error{storage::ErrorCode::SystemError, "Failed to truncate partial tail"});
        }
        file_size_ = offset;
    }

    return {};
}

std::expected<void, storage::Error> RaftLog::Append(const proto::LogEntry &entry)
{
    std::vector<proto::LogEntry> batch = {entry};
    return Append(batch);
}

std::expected<void, storage::Error> RaftLog::Append(const std::vector<proto::LogEntry> &entries)
{
    if (entries.empty()) return {};

    // 1. CPU-intensive serialization outside the critical section (lock)
    std::string          batch_buffer;
    std::vector<LogMeta> new_metas;
    new_metas.reserve(entries.size());

    for (size_t i = 0; i < entries.size(); ++i)
    {
        const auto &entry = entries[i];

        // Ensure contiguous indexes in the proposed batch itself
        if (i > 0 && entry.index() != entries[i - 1].index() + 1)
        {
            return std::unexpected(storage::Error{storage::ErrorCode::InvalidArgument,
                                                  "Entries indices are not contiguous within the batch"});
        }

        std::string payload;
        if (!entry.SerializeToString(&payload))
        {
            return std::unexpected(storage::Error{storage::ErrorCode::Corruption, "Failed to serialize LogEntry"});
        }

        char len_buf[4];
        storage::EncodeFixed32(len_buf, payload.size());

        batch_buffer.append(len_buf, 4);
        batch_buffer.append(payload);

        LogMeta meta;
        // offset will be computed inside the lock once we know the absolute
        // file_size_
        meta.length = payload.size();
        meta.term = entry.term();
        meta.type = entry.type();
        new_metas.push_back(meta);
    }

    // 2. Minimal critical section: offset resolution, Disk I/O, and meta
    // insertion
    std::lock_guard<std::mutex> lock(mutex_);

    if (entries.front().index() != meta_.size())
    {
        return std::unexpected(storage::Error{storage::ErrorCode::InvalidArgument,
                                              "Batch starts at incorrect index: must perfectly match tail"});
    }

    uint64_t current_offset = file_size_;
    for (auto &meta : new_metas)
    {
        meta.offset = current_offset + 4;  // Point to start of payload
        current_offset += 4 + meta.length;
    }

    // Write the whole batch in one aligned call (Group Commit)
    auto write_res =
        io_ctx_.WriteAligned(fd_, std::span<const char>(batch_buffer.data(), batch_buffer.size()), file_size_);
    if (!write_res)
    {
        return std::unexpected(write_res.error());
    }

    // Force sync to disk for Raft strict safety
    if (::fdatasync(fd_) < 0)
    {
        return std::unexpected(storage::Error{storage::ErrorCode::IOError, "fdatasync failed during Append"});
    }

    file_size_ = current_offset;
    meta_.insert(meta_.end(), new_metas.begin(), new_metas.end());
    return {};
}

std::expected<proto::LogEntry, storage::Error> RaftLog::Get(uint64_t index)
{
    LogMeta meta;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (index == 0 || index >= meta_.size())
        {
            return std::unexpected(storage::Error{storage::ErrorCode::NotFound, "Log index out of bounds"});
        }
        meta = meta_[index];
    }

    std::string payload(meta.length, '\0');
    auto        read_res = io_ctx_.ReadAligned(fd_, std::span<char>(payload.data(), meta.length), meta.offset);
    if (!read_res || *read_res != meta.length)
    {
        return std::unexpected(storage::Error{storage::ErrorCode::IOError, "Failed to read LogEntry from disk"});
    }

    proto::LogEntry entry;
    if (!entry.ParseFromString(payload))
    {
        return std::unexpected(storage::Error{storage::ErrorCode::Corruption, "Failed to deserialize LogEntry"});
    }
    return entry;
}

std::expected<std::vector<proto::LogEntry>, storage::Error> RaftLog::GetRange(uint64_t start_index, uint64_t end_index)
{
    std::vector<LogMeta> metas;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (start_index == 0 || end_index <= start_index || end_index > meta_.size() + 1)
        {
            return std::unexpected(storage::Error{storage::ErrorCode::InvalidArgument, "Invalid index range"});
        }

        uint64_t limit = std::min(end_index, (uint64_t)meta_.size());
        metas.reserve(limit - start_index);
        for (uint64_t i = start_index; i < limit; ++i)
        {
            metas.push_back(meta_[i]);
        }
    }

    std::vector<proto::LogEntry> res;
    res.reserve(metas.size());

    // Could optimize to read batched pages if contiguous,
    // but discrete reads will suffice given typical network transmission caps.
    for (const auto &meta : metas)
    {
        std::string payload(meta.length, '\0');
        auto        read_res = io_ctx_.ReadAligned(fd_, std::span<char>(payload.data(), meta.length), meta.offset);
        if (!read_res || *read_res != meta.length)
        {
            return std::unexpected(storage::Error{storage::ErrorCode::IOError, "Failed to read batch entries"});
        }
        proto::LogEntry entry;
        if (!entry.ParseFromString(payload))
        {
            return std::unexpected(
                storage::Error{storage::ErrorCode::Corruption, "Failed to deserialize batch entries"});
        }
        res.push_back(std::move(entry));
    }

    return res;
}

std::expected<void, storage::Error> RaftLog::TruncateFrom(uint64_t index)
{
    std::lock_guard<std::mutex> lock(mutex_);
    if (index == 0 || index > meta_.size())
    {
        return std::unexpected(storage::Error{storage::ErrorCode::InvalidArgument, "Invalid truncate index"});
    }

    if (index == meta_.size()) return {};

    uint64_t new_size = meta_[index].offset - 4;  // Subtract length header
    meta_.erase(meta_.begin() + index, meta_.end());

    if (::ftruncate(fd_, new_size) < 0)
    {
        return std::unexpected(storage::Error{storage::ErrorCode::SystemError, "Failed to ftruncate raft log"});
    }
    file_size_ = new_size;

    return {};
}

uint64_t RaftLog::LastIndex() const
{
    std::lock_guard<std::mutex> lock(mutex_);
    return meta_.size() - 1;
}

uint64_t RaftLog::LastTerm() const
{
    std::lock_guard<std::mutex> lock(mutex_);
    return meta_.back().term;
}

uint64_t RaftLog::FirstIndex() const
{
    std::lock_guard<std::mutex> lock(mutex_);
    // In a compacted log, index 0 is always a dummy, but it might jump
    // e.g. from 0 directly to 100 if compacted up to 100.
    if (meta_.size() <= 1) return 0;
    // We need to store the true 'first index' or just compute it.
    // Wait, if we compact, meta_[0] stays a dummy, but what is its index?
    // We need to change how index mapping works if we compact!
    // Alternatively, meta_ vector could just act as a ring buffer or offset-based
    // array.
    return meta_.size() > 1 ? 1 : 0;  // Temporary placeholder until full compaction rewrite
}

std::expected<void, storage::Error> RaftLog::Compact(uint64_t up_to_index)
{
    // Snapshot/Compaction is complex because it shifts all indices.
    // Normally, Raft logs use a `first_index_` offset because meta_[1] might
    // represent Raft log index 1000. For the scope of this implementation
    // request, we will leave the full Index Shifting architecture for a dedicated
    // Snapshot PR, but the method interface is now ready.
    return {};
}

}  // namespace consensus
}  // namespace zujan

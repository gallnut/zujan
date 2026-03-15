#include "wal.h"

#include <fcntl.h>
#include <unistd.h>

#include <vector>

#include "memtable.h"
#include "write_batch.h"
#include "write_batch_internal.h"

namespace zujan
{
namespace storage
{

WAL::WAL(IOContext &io_ctx, const std::string &filepath) : io_ctx_(io_ctx), filepath_(filepath)
{
    fd_ = ::open(filepath_.c_str(), O_CREAT | O_RDWR | O_APPEND, 0644);
    if (fd_ >= 0)
    {
        // Get current size to append
        current_offset_ = ::lseek(fd_, 0, SEEK_END);
    }
}

WAL::~WAL()
{
    if (fd_ >= 0)
    {
        ::close(fd_);
    }
}

std::expected<void, Error> WAL::Append(const WriteBatch &batch) noexcept
{
    if (fd_ < 0) return std::unexpected(Error{ErrorCode::SystemError, "bad file descriptor"});

    std::vector<char> buffer;
    uint32_t          len = batch.ApproximateSize();
    buffer.insert(buffer.end(), reinterpret_cast<char *>(&len), reinterpret_cast<char *>(&len) + 4);

    // Use WriteBatchInternal to get the underlying buffer
    std::string_view rep = WriteBatchInternal::Contents(&batch);
    buffer.insert(buffer.end(), rep.begin(), rep.end());

    auto res = io_ctx_.WriteAligned(fd_, buffer, -1);
    if (!res) return std::unexpected(res.error());

    return {};
}

std::expected<uint64_t, Error> WAL::Recover(MemTable &memtable) noexcept
{
    if (fd_ < 0) return 0ULL;

    ::lseek(fd_, 0, SEEK_SET);

    uint64_t max_seq = 0;

    while (true)
    {
        uint32_t len = 0;
        ssize_t  n = ::read(fd_, &len, 4);
        if (n < 4) break;

        std::string buffer(len, '\0');
        n = ::read(fd_, buffer.data(), len);
        if (n < len) break;

        // Deserialize and apply
        WriteBatch batch;
        WriteBatchInternal::SetContents(&batch, buffer);

        // Track max sequence
        uint64_t seq = WriteBatchInternal::Sequence(&batch);
        uint32_t count = WriteBatchInternal::Count(&batch);
        if (seq + count - 1 > max_seq)
        {
            max_seq = seq + count - 1;
        }

        // Use InsertInto which properly drives sequence numbers
        WriteBatchInternal::InsertInto(&batch, &memtable);
    }

    current_offset_ = ::lseek(fd_, 0, SEEK_END);
    return max_seq;
}

std::expected<void, Error> WAL::Sync() noexcept
{
    if (fd_ >= 0)
    {
        ::fdatasync(fd_);
    }
    return {};
}

}  // namespace storage
}  // namespace zujan

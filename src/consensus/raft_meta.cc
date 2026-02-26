#include "raft_meta.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

namespace zujan
{
namespace consensus
{

// The disk format is just 16 bytes:
// [8 bytes: current_term]
// [8 bytes: voted_for]
struct MetaPayload
{
    uint64_t current_term;
    uint64_t voted_for;
};

std::expected<std::unique_ptr<RaftMeta>, storage::Error> RaftMeta::Open(storage::IOContext &io_ctx,
                                                                        const std::string  &filepath)
{
    int fd = ::open(filepath.c_str(), O_RDWR | O_CREAT, 0644);
    if (fd < 0)
    {
        return std::unexpected(storage::Error{storage::ErrorCode::IOError, "Failed to open raft meta file"});
    }

    auto meta = std::unique_ptr<RaftMeta>(new RaftMeta(io_ctx, fd, filepath));
    auto err = meta->Recover();
    if (!err)
    {
        return std::unexpected(err.error());
    }

    return meta;
}

RaftMeta::RaftMeta(storage::IOContext &io_ctx, int fd, const std::string &filepath)
    : io_ctx_(io_ctx), fd_(fd), filepath_(filepath)
{
}

RaftMeta::~RaftMeta()
{
    if (fd_ >= 0)
    {
        ::close(fd_);
    }
}

std::expected<void, storage::Error> RaftMeta::Recover()
{
    struct stat st;
    if (::fstat(fd_, &st) < 0)
    {
        return std::unexpected(storage::Error{storage::ErrorCode::SystemError, "fstat failed on meta file"});
    }

    if (st.st_size == 0)
    {
        return {};  // Blank file, defaults to 0
    }

    if (st.st_size != sizeof(MetaPayload))
    {
        return std::unexpected(storage::Error{storage::ErrorCode::Corruption, "Corrupted meta file size"});
    }

    MetaPayload payload;
    auto        read_res =
        io_ctx_.ReadAligned(fd_, std::span<char>(reinterpret_cast<char *>(&payload), sizeof(MetaPayload)), 0);
    if (!read_res || *read_res != sizeof(MetaPayload))
    {
        return std::unexpected(storage::Error{storage::ErrorCode::IOError, "Failed to read from meta file"});
    }

    current_term_ = payload.current_term;
    voted_for_ = payload.voted_for;
    return {};
}

std::expected<void, storage::Error> RaftMeta::Save(uint64_t current_term, uint64_t voted_for)
{
    MetaPayload payload{current_term, voted_for};

    // Overwrite at offset 0
    auto write_res = io_ctx_.WriteAligned(
        fd_, std::span<const char>(reinterpret_cast<const char *>(&payload), sizeof(MetaPayload)), 0);

    if (!write_res)
    {
        return std::unexpected(write_res.error());
    }

    // Strict persistence to avoid split-brain
    if (::fdatasync(fd_) < 0)
    {
        return std::unexpected(storage::Error{storage::ErrorCode::IOError, "fdatasync failed during RaftMeta Save"});
    }

    current_term_ = current_term;
    voted_for_ = voted_for;
    return {};
}

}  // namespace consensus
}  // namespace zujan

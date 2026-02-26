#include "posix_io.h"

#include <unistd.h>

namespace zujan
{
namespace storage
{

std::expected<void, Error> PosixIOContext::Init() noexcept
{
    // No special initialization required for POSIX pwrite/pread
    return {};
}

std::expected<int, Error> PosixIOContext::ReadAligned(int fd, std::span<char> buf, off_t offset) noexcept
{
    ssize_t ret = ::pread(fd, buf.data(), buf.size(), offset);
    if (ret < 0)
    {
        return std::unexpected(Error{ErrorCode::SystemError, "pread failed"});
    }
    return ret;
}

std::expected<int, Error> PosixIOContext::WriteAligned(int fd, std::span<const char> buf, off_t offset) noexcept
{
    ssize_t ret = ::pwrite(fd, buf.data(), buf.size(), offset);
    if (ret < 0)
    {
        return std::unexpected(Error{ErrorCode::SystemError, "pwrite failed"});
    }
    return ret;
}

std::future<std::expected<int, Error>> PosixIOContext::ReadAsync(int fd, std::span<char> buf, off_t offset) noexcept
{
    return std::async(std::launch::async, [this, fd, buf, offset]() { return ReadAligned(fd, buf, offset); });
}

std::future<std::expected<int, Error>> PosixIOContext::WriteAsync(int fd, std::span<const char> buf,
                                                                  off_t offset) noexcept
{
    return std::async(std::launch::async, [this, fd, buf, offset]() { return WriteAligned(fd, buf, offset); });
}

}  // namespace storage
}  // namespace zujan

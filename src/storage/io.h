#pragma once

#include <expected>
#include <future>
#include <span>

#include "status.h"

namespace zujan
{
namespace storage
{

class IOContext
{
public:
    virtual ~IOContext() = default;

    virtual std::expected<void, Error> Init() noexcept = 0;

    virtual std::expected<int, Error> ReadAligned(int fd, std::span<char> buf, off_t offset) noexcept = 0;
    virtual std::expected<int, Error> WriteAligned(int fd, std::span<const char> buf, off_t offset) noexcept = 0;

    virtual std::future<std::expected<int, Error>> ReadAsync(int fd, std::span<char> buf, off_t offset) noexcept = 0;
    virtual std::future<std::expected<int, Error>> WriteAsync(int fd, std::span<const char> buf,
                                                              off_t offset) noexcept = 0;
};

}  // namespace storage
}  // namespace zujan

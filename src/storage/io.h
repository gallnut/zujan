#pragma once

#include <expected>
#include <future>
#include <span>

#include "status.h"

namespace zujan
{
namespace storage
{

/**
 * @brief The IOContext interface abstracting underlying OS I/O operations
 */
class IOContext
{
public:
    virtual ~IOContext() = default;

    /**
     * @brief Initialize the IO context
     * @return std::expected<void, Error> Success or error status
     */
    virtual std::expected<void, Error> Init() noexcept = 0;

    /**
     * @brief Perform an aligned read synchronously
     * @param fd File descriptor
     * @param buf Buffer to read into
     * @param offset Offset in the file
     * @return std::expected<int, Error> Number of bytes read or error status
     */
    virtual std::expected<int, Error> ReadAligned(int fd, std::span<char> buf, off_t offset) noexcept = 0;

    /**
     * @brief Perform an aligned write synchronously
     * @param fd File descriptor
     * @param buf Buffer containing data to write
     * @param offset Offset in the file
     * @return std::expected<int, Error> Number of bytes written or error status
     */
    virtual std::expected<int, Error> WriteAligned(int fd, std::span<const char> buf, off_t offset) noexcept = 0;

    /**
     * @brief Perform an asynchronous read
     * @param fd File descriptor
     * @param buf Buffer to read into
     * @param offset Offset in the file
     * @return std::future<std::expected<int, Error>> Future resolving to number of bytes read or error
     */
    virtual std::future<std::expected<int, Error>> ReadAsync(int fd, std::span<char> buf, off_t offset) noexcept = 0;

    /**
     * @brief Perform an asynchronous write
     * @param fd File descriptor
     * @param buf Buffer containing data to write
     * @param offset Offset in the file
     * @return std::future<std::expected<int, Error>> Future resolving to number of bytes written or error
     */
    virtual std::future<std::expected<int, Error>> WriteAsync(int fd, std::span<const char> buf,
                                                              off_t offset) noexcept = 0;
};

}  // namespace storage
}  // namespace zujan

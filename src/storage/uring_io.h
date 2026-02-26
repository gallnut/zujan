#pragma once

#include <liburing.h>

#include <atomic>
#include <mutex>
#include <span>
#include <thread>

#include "io.h"

namespace zujan
{
namespace storage
{

class URingIOContext : public IOContext
{
public:
    URingIOContext(unsigned entries = 256);
    ~URingIOContext() override;

    URingIOContext(const URingIOContext &) = delete;
    URingIOContext &operator=(const URingIOContext &) = delete;

    std::expected<void, Error> Init() noexcept override;

    // Core operations wrapper
    std::expected<int, Error> ReadAligned(int fd, std::span<char> buf, off_t offset) noexcept override;
    std::expected<int, Error> WriteAligned(int fd, std::span<const char> buf, off_t offset) noexcept override;

    std::future<std::expected<int, Error>> ReadAsync(int fd, std::span<char> buf, off_t offset) noexcept override;
    std::future<std::expected<int, Error>> WriteAsync(int fd, std::span<const char> buf,
                                                      off_t offset) noexcept override;

private:
    void PollCQE();

    struct io_uring   ring_;
    std::mutex        mutex_;  // Basic thread safety for the ring
    bool              initialized_{false};
    unsigned          entries_;
    std::thread       poller_;
    std::atomic<bool> stop_poller_{false};
};

}  // namespace storage
}  // namespace zujan

#include "uring_io.h"
#include <unistd.h>

namespace zujan {
namespace storage {

URingIOContext::URingIOContext(unsigned entries) : entries_(entries) {}

URingIOContext::~URingIOContext() {
  if (initialized_) {
    stop_poller_ = true;
    if (poller_.joinable()) {
      poller_.join();
    }
    io_uring_queue_exit(&ring_);
  }
}

std::expected<void, Error> URingIOContext::Init() noexcept {
  int ret = io_uring_queue_init(entries_, &ring_, 0);
  if (ret < 0) {
    return std::unexpected(
        Error{ErrorCode::SystemError, "io_uring_queue_init failed"});
  }
  initialized_ = true;
  poller_ = std::thread(&URingIOContext::PollCQE, this);
  return {};
}

void URingIOContext::PollCQE() {
  while (!stop_poller_) {
    struct io_uring_cqe *cqe;
    struct __kernel_timespec ts;
    ts.tv_sec = 0;
    ts.tv_nsec = 1000 * 1000 * 5; // 5ms timeout to allow clean shutdown

    int ret = io_uring_wait_cqe_timeout(&ring_, &cqe, &ts);
    if (ret == 0 && cqe) {
      if (cqe->user_data) {
        auto *promise =
            reinterpret_cast<std::promise<std::expected<int, Error>> *>(
                cqe->user_data);
        if (cqe->res < 0) {
          promise->set_value(std::unexpected(
              Error{ErrorCode::IOError, "Async io_uring operation failed"}));
        } else {
          promise->set_value(cqe->res);
        }
        delete promise;
      }
      io_uring_cqe_seen(&ring_, cqe);
    }
  }
}

std::future<std::expected<int, Error>>
URingIOContext::ReadAsync(int fd, std::span<char> buf, off_t offset) noexcept {
  auto *promise = new std::promise<std::expected<int, Error>>();
  auto future = promise->get_future();

  std::lock_guard<std::mutex> lock(mutex_);
  struct io_uring_sqe *sqe = io_uring_get_sqe(&ring_);
  if (!sqe) {
    promise->set_value(
        std::unexpected(Error{ErrorCode::IOError, "io_uring_get_sqe failed"}));
    delete promise;
    return future;
  }

  io_uring_prep_read(sqe, fd, buf.data(), buf.size(), offset);
  io_uring_sqe_set_data(sqe, promise);
  io_uring_submit(&ring_);
  return future;
}

std::future<std::expected<int, Error>>
URingIOContext::WriteAsync(int fd, std::span<const char> buf,
                           off_t offset) noexcept {
  auto *promise = new std::promise<std::expected<int, Error>>();
  auto future = promise->get_future();

  std::lock_guard<std::mutex> lock(mutex_);
  struct io_uring_sqe *sqe = io_uring_get_sqe(&ring_);
  if (!sqe) {
    promise->set_value(
        std::unexpected(Error{ErrorCode::IOError, "io_uring_get_sqe failed"}));
    delete promise;
    return future;
  }

  io_uring_prep_write(sqe, fd, buf.data(), buf.size(), offset);
  io_uring_sqe_set_data(sqe, promise);
  io_uring_submit(&ring_);
  return future;
}

std::expected<int, Error> URingIOContext::ReadAligned(int fd,
                                                      std::span<char> buf,
                                                      off_t offset) noexcept {
  return ReadAsync(fd, buf, offset).get();
}

std::expected<int, Error>
URingIOContext::WriteAligned(int fd, std::span<const char> buf,
                             off_t offset) noexcept {
  return WriteAsync(fd, buf, offset).get();
}

} // namespace storage
} // namespace zujan

#pragma once

#include <condition_variable>
#include <mutex>
#include <optional>
#include <queue>

namespace zujan
{
namespace consensus
{

/**
 * @brief A thread-safe queue implementation using a mutex and condition variable
 * @tparam T The type of items stored in the queue
 */
template <typename T>
class ThreadSafeQueue
{
public:
    /**
     * @brief Push an item into the queue and notify waiting threads
     * @param item The item to push
     */
    void Push(T item)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        queue_.push(std::move(item));
        cv_.notify_one();
    }

    /**
     * @brief Pop an item from the queue, blocking until one is available or the queue is stopped
     * @return std::optional<T> The popped item, or std::nullopt if stopped
     */
    std::optional<T> Pop()
    {
        std::unique_lock<std::mutex> lock(mutex_);
        cv_.wait(lock, [this]() { return !queue_.empty() || stop_; });
        if (stop_ && queue_.empty())
        {
            return std::nullopt;
        }
        T item = std::move(queue_.front());
        queue_.pop();
        return item;
    }

    /**
     * @brief Stop the queue, waking up any blocked Pop calls
     */
    void Stop()
    {
        std::lock_guard<std::mutex> lock(mutex_);
        stop_ = true;
        cv_.notify_all();
    }

private:
    std::queue<T>           queue_;
    std::mutex              mutex_;
    std::condition_variable cv_;
    bool                    stop_{false};
};

}  // namespace consensus
}  // namespace zujan

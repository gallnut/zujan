#pragma once

#include <condition_variable>
#include <mutex>
#include <optional>
#include <queue>

namespace zujan
{
namespace consensus
{

template <typename T>
class ThreadSafeQueue
{
public:
    void Push(T item)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        queue_.push(std::move(item));
        cv_.notify_one();
    }

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

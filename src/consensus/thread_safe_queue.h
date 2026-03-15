#pragma once

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <optional>

namespace zujan
{
namespace consensus
{

/**
 * @brief A true MPSC (Multi-Producer Single-Consumer) Lock-Free queue
 * @tparam T The type of items stored in the queue
 */
template <typename T>
class ThreadSafeQueue
{
private:
    struct Node
    {
        T                  data;
        std::atomic<Node*> next{nullptr};

        Node() = default;
        Node(T val) : data(std::move(val)) {}
    };

    alignas(64) std::atomic<Node*> head_;
    alignas(64) Node*              tail_;

    std::mutex              cv_mutex_;
    std::condition_variable cv_;
    std::atomic<bool>       stop_{false};
    std::atomic<bool>       waiting_{false};

public:
    ThreadSafeQueue()
    {
        Node* dummy = new Node();
        head_.store(dummy, std::memory_order_relaxed);
        tail_ = dummy;
    }

    ~ThreadSafeQueue()
    {
        while (Pop().has_value()) {}
        delete tail_;
    }

    /**
     * @brief Wait-Free Push an item into the queue
     * @param item The item to push
     */
    void Push(T item)
    {
        Node* node = new Node(std::move(item));
        Node* prev = head_.exchange(node, std::memory_order_acq_rel);
        prev->next.store(node, std::memory_order_release);

        // Slow path: awaken the consumer if it is sleeping
        if (waiting_.load(std::memory_order_acquire))
        {
            std::lock_guard<std::mutex> lock(cv_mutex_);
            cv_.notify_one();
        }
    }

    /**
     * @brief Pop an item from the queue
     * @return std::optional<T> The popped item, or std::nullopt if stopped
     */
    std::optional<T> Pop()
    {
        while (true)
        {
            Node* tail = tail_;
            Node* next = tail->next.load(std::memory_order_acquire);

            if (next != nullptr)
            {
                T data = std::move(next->data);
                tail_ = next;
                delete tail;  // safely release the old dummy
                return data;
            }

            if (stop_.load(std::memory_order_acquire))
            {
                return std::nullopt;
            }

            // Enter slow path
            waiting_.store(true, std::memory_order_release);
            // Double check to avoid race condition where producer pushed before waiting_ was true
            next = tail->next.load(std::memory_order_acquire);
            if (next != nullptr)
            {
                waiting_.store(false, std::memory_order_release);
                continue;
            }

            {
                std::unique_lock<std::mutex> lock(cv_mutex_);
                cv_.wait(lock, [this, tail]() {
                    return tail->next.load(std::memory_order_acquire) != nullptr || stop_.load(std::memory_order_acquire);
                });
            }
            waiting_.store(false, std::memory_order_release);
        }
    }

    /**
     * @brief Stop the queue, waking up any blocked Pop calls
     */
    void Stop()
    {
        stop_.store(true, std::memory_order_release);
        std::lock_guard<std::mutex> lock(cv_mutex_);
        cv_.notify_all();
    }

    /**
     * @brief Checks if the queue is temporarily empty.
     */
    bool IsEmpty() const
    {
        return tail_->next.load(std::memory_order_acquire) == nullptr;
    }
};

}  // namespace consensus
}  // namespace zujan

#pragma once

#include <atomic>
#include <cassert>

#include "arena.h"

namespace zujan
{
namespace storage
{

template <typename Key, class Comparator>
class SkipList
{
private:
    struct Node;

public:
    /**
     * @brief Create a new SkipList object that will use "cmp" for comparing keys,
     * and will allocate memory using "*arena". Objects allocated in the arena
     * must remain allocated for the lifetime of the skiplist object.
     *
     * @param cmp Comparator to use for keys
     * @param arena Arena for memory allocation
     */
    explicit SkipList(Comparator cmp, Arena *arena);

    SkipList(const SkipList &) = delete;
    SkipList &operator=(const SkipList &) = delete;

    /**
     * @brief Insert key into the list.
     * REQUIRES: nothing that compares equal to key is currently in the list.
     *
     * @param key The key to insert
     */
    void Insert(const Key &key);

    /**
     * @brief Returns true if an entry that compares equal to key is in the list.
     *
     * @param key The key to check for
     * @return bool True if key is present, false otherwise
     */
    bool Contains(const Key &key) const;

    /**
     * @brief Iteration over the contents of a skip list
     */
    class Iterator
    {
    public:
        explicit Iterator(const SkipList *list);

        bool       Valid() const;
        const Key &key() const;
        void       Next();
        void       Prev();
        void       Seek(const Key &target);
        void       SeekToFirst();
        void       SeekToLast();

    private:
        const SkipList *list_;
        Node           *node_;
    };

private:
    enum
    {
        kMaxHeight = 12
    };

    inline int GetMaxHeight() const { return max_height_.load(std::memory_order_relaxed); }

    Node *NewNode(const Key &key, int height);
    int   RandomHeight();
    bool  Equal(const Key &a, const Key &b) const { return (compare_(a, b) == 0); }
    bool  KeyIsAfterNode(const Key &key, Node *n) const;
    Node *FindGreaterOrEqual(const Key &key, Node **prev) const;
    Node *FindLessThan(const Key &key) const;
    Node *FindLast() const;

    Comparator const compare_;
    Arena *const     arena_;
    Node *const      head_;
    std::atomic<int> max_height_;  // Height of the entire list
    uint32_t         rnd_;         // simple random seed
};

// Implementation details follow
template <typename Key, class Comparator>
struct SkipList<Key, Comparator>::Node
{
    explicit Node(const Key &k) : key(k) {}

    Key const key;

    // Accessors/mutators for links. Wrapped in methods so we can
    // add the appropriate barriers as necessary.
    Node *Next(int n)
    {
        assert(n >= 0);
        // Use an 'acquire load' so that we observe a fully initialized
        // version of the returned Node.
        return next_[n].load(std::memory_order_acquire);
    }
    void SetNext(int n, Node *x)
    {
        assert(n >= 0);
        // Use a 'release store' so that anybody who reads through this
        // pointer observes a fully initialized version of the inserted node.
        next_[n].store(x, std::memory_order_release);
    }

    // No-barrier variants that can be safely used in a few locations.
    Node *NoBarrier_Next(int n)
    {
        assert(n >= 0);
        return next_[n].load(std::memory_order_relaxed);
    }
    void NoBarrier_SetNext(int n, Node *x)
    {
        assert(n >= 0);
        next_[n].store(x, std::memory_order_relaxed);
    }

private:
    // Array of length equal to the node height. next_[0] is lowest level link.
    std::atomic<Node *> next_[1];
};

template <typename Key, class Comparator>
typename SkipList<Key, Comparator>::Node *SkipList<Key, Comparator>::NewNode(const Key &key, int height)
{
    char *node_memory = arena_->AllocateAligned(sizeof(Node) + sizeof(std::atomic<Node *>) * (height - 1));
    return new (node_memory) Node(key);
}

template <typename Key, class Comparator>
inline SkipList<Key, Comparator>::Iterator::Iterator(const SkipList *list)
{
    list_ = list;
    node_ = nullptr;
}

template <typename Key, class Comparator>
inline bool SkipList<Key, Comparator>::Iterator::Valid() const
{
    return node_ != nullptr;
}

template <typename Key, class Comparator>
inline const Key &SkipList<Key, Comparator>::Iterator::key() const
{
    assert(Valid());
    return node_->key;
}

template <typename Key, class Comparator>
inline void SkipList<Key, Comparator>::Iterator::Next()
{
    assert(Valid());
    node_ = node_->Next(0);
}

template <typename Key, class Comparator>
inline void SkipList<Key, Comparator>::Iterator::Prev()
{
    assert(Valid());
    node_ = list_->FindLessThan(node_->key);
    if (node_ == list_->head_)
    {
        node_ = nullptr;
    }
}

template <typename Key, class Comparator>
inline void SkipList<Key, Comparator>::Iterator::Seek(const Key &target)
{
    node_ = list_->FindGreaterOrEqual(target, nullptr);
}

template <typename Key, class Comparator>
inline void SkipList<Key, Comparator>::Iterator::SeekToFirst()
{
    node_ = list_->head_->Next(0);
}

template <typename Key, class Comparator>
inline void SkipList<Key, Comparator>::Iterator::SeekToLast()
{
    node_ = list_->FindLast();
    if (node_ == list_->head_)
    {
        node_ = nullptr;
    }
}

template <typename Key, class Comparator>
int SkipList<Key, Comparator>::RandomHeight()
{
    // simple LCG
    rnd_ = rnd_ * 1664525 + 1013904223;
    int      height = 1;
    uint32_t val = rnd_;
    while (height < kMaxHeight && ((val & 3) == 0))
    {
        height++;
        val >>= 2;
    }
    return height;
}

template <typename Key, class Comparator>
bool SkipList<Key, Comparator>::KeyIsAfterNode(const Key &key, Node *n) const
{
    return (n != nullptr) && (compare_(n->key, key) < 0);
}

template <typename Key, class Comparator>
typename SkipList<Key, Comparator>::Node *SkipList<Key, Comparator>::FindGreaterOrEqual(const Key &key,
                                                                                        Node     **prev) const
{
    Node *x = head_;
    int   level = GetMaxHeight() - 1;
    while (true)
    {
        Node *next = x->Next(level);
        if (KeyIsAfterNode(key, next))
        {
            x = next;
        }
        else
        {
            if (prev != nullptr) prev[level] = x;
            if (level == 0)
            {
                return next;
            }
            else
            {
                level--;
            }
        }
    }
}

template <typename Key, class Comparator>
typename SkipList<Key, Comparator>::Node *SkipList<Key, Comparator>::FindLessThan(const Key &key) const
{
    Node *x = head_;
    int   level = GetMaxHeight() - 1;
    while (true)
    {
        assert(x == head_ || compare_(x->key, key) < 0);
        Node *next = x->Next(level);
        if (next == nullptr || compare_(next->key, key) >= 0)
        {
            if (level == 0)
            {
                return x;
            }
            else
            {
                level--;
            }
        }
        else
        {
            x = next;
        }
    }
}

template <typename Key, class Comparator>
typename SkipList<Key, Comparator>::Node *SkipList<Key, Comparator>::FindLast() const
{
    Node *x = head_;
    int   level = GetMaxHeight() - 1;
    while (true)
    {
        Node *next = x->Next(level);
        if (next == nullptr)
        {
            if (level == 0)
            {
                return x;
            }
            else
            {
                level--;
            }
        }
        else
        {
            x = next;
        }
    }
}

template <typename Key, class Comparator>
SkipList<Key, Comparator>::SkipList(Comparator cmp, Arena *arena)
    : compare_(cmp),
      arena_(arena),
      head_(NewNode(0 /* any key will do */, kMaxHeight)),
      max_height_(1),
      rnd_(0xdeadbeef)
{
    for (int i = 0; i < kMaxHeight; i++)
    {
        head_->NoBarrier_SetNext(i, nullptr);
    }
}

template <typename Key, class Comparator>
void SkipList<Key, Comparator>::Insert(const Key &key)
{
    Node *prev[kMaxHeight];
    Node *x = FindGreaterOrEqual(key, prev);

    assert(x == nullptr || !Equal(key, x->key));

    int height = RandomHeight();
    if (height > GetMaxHeight())
    {
        for (int i = GetMaxHeight(); i < height; i++)
        {
            prev[i] = head_;
        }
        max_height_.store(height, std::memory_order_relaxed);
    }

    x = NewNode(key, height);
    for (int i = 0; i < height; i++)
    {
        x->NoBarrier_SetNext(i, prev[i]->NoBarrier_Next(i));
        prev[i]->SetNext(i, x);
    }
}

template <typename Key, class Comparator>
bool SkipList<Key, Comparator>::Contains(const Key &key) const
{
    Node *x = FindGreaterOrEqual(key, nullptr);
    if (x != nullptr && Equal(key, x->key))
    {
        return true;
    }
    else
    {
        return false;
    }
}

}  // namespace storage
}  // namespace zujan

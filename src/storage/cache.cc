#include "cache.h"

#include <cassert>
#include <cstdlib>
#include <cstring>
#include <mutex>

#include "hash.h"

namespace zujan
{
namespace storage
{

Cache::~Cache() {}

namespace
{

// LRU cache implementation
// - in-use list: items currently referenced by clients.
// - LRU list: items not currently referenced, in LRU order.
struct LRUHandle
{
    void *value;
    void (*deleter)(std::string_view, void *value);
    LRUHandle *next_hash;
    LRUHandle *next;
    LRUHandle *prev;
    size_t     charge;  // TODO(opt): Only allow uint32_t?
    size_t     key_length;
    bool       in_cache;     // Whether entry is in the cache.
    uint32_t   refs;         // References, including cache reference, if present.
    uint32_t   hash;         // Hash of key(); used for fast sharding and comparisons
    char       key_data[1];  // Beginning of key

    std::string_view key() const { return std::string_view(key_data, key_length); }
};

// A simple, fast custom hash table for cache handles
class HandleTable
{
public:
    HandleTable() : length_(0), elems_(0), list_(nullptr) { Resize(); }
    ~HandleTable() { delete[] list_; }

    LRUHandle *Lookup(std::string_view key, uint32_t hash) { return *FindPointer(key, hash); }

    LRUHandle *Insert(LRUHandle *h)
    {
        LRUHandle **ptr = FindPointer(h->key(), h->hash);
        LRUHandle  *old = *ptr;
        h->next_hash = (old == nullptr ? nullptr : old->next_hash);
        *ptr = h;
        if (old == nullptr)
        {
            ++elems_;
            if (elems_ > length_)
            {
                // Since each cache entry is fairly large, we aim for a small
                // average linked list length (<= 1).
                Resize();
            }
        }
        return old;
    }

    LRUHandle *Remove(std::string_view key, uint32_t hash)
    {
        LRUHandle **ptr = FindPointer(key, hash);
        LRUHandle  *result = *ptr;
        if (result != nullptr)
        {
            *ptr = result->next_hash;
            --elems_;
        }
        return result;
    }

private:
    // Array of buckets (linked lists of cache entries)
    uint32_t    length_;
    uint32_t    elems_;
    LRUHandle **list_;

    // Return a pointer to slot that matches key/hash or the trailing slot.
    LRUHandle **FindPointer(std::string_view key, uint32_t hash)
    {
        LRUHandle **ptr = &list_[hash & (length_ - 1)];
        while (*ptr != nullptr && ((*ptr)->hash != hash || key != (*ptr)->key()))
        {
            ptr = &(*ptr)->next_hash;
        }
        return ptr;
    }

    void Resize()
    {
        uint32_t new_length = 4;
        while (new_length < elems_)
        {
            new_length *= 2;
        }
        LRUHandle **new_list = new LRUHandle *[new_length];
        memset(new_list, 0, sizeof(new_list[0]) * new_length);
        uint32_t count = 0;
        for (uint32_t i = 0; i < length_; i++)
        {
            LRUHandle *h = list_[i];
            while (h != nullptr)
            {
                LRUHandle  *next = h->next_hash;
                uint32_t    hash = h->hash;
                LRUHandle **ptr = &new_list[hash & (new_length - 1)];
                h->next_hash = *ptr;
                *ptr = h;
                h = next;
                count++;
            }
        }
        assert(elems_ == count);
        delete[] list_;
        list_ = new_list;
        length_ = new_length;
    }
};

// A single shard of sharded cache.
class LRUCache
{
public:
    LRUCache();
    ~LRUCache();

    // Separate from constructor so caller can easily make an array of LRUCache
    void SetCapacity(size_t capacity) { capacity_ = capacity; }

    // Like Cache methods, but with an extra "hash" parameter.
    Cache::Handle *Insert(std::string_view key, uint32_t hash, void *value, size_t charge,
                          void (*deleter)(std::string_view key, void *value));
    Cache::Handle *Lookup(std::string_view key, uint32_t hash);
    void           Release(Cache::Handle *handle);
    void           Erase(std::string_view key, uint32_t hash);
    void           Prune();
    size_t         TotalCharge() const
    {
        std::lock_guard<std::mutex> l(mutex_);
        return usage_;
    }

private:
    void LRU_Remove(LRUHandle *e);
    void LRU_Append(LRUHandle *list, LRUHandle *e);
    void Ref(LRUHandle *e);
    void Unref(LRUHandle *e);
    bool FinishErase(LRUHandle *e);

    // Initialized before use.
    size_t capacity_;

    // mutex_ protects the following state.
    mutable std::mutex mutex_;

    // Dummy head of LRU list
    LRUHandle lru_;

    // Dummy head of in-use list
    LRUHandle in_use_;

    HandleTable table_;
    size_t      usage_;
};

LRUCache::LRUCache() : capacity_(0), usage_(0)
{
    // Make empty circular linked lists.
    lru_.next = &lru_;
    lru_.prev = &lru_;
    in_use_.next = &in_use_;
    in_use_.prev = &in_use_;
}

LRUCache::~LRUCache()
{
    assert(in_use_.next == &in_use_);  // Error if caller has an unreleased handle
    for (LRUHandle *e = lru_.next; e != &lru_;)
    {
        LRUHandle *next = e->next;
        assert(e->in_cache);
        e->in_cache = false;
        assert(e->refs == 1);  // In LRU list, only one reference from cache
        Unref(e);
        e = next;
    }
}

void LRUCache::Ref(LRUHandle *e)
{
    if (e->refs == 1 && e->in_cache)
    {
        // If on lru_ list, move to in_use_ list.
        LRU_Remove(e);
        LRU_Append(&in_use_, e);
    }
    e->refs++;
}

void LRUCache::Unref(LRUHandle *e)
{
    assert(e->refs > 0);
    e->refs--;
    if (e->refs == 0)
    {  // Deallocate.
        assert(!e->in_cache);
        (*e->deleter)(e->key(), e->value);
        free(e);
    }
    else if (e->in_cache && e->refs == 1)
    {
        // No longer in use; move to lru_ list.
        LRU_Remove(e);
        LRU_Append(&lru_, e);
    }
}

void LRUCache::LRU_Remove(LRUHandle *e)
{
    e->next->prev = e->prev;
    e->prev->next = e->next;
}

void LRUCache::LRU_Append(LRUHandle *list, LRUHandle *e)
{
    // Make "e" newest entry by inserting just before lru_
    e->next = list;
    e->prev = list->prev;
    e->prev->next = e;
    e->next->prev = e;
}

Cache::Handle *LRUCache::Lookup(std::string_view key, uint32_t hash)
{
    std::lock_guard<std::mutex> l(mutex_);
    LRUHandle                  *e = table_.Lookup(key, hash);
    if (e != nullptr)
    {
        Ref(e);
    }
    return reinterpret_cast<Cache::Handle *>(e);
}

void LRUCache::Release(Cache::Handle *handle)
{
    std::lock_guard<std::mutex> l(mutex_);
    Unref(reinterpret_cast<LRUHandle *>(handle));
}

Cache::Handle *LRUCache::Insert(std::string_view key, uint32_t hash, void *value, size_t charge,
                                void (*deleter)(std::string_view key, void *value))
{
    std::lock_guard<std::mutex> l(mutex_);

    LRUHandle *e = reinterpret_cast<LRUHandle *>(malloc(sizeof(LRUHandle) - 1 + key.size()));
    e->value = value;
    e->deleter = deleter;
    e->charge = charge;
    e->key_length = key.size();
    e->hash = hash;
    e->in_cache = false;
    e->refs = 1;  // for the returned handle.
    std::memcpy(e->key_data, key.data(), key.size());

    if (capacity_ > 0)
    {
        e->refs++;  // for the cache's reference.
        e->in_cache = true;
        LRU_Append(&in_use_, e);
        usage_ += charge;
        FinishErase(table_.Insert(e));
    }
    else
    {  // don't cache. (capacity_==0 is supported and turns off caching.)
        // next is read by key() in an assert, so it must be initialized
        e->next = nullptr;
    }
    while (usage_ > capacity_ && lru_.next != &lru_)
    {
        LRUHandle *old = lru_.next;
        assert(old->refs == 1);
        bool erased = FinishErase(table_.Remove(old->key(), old->hash));
        if (!erased)
        {  // to avoid unused variable when compiled NDEBUG
            assert(erased);
        }
    }

    return reinterpret_cast<Cache::Handle *>(e);
}

// If e != nullptr, finish removing *e from the cache; it has already been
// removed from the hash table.  Return whether e != nullptr.
bool LRUCache::FinishErase(LRUHandle *e)
{
    if (e != nullptr)
    {
        assert(e->in_cache);
        LRU_Remove(e);
        e->in_cache = false;
        usage_ -= e->charge;
        Unref(e);
    }
    return e != nullptr;
}

void LRUCache::Erase(std::string_view key, uint32_t hash)
{
    std::lock_guard<std::mutex> l(mutex_);
    FinishErase(table_.Remove(key, hash));
}

void LRUCache::Prune()
{
    std::lock_guard<std::mutex> l(mutex_);
    while (lru_.next != &lru_)
    {
        LRUHandle *e = lru_.next;
        assert(e->refs == 1);
        bool erased = FinishErase(table_.Remove(e->key(), e->hash));
        if (!erased)
        {  // to avoid unused variable when compiled NDEBUG
            assert(erased);
        }
    }
}

static const int kNumShardBits = 4;
static const int kNumShards = 1 << kNumShardBits;

class ShardedLRUCache : public Cache
{
private:
    LRUCache   shard_[kNumShards];
    std::mutex id_mutex_;
    uint64_t   last_id_;

    static inline uint32_t HashSlice(std::string_view s) { return Hash(s.data(), s.size(), 0); }

    static uint32_t Shard(uint32_t hash) { return hash >> (32 - kNumShardBits); }

public:
    explicit ShardedLRUCache(size_t capacity) : last_id_(0)
    {
        const size_t per_shard = (capacity + (kNumShards - 1)) / kNumShards;
        for (int s = 0; s < kNumShards; s++)
        {
            shard_[s].SetCapacity(per_shard);
        }
    }
    ~ShardedLRUCache() override {}
    Handle *Insert(std::string_view key, void *value, size_t charge,
                   void (*deleter)(std::string_view key, void *value)) override
    {
        const uint32_t hash = HashSlice(key);
        return shard_[Shard(hash)].Insert(key, hash, value, charge, deleter);
    }
    Handle *Lookup(std::string_view key) override
    {
        const uint32_t hash = HashSlice(key);
        return shard_[Shard(hash)].Lookup(key, hash);
    }
    void Release(Handle *handle) override
    {
        LRUHandle *h = reinterpret_cast<LRUHandle *>(handle);
        shard_[Shard(h->hash)].Release(handle);
    }
    void Erase(std::string_view key) override
    {
        const uint32_t hash = HashSlice(key);
        shard_[Shard(hash)].Erase(key, hash);
    }
    void    *Value(Handle *handle) override { return reinterpret_cast<LRUHandle *>(handle)->value; }
    uint64_t NewId() override
    {
        std::lock_guard<std::mutex> l(id_mutex_);
        return ++(last_id_);
    }
    void Prune() override
    {
        for (int s = 0; s < kNumShards; s++)
        {
            shard_[s].Prune();
        }
    }
    size_t TotalCharge() const override
    {
        size_t total = 0;
        for (int s = 0; s < kNumShards; s++)
        {
            total += shard_[s].TotalCharge();
        }
        return total;
    }
};

}  // end anonymous namespace

std::shared_ptr<Cache> NewLRUCache(size_t capacity) { return std::make_shared<ShardedLRUCache>(capacity); }

}  // namespace storage
}  // namespace zujan

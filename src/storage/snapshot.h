#pragma once

#include <cstdint>

namespace zujan
{
namespace storage
{

class SnapshotList;

class Snapshot
{
private:
    friend class SnapshotList;
    friend class LSMStore;

    uint64_t  sequence_;
    Snapshot* prev_;
    Snapshot* next_;

    explicit Snapshot(uint64_t seq) : sequence_(seq), prev_(this), next_(this) {}
    ~Snapshot() = default;
};

class SnapshotList
{
public:
    SnapshotList() : head_(0)
    {
        head_.prev_ = &head_;
        head_.next_ = &head_;
    }

    bool empty() const { return head_.next_ == &head_; }

    Snapshot* oldest() const { return empty() ? nullptr : head_.next_; }
    Snapshot* newest() const { return empty() ? nullptr : head_.prev_; }

    Snapshot* New(uint64_t seq)
    {
        Snapshot* s = new Snapshot(seq);
        s->next_ = &head_;
        s->prev_ = head_.prev_;
        s->prev_->next_ = s;
        s->next_->prev_ = s;
        return s;
    }

    void Delete(const Snapshot* s)
    {
        Snapshot* curr = const_cast<Snapshot*>(s);
        curr->prev_->next_ = curr->next_;
        curr->next_->prev_ = curr->prev_;
        delete curr;
    }

private:
    Snapshot head_;
};

}  // namespace storage
}  // namespace zujan

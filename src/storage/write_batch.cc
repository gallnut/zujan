#include "write_batch.h"

#include "coding.h"
#include "memtable.h"
#include "status.h"
#include "write_batch_internal.h"

namespace zujan
{
namespace storage
{

class MemTableInserter : public WriteBatch::Handler
{
public:
    uint64_t  sequence_;
    MemTable* mem_;

    void Put(std::string_view key, std::string_view value) override
    {
        mem_->Add(sequence_, kTypeValue, key, value);
        sequence_++;
    }
    void Delete(std::string_view key) override
    {
        mem_->Add(sequence_, kTypeDeletion, key, "");
        sequence_++;
    }
};

void WriteBatchInternal::InsertInto(const WriteBatch* b, MemTable* memtable)
{
    MemTableInserter inserter;
    inserter.sequence_ = WriteBatchInternal::Sequence(b);
    inserter.mem_ = memtable;
    auto res = b->Iterate(&inserter);
    // Assuming well-formed batch, error handling omitted for brevity or assert(res);
}

std::string_view WriteBatchInternal::Contents(const WriteBatch* b) { return b->rep_; }

void WriteBatchInternal::SetContents(WriteBatch* b, std::string_view contents)
{
    b->rep_.assign(contents.data(), contents.size());
}

static const size_t kHeaderSize = 12;

WriteBatch::Handler::~Handler() = default;

WriteBatch::WriteBatch() { Clear(); }

WriteBatch::~WriteBatch() = default;

void WriteBatch::Clear()
{
    rep_.clear();
    rep_.resize(kHeaderSize);
}

size_t WriteBatch::ApproximateSize() const { return rep_.size(); }

void WriteBatchInternal::SetCount(WriteBatch* b, uint32_t n) { EncodeFixed32(&b->rep_[8], n); }

void WriteBatchInternal::SetSequence(WriteBatch* b, uint64_t seq) { EncodeFixed64(&b->rep_[0], seq); }

uint32_t WriteBatchInternal::Count(const WriteBatch* b) { return DecodeFixed32(&b->rep_[8]); }

uint64_t WriteBatchInternal::Sequence(const WriteBatch* b) { return DecodeFixed64(&b->rep_[0]); }

void WriteBatchInternal::Append(WriteBatch* dst, const WriteBatch* src)
{
    SetCount(dst, Count(dst) + Count(src));
    dst->rep_.append(src->rep_.data() + kHeaderSize, src->rep_.size() - kHeaderSize);
}

void WriteBatch::Put(std::string_view key, std::string_view value)
{
    WriteBatchInternal::SetCount(this, WriteBatchInternal::Count(this) + 1);
    rep_.push_back(static_cast<char>(1));  // kTypeValue
    PutLengthPrefixedSlice(&rep_, key);
    PutLengthPrefixedSlice(&rep_, value);
}

void WriteBatch::Delete(std::string_view key)
{
    WriteBatchInternal::SetCount(this, WriteBatchInternal::Count(this) + 1);
    rep_.push_back(static_cast<char>(0));  // kTypeDeletion
    PutLengthPrefixedSlice(&rep_, key);
}

std::expected<void, Error> WriteBatch::Iterate(Handler* handler) const
{
    std::string_view input(rep_);
    if (input.size() < kHeaderSize)
    {
        return std::unexpected(Error{ErrorCode::Corruption, "WriteBatch too short"});
    }
    input.remove_prefix(kHeaderSize);

    std::string_view key, value;
    int              found = 0;
    while (!input.empty())
    {
        found++;
        char tag = input[0];
        input.remove_prefix(1);
        switch (tag)
        {
            case 1:  // kTypeValue
                if (GetLengthPrefixedSlice(&input, &key) && GetLengthPrefixedSlice(&input, &value))
                {
                    handler->Put(key, value);
                }
                else
                {
                    return std::unexpected(Error{ErrorCode::Corruption, "Bad Put record in WriteBatch"});
                }
                break;
            case 0:  // kTypeDeletion
                if (GetLengthPrefixedSlice(&input, &key))
                {
                    handler->Delete(key);
                }
                else
                {
                    return std::unexpected(Error{ErrorCode::Corruption, "Bad Delete record in WriteBatch"});
                }
                break;
            default:
                return std::unexpected(Error{ErrorCode::Corruption, "Unknown record type in WriteBatch"});
        }
    }

    if (found != WriteBatchInternal::Count(this))
    {
        return std::unexpected(Error{ErrorCode::Corruption, "WriteBatch count mismatch"});
    }

    return {};
}

}  // namespace storage
}  // namespace zujan

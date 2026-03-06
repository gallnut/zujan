#include "version_edit.h"

#include "coding.h"

namespace zujan
{
namespace storage
{

enum Tag
{
    kLogNumber = 1,
    kPrevLogNumber = 2,
    kNextFileNumber = 3,
    kLastSequence = 4,
    kDeletedFile = 5,
    kNewFile = 6,
};

void VersionEdit::Clear()
{
    has_log_number_ = false;
    has_prev_log_number_ = false;
    has_next_file_number_ = false;
    has_last_sequence_ = false;
    log_number_ = 0;
    prev_log_number_ = 0;
    next_file_number_ = 0;
    last_sequence_ = 0;
    deleted_files_.clear();
    new_files_.clear();
}

void VersionEdit::EncodeTo(std::string* dst) const
{
    if (has_log_number_)
    {
        PutVarint32(dst, static_cast<uint32_t>(kLogNumber));
        PutVarint64(dst, log_number_);
    }
    if (has_prev_log_number_)
    {
        PutVarint32(dst, static_cast<uint32_t>(kPrevLogNumber));
        PutVarint64(dst, prev_log_number_);
    }
    if (has_next_file_number_)
    {
        PutVarint32(dst, static_cast<uint32_t>(kNextFileNumber));
        PutVarint64(dst, next_file_number_);
    }
    if (has_last_sequence_)
    {
        PutVarint32(dst, static_cast<uint32_t>(kLastSequence));
        PutVarint64(dst, last_sequence_);
    }

    for (const auto& kv : deleted_files_)
    {
        PutVarint32(dst, static_cast<uint32_t>(kDeletedFile));
        PutVarint32(dst, static_cast<uint32_t>(kv.first.first));
        PutVarint64(dst, kv.first.second);
    }

    for (const auto& item : new_files_)
    {
        const int           level = item.first;
        const FileMetaData& f = item.second;
        PutVarint32(dst, static_cast<uint32_t>(kNewFile));
        PutVarint32(dst, static_cast<uint32_t>(level));
        PutVarint64(dst, f.number);
        PutVarint64(dst, f.file_size);
        PutLengthPrefixedSlice(dst, f.smallest);
        PutLengthPrefixedSlice(dst, f.largest);
    }
}

static bool GetInternalKey(std::string_view* input, std::string* dst)
{
    std::string_view str;
    if (GetLengthPrefixedSlice(input, &str))
    {
        dst->assign(str.data(), str.size());
        return true;
    }
    else
    {
        return false;
    }
}

static bool GetLevel(std::string_view* input, int* level)
{
    uint32_t v;
    if (GetVarint32(input, &v))
    {
        *level = v;
        return true;
    }
    else
    {
        return false;
    }
}

bool VersionEdit::DecodeFrom(std::string_view src)
{
    Clear();
    std::string_view input = src;
    const char*      msg = nullptr;
    uint32_t         tag;

    while (msg == nullptr && GetVarint32(&input, &tag))
    {
        switch (tag)
        {
            case kLogNumber:
                if (GetVarint64(&input, &log_number_))
                {
                    has_log_number_ = true;
                }
                else
                {
                    msg = "log number";
                }
                break;
            case kPrevLogNumber:
                if (GetVarint64(&input, &prev_log_number_))
                {
                    has_prev_log_number_ = true;
                }
                else
                {
                    msg = "prev log number";
                }
                break;
            case kNextFileNumber:
                if (GetVarint64(&input, &next_file_number_))
                {
                    has_next_file_number_ = true;
                }
                else
                {
                    msg = "next file number";
                }
                break;
            case kLastSequence:
                if (GetVarint64(&input, &last_sequence_))
                {
                    has_last_sequence_ = true;
                }
                else
                {
                    msg = "last sequence";
                }
                break;
            case kDeletedFile:
            {
                int      level;
                uint64_t number;
                if (GetLevel(&input, &level) && GetVarint64(&input, &number))
                {
                    deleted_files_.insert(std::make_pair(std::make_pair(level, number), true));
                }
                else
                {
                    msg = "deleted file";
                }
                break;
            }
            case kNewFile:
            {
                int         level;
                uint64_t    number;
                uint64_t    file_size;
                std::string smallest, largest;
                if (GetLevel(&input, &level) && GetVarint64(&input, &number) && GetVarint64(&input, &file_size) &&
                    GetInternalKey(&input, &smallest) && GetInternalKey(&input, &largest))
                {
                    AddFile(level, number, file_size, smallest, largest);
                }
                else
                {
                    msg = "new-file entry";
                }
                break;
            }
            default:
                msg = "unknown tag";
                break;
        }
    }

    if (msg == nullptr && !input.empty())
    {
        msg = "invalid tag";
    }

    return msg == nullptr;
}

}  // namespace storage
}  // namespace zujan

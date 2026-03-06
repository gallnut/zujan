#pragma once

#include <cstdint>
#include <map>
#include <string>
#include <vector>

namespace zujan
{
namespace storage
{

struct FileMetaData
{
    int         refs;
    int         allowed_seeks;
    uint64_t    number;
    uint64_t    file_size;
    std::string smallest;
    std::string largest;

    FileMetaData() : refs(0), allowed_seeks(1 << 30), number(0), file_size(0) {}
};

class VersionEdit
{
public:
    VersionEdit() { Clear(); }
    ~VersionEdit() = default;

    void Clear();

    void SetLogNumber(uint64_t num)
    {
        has_log_number_ = true;
        log_number_ = num;
    }

    void SetPrevLogNumber(uint64_t num)
    {
        has_prev_log_number_ = true;
        prev_log_number_ = num;
    }

    void SetNextFile(uint64_t num)
    {
        has_next_file_number_ = true;
        next_file_number_ = num;
    }

    void SetLastSequence(uint64_t seq)
    {
        has_last_sequence_ = true;
        last_sequence_ = seq;
    }

    void AddFile(int level, uint64_t file, uint64_t file_size, const std::string& smallest, const std::string& largest)
    {
        new_files_.push_back(std::make_pair(level, FileMetaData()));
        FileMetaData& f = new_files_.back().second;
        f.number = file;
        f.file_size = file_size;
        f.smallest = smallest;
        f.largest = largest;
    }

    void DeleteFile(int level, uint64_t file)
    {
        deleted_files_.insert(std::make_pair(std::make_pair(level, file), true));
    }

    void EncodeTo(std::string* dst) const;
    bool DecodeFrom(std::string_view src);

    bool     has_log_number() const { return has_log_number_; }
    uint64_t log_number() const { return log_number_; }

    bool     has_prev_log_number() const { return has_prev_log_number_; }
    uint64_t prev_log_number() const { return prev_log_number_; }

    bool     has_next_file_number() const { return has_next_file_number_; }
    uint64_t next_file_number() const { return next_file_number_; }

    bool     has_last_sequence() const { return has_last_sequence_; }
    uint64_t last_sequence() const { return last_sequence_; }

    const std::vector<std::pair<int, FileMetaData>>& new_files() const { return new_files_; }
    const std::map<std::pair<int, uint64_t>, bool>&  deleted_files() const { return deleted_files_; }

private:
    friend class VersionSet;

    bool     has_log_number_;
    uint64_t log_number_;

    bool     has_prev_log_number_;
    uint64_t prev_log_number_;

    bool     has_next_file_number_;
    uint64_t next_file_number_;

    bool     has_last_sequence_;
    uint64_t last_sequence_;

    std::vector<std::pair<int, FileMetaData>> new_files_;
    std::map<std::pair<int, uint64_t>, bool>
        deleted_files_;  // Value is unused, just a dummy for std::set functionality or std::set<std::pair<int,
                         // uint64_t>>. Use map for simplicity with decode.
};

}  // namespace storage
}  // namespace zujan

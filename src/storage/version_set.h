#pragma once

#include <mutex>
#include <string>
#include <vector>

#include "table_builder.h"
#include "version_edit.h"

namespace zujan
{
namespace storage
{

class VersionSet;
class Version;

class Version
{
public:
    void Ref();
    void Unref();

    std::vector<FileMetaData*> files_[2];  // Supports 2 levels for now: 0 and 1

private:
    friend class VersionSet;
    friend struct std::default_delete<Version>;
    explicit Version(VersionSet* vset);
    ~Version();

    VersionSet* vset_;
    Version*    next_;
    Version*    prev_;
    int         refs_;

    Version(const Version&) = delete;
    Version& operator=(const Version&) = delete;
};

class VersionSet
{
public:
    VersionSet(const std::string& dbname, const TableBuilderOptions* options, IOContext* io_ctx);
    ~VersionSet();

    std::expected<void, Error> LogAndApply(VersionEdit* edit, std::mutex* mu);

    std::expected<void, Error> Recover();

    Version* current() const { return current_; }
    uint64_t LastSequence() const { return last_sequence_; }
    void     SetLastSequence(uint64_t s)
    {
        if (s > last_sequence_)
        {
            last_sequence_ = s;
        }
    }
    uint64_t NewFileNumber() { return next_file_number_++; }
    uint64_t LogNumber() const { return log_number_; }
    uint64_t PrevLogNumber() const { return prev_log_number_; }

private:
    friend class Version;
    void                       AppendVersion(Version* v);
    std::expected<void, Error> WriteSnapshot(VersionEdit* edit);

    std::string                dbname_;
    const TableBuilderOptions* options_;
    IOContext*                 io_ctx_;

    uint64_t next_file_number_;
    uint64_t manifest_file_number_;
    uint64_t last_sequence_;
    uint64_t log_number_;
    uint64_t prev_log_number_;  // 0 or backing store for memtable being compacted

    int descriptor_fd_;  // MANIFEST file fd

    Version  dummy_versions_;  // Head of circular doubly-linked list
    Version* current_;
};

}  // namespace storage
}  // namespace zujan

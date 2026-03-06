#include "version_set.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cassert>
#include <memory>
#include <set>

#include "coding.h"

namespace zujan
{
namespace storage
{

Version::Version(VersionSet* vset) : vset_(vset), next_(this), prev_(this), refs_(0) {}

Version::~Version()
{
    assert(refs_ == 0);
    prev_->next_ = next_;
    next_->prev_ = prev_;
    for (int level = 0; level < 2; level++)
    {
        for (size_t i = 0; i < files_[level].size(); i++)
        {
            FileMetaData* f = files_[level][i];
            f->refs--;
            if (f->refs <= 0)
            {
                delete f;
            }
        }
    }
}

void Version::Ref() { refs_++; }

void Version::Unref()
{
    assert(this != &vset_->dummy_versions_);
    assert(refs_ >= 1);
    refs_--;
    if (refs_ == 0)
    {
        delete this;
    }
}

VersionSet::VersionSet(const std::string& dbname, const TableBuilderOptions* options, IOContext* io_ctx)
    : dbname_(dbname),
      options_(options),
      io_ctx_(io_ctx),
      next_file_number_(2),
      manifest_file_number_(0),
      last_sequence_(0),
      log_number_(0),
      prev_log_number_(0),
      descriptor_fd_(-1),
      dummy_versions_(this),
      current_(nullptr)
{
    AppendVersion(new Version(this));
}

VersionSet::~VersionSet()
{
    if (descriptor_fd_ >= 0)
    {
        ::close(descriptor_fd_);
    }
    current_->Unref();
}

void VersionSet::AppendVersion(Version* v)
{
    assert(v->refs_ == 0);
    assert(v != current_);
    if (current_ != nullptr)
    {
        current_->Unref();
    }
    current_ = v;
    v->Ref();

    v->prev_ = dummy_versions_.prev_;
    v->next_ = &dummy_versions_;
    v->prev_->next_ = v;
    v->next_->prev_ = v;
}

class Builder
{
public:
    Builder(VersionSet* vset, Version* base) : vset_(vset), base_(base)
    {
        base_->Ref();
        for (int level = 0; level < 2; level++)
        {
            for (auto* f : base_->files_[level])
            {
                f->refs++;
                levels_[level].added_files.insert(f);
            }
        }
    }

    ~Builder()
    {
        for (int level = 0; level < 2; level++)
        {
            for (auto* f : levels_[level].added_files)
            {
                f->refs--;
                if (f->refs <= 0)
                {
                    delete f;
                }
            }
        }
        base_->Unref();
    }

    void Apply(const VersionEdit* edit)
    {
        for (const auto& kv : edit->deleted_files())
        {
            const int      level = kv.first.first;
            const uint64_t number = kv.first.second;
            levels_[level].deleted_files.insert(number);
        }

        for (const auto& item : edit->new_files())
        {
            const int     level = item.first;
            FileMetaData* f = new FileMetaData(item.second);
            f->refs = 1;
            levels_[level].added_files.insert(f);
            levels_[level].deleted_files.erase(f->number);
        }
    }

    void SaveTo(Version* v)
    {
        for (int level = 0; level < 2; level++)
        {
            for (auto* f : levels_[level].added_files)
            {
                if (levels_[level].deleted_files.count(f->number) == 0)
                {
                    f->refs++;
                    v->files_[level].push_back(f);
                }
            }
        }
    }

private:
    VersionSet* vset_;
    Version*    base_;
    struct LevelState
    {
        std::set<uint64_t>      deleted_files;
        std::set<FileMetaData*> added_files;
    };
    LevelState levels_[2];
};

std::expected<void, Error> VersionSet::LogAndApply(VersionEdit* edit, std::mutex* mu)
{
    if (edit->has_log_number_)
    {
        assert(edit->log_number_ >= log_number_);
        assert(edit->log_number_ < next_file_number_);
    }
    else
    {
        edit->SetLogNumber(log_number_);
    }

    if (!edit->has_prev_log_number_)
    {
        edit->SetPrevLogNumber(prev_log_number_);
    }

    edit->SetNextFile(next_file_number_);
    edit->SetLastSequence(last_sequence_);

    std::unique_ptr<Version> v(new Version(this));
    {
        Builder builder(this, current_);
        builder.Apply(edit);
        builder.SaveTo(v.get());
    }

    std::string record;
    edit->EncodeTo(&record);

    // Simple manifest record: size prefix + var bytes
    std::string final_record;
    PutVarint32(&final_record, record.size());
    final_record.append(record);

    if (descriptor_fd_ < 0)
    {
        manifest_file_number_ = NewFileNumber();
        std::string manifest_path = dbname_ + "/MANIFEST-" + std::to_string(manifest_file_number_);
        descriptor_fd_ = ::open(manifest_path.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
        if (descriptor_fd_ < 0)
        {
            return std::unexpected(Error{ErrorCode::IOError, "Failed to open MANIFEST"});
        }

        if (auto res = WriteSnapshot(edit); !res)
        {
            return std::unexpected(res.error());
        }
        final_record.clear();
        PutVarint32(&final_record, record.size());
        final_record.append(record);

        // Update CURRENT
        std::string current_path = dbname_ + "/CURRENT";
        std::string tmp_path = dbname_ + "/CURRENT.tmp";
        std::string manifest_filename = "MANIFEST-" + std::to_string(manifest_file_number_) + "\n";
        int         fd = ::open(tmp_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (fd >= 0)
        {
            auto unused = ::write(fd, manifest_filename.data(), manifest_filename.size());
            ::close(fd);
            ::rename(tmp_path.c_str(), current_path.c_str());
        }
    }

    if (::write(descriptor_fd_, final_record.data(), final_record.size()) != (ssize_t)final_record.size())
    {
        return std::unexpected(Error{ErrorCode::IOError, "Failed to write MANIFEST"});
    }

    // sync
    ::fsync(descriptor_fd_);

    AppendVersion(v.release());
    log_number_ = edit->log_number_;
    prev_log_number_ = edit->prev_log_number_;

    return {};
}

std::expected<void, Error> VersionSet::WriteSnapshot(VersionEdit* edit)
{
    VersionEdit snapshot;
    for (int level = 0; level < 2; level++)
    {
        for (const auto* f : current_->files_[level])
        {
            snapshot.AddFile(level, f->number, f->file_size, f->smallest, f->largest);
        }
    }

    std::string record;
    snapshot.EncodeTo(&record);
    std::string final_record;
    PutVarint32(&final_record, record.size());
    final_record.append(record);

    if (::write(descriptor_fd_, final_record.data(), final_record.size()) != (ssize_t)final_record.size())
    {
        return std::unexpected(Error{ErrorCode::IOError, "Failed to write MANIFEST snapshot"});
    }
    return {};
}

std::expected<void, Error> VersionSet::Recover()
{
    std::string current_path = dbname_ + "/CURRENT";
    int         fd = ::open(current_path.c_str(), O_RDONLY);
    if (fd < 0)
    {
        return std::unexpected(Error{ErrorCode::IOError, "Manifest not found"});
    }

    char    buf[512];
    ssize_t n = ::read(fd, buf, sizeof(buf) - 1);
    ::close(fd);
    if (n <= 0)
    {
        return std::unexpected(Error{ErrorCode::IOError, "Manifest empty"});
    }
    buf[n] = '\0';
    std::string current_str(buf);
    if (current_str.back() == '\n') current_str.pop_back();

    std::string manifest_path = dbname_ + "/" + current_str;
    int         mfd = ::open(manifest_path.c_str(), O_RDONLY);
    if (mfd < 0)
    {
        return std::unexpected(Error{ErrorCode::IOError, "Could not open MANIFEST"});
    }

    struct stat st;
    ::fstat(mfd, &st);
    std::string manifest_data;
    manifest_data.resize(st.st_size);
    if (::read(mfd, manifest_data.data(), manifest_data.size()) < 0)
    {
        ::close(mfd);
        return std::unexpected(Error{ErrorCode::IOError, "Failed to read MANIFEST"});
    }
    ::close(mfd);

    Builder          builder(this, current_);
    std::string_view input(manifest_data);

    bool have_log_number = false;
    bool have_prev_log_number = false;
    bool have_next_file = false;
    bool have_last_sequence = false;

    while (!input.empty())
    {
        uint32_t len;
        if (!GetVarint32(&input, &len)) break;
        if (input.size() < len) break;
        std::string_view edit_data(input.data(), len);
        input.remove_prefix(len);

        VersionEdit edit;
        if (edit.DecodeFrom(edit_data))
        {
            if (edit.has_log_number_)
            {
                log_number_ = edit.log_number_;
                have_log_number = true;
            }
            if (edit.has_prev_log_number_)
            {
                prev_log_number_ = edit.prev_log_number_;
                have_prev_log_number = true;
            }
            if (edit.has_next_file_number_)
            {
                next_file_number_ = edit.next_file_number_;
                have_next_file = true;
            }
            if (edit.has_last_sequence_)
            {
                last_sequence_ = edit.last_sequence_;
                have_last_sequence = true;
            }
            builder.Apply(&edit);
        }
    }

    if (!have_next_file)
    {
        return std::unexpected(Error{ErrorCode::IOError, "no next-file-number in local db"});
    }

    std::unique_ptr<Version> v(new Version(this));
    builder.SaveTo(v.get());
    AppendVersion(v.release());

    descriptor_fd_ = ::open(manifest_path.c_str(), O_WRONLY | O_APPEND);
    if (descriptor_fd_ < 0)
    {
        return std::unexpected(Error{ErrorCode::IOError, "Could not reopen MANIFEST"});
    }

    return {};
}

}  // namespace storage
}  // namespace zujan

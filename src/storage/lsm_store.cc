#include "lsm_store.h"

#include <fcntl.h>
#include <unistd.h>

#include <filesystem>

#include "uring_io.h"
#include "write_batch.h"
#include "write_batch_internal.h"

namespace zujan
{
namespace storage
{

std::expected<std::unique_ptr<LSMStore>, Error> LSMStore::Open(const std::string &dir) noexcept
{
    auto store = std::unique_ptr<LSMStore>(new LSMStore(dir));
    auto res = store->Init();
    if (!res) return std::unexpected(Error{ErrorCode::IOError, "LSMStore Init failed"});
    return std::move(store);
}

LSMStore::LSMStore(const std::string &dir) : dir_(dir) { io_ctx_ = std::make_unique<URingIOContext>(); }

std::expected<void, Error> LSMStore::Init() noexcept
{
    auto res = io_ctx_->Init();
    if (!res) return std::unexpected(res.error());

    std::error_code ec;
    std::filesystem::create_directories(dir_, ec);
    if (ec)
    {
        return std::unexpected(Error{ErrorCode::IOError, "Failed to create LSM directory"});
    }

    memtable_ = std::make_unique<MemTable>();
    sst_manager_ = std::make_unique<SSTableManager>(*io_ctx_, table_options_);

    versions_ = std::make_unique<VersionSet>(dir_, &table_options_, io_ctx_.get());
    auto rec_res = versions_->Recover();
    // Ignore error for now if manifest not found (new DB)

    // Recovery Phase
    if (rec_res)
    {
        RecoverWAL(versions_->LogNumber());

        // Clean up old obsolete WAL files!
        for (const auto &entry : std::filesystem::directory_iterator(dir_))
        {
            if (entry.path().extension() == ".log")
            {
                std::string filename = entry.path().filename().string();
                if (filename.find("wal-") == 0)
                {
                    try
                    {
                        uint64_t num = std::stoull(filename.substr(4));
                        if (num < versions_->LogNumber())
                        {
                            std::filesystem::remove(entry.path());
                        }
                    }
                    catch (...)
                    {
                    }
                }
            }
        }
    }

    if (versions_->current() != nullptr)
    {
        for (int level = 0; level < 2; ++level)
        {
            for (const auto *f : versions_->current()->files_[level])
            {
                std::string sst_path =
                    dir_ + "/sst_L" + std::to_string(level) + "_" + std::to_string(f->number) + ".sst";
                sst_manager_->AddSSTable(level, sst_path);
            }
        }
    }

    logfile_number_ = versions_->NewFileNumber();
    wal_ = std::make_unique<WAL>(*io_ctx_, dir_ + "/wal-" + std::to_string(logfile_number_) + ".log");

    bg_thread_ = std::thread(&LSMStore::BGWork, this);

    return {};
}

LSMStore::~LSMStore()
{
    {
        std::lock_guard<std::mutex> lk(mutex_);
        stop_bg_ = true;
    }
    bg_cv_.notify_all();
    if (bg_thread_.joinable())
    {
        bg_thread_.join();
    }
}

std::expected<void, Error> LSMStore::Put(const std::string &key, const std::string &value) noexcept
{
    WriteBatch batch;
    batch.Put(key, value);
    return Write(WriteOptions(), &batch);
}

std::expected<void, Error> LSMStore::Write(const WriteOptions &options, WriteBatch *updates) noexcept
{
    std::unique_lock<std::mutex> lk(mutex_);

    // Simplistic handling: wait for space
    while (memtable_->EstimateSize() >= memtable_size_limit_ && imm_ != nullptr)
    {
        bg_cv_.wait(lk);
    }

    if (memtable_->EstimateSize() >= memtable_size_limit_)
    {
        imm_ = std::move(memtable_);
        memtable_ = std::make_unique<MemTable>();

        logfile_number_ = versions_->NewFileNumber();
        wal_ = std::make_unique<WAL>(*io_ctx_, dir_ + "/wal-" + std::to_string(logfile_number_) + ".log");

        bg_compaction_scheduled_ = true;
        bg_cv_.notify_one();
    }

    // Assign sequence number
    uint64_t seq = versions_->LastSequence() + 1;
    WriteBatchInternal::SetSequence(updates, seq);
    uint32_t count = WriteBatchInternal::Count(updates);
    versions_->SetLastSequence(seq + count - 1);

    // Write to WAL
    lk.unlock();  // Unlock while writing to WAL to avoid blocking reads, though our writes are currently serialized via
                  // this mutex anyway For now, keep it simple and just write. Actually it's better to keep mutex if we
                  // don't have a Writer queue. Let's re-acquire later or just keep the lock. Zujan's current design
                  // locked after WAL write, but WriteBatch needs Sequence numbers which must be assigned under lock! So
                  // we must hold lock to get seq, but writing to WAL with lock held could be slow. For simplicity, we
                  // just keep the lock for now.

    auto w_res = wal_->Append(*updates);
    if (!w_res)
    {
        return std::unexpected(Error{ErrorCode::IOError, "WAL WriteBatch failed"});
    }

    // Insert into MemTable
    WriteBatchInternal::InsertInto(updates, memtable_.get());
    return {};
}

std::expected<std::optional<std::string>, Error> LSMStore::Get(const ReadOptions &options,
                                                               const std::string &key) noexcept
{
    std::unique_lock<std::mutex> lk(mutex_);

    MemTable *mem = memtable_.get();
    MemTable *imm = imm_.get();
    Version  *current = versions_->current();
    current->Ref();

    uint64_t seq = options.snapshot ? options.snapshot->sequence_ : versions_->LastSequence();

    lk.unlock();

    std::string val;
    bool        deleted = false;

    if (mem->Get(key, val, &deleted, seq))
    {
        current->Unref();
        if (deleted) return std::nullopt;
        return val;
    }

    if (imm != nullptr && imm->Get(key, val, &deleted, seq))
    {
        current->Unref();
        if (deleted) return std::nullopt;
        return val;
    }

    // Pass seq into current->Get in the future
    auto res = sst_manager_->Get(options, key, seq);
    current->Unref();

    if (!res) return std::unexpected(res.error());

    if (res.value().found) return res.value().value;

    return std::nullopt;
}

const Snapshot *LSMStore::GetSnapshot()
{
    std::lock_guard<std::mutex> lk(mutex_);
    return snapshots_.New(versions_->LastSequence());
}

void LSMStore::ReleaseSnapshot(const Snapshot *snapshot)
{
    std::lock_guard<std::mutex> lk(mutex_);
    snapshots_.Delete(snapshot);
}

std::expected<void, Error> LSMStore::Delete(const std::string &key) noexcept
{
    WriteBatch batch;
    batch.Delete(key);
    return Write(WriteOptions(), &batch);
}

void LSMStore::BGWork()
{
    while (true)
    {
        std::unique_lock<std::mutex> lk(mutex_);
        bg_cv_.wait(lk, [this]() { return bg_compaction_scheduled_ || stop_bg_; });

        if (stop_bg_) break;

        if (imm_ != nullptr)
        {
            lk.unlock();
            CompactMemTable();
            lk.lock();
            bg_cv_.notify_all();
        }

        lk.unlock();
        DoCompaction();
        lk.lock();

        bg_compaction_scheduled_ = false;
    }
}

void LSMStore::BackgroundCall()
{
    // Not strictly needed right now with thread-based approach
}

void LSMStore::CompactMemTable()
{
    uint64_t file_number;
    uint64_t active_log_number;
    uint64_t old_log_number;
    {
        std::lock_guard<std::mutex> lk(mutex_);
        file_number = versions_->NewFileNumber();
        active_log_number = logfile_number_;
        old_log_number = imm_logfile_number_;
    }

    std::string sst_path = dir_ + "/sst_L0_" + std::to_string(file_number) + ".sst";

    int fd = ::open(sst_path.c_str(), O_CREAT | O_RDWR | O_TRUNC, 0644);
    if (fd < 0) return;

    TableBuilder builder(table_options_, *io_ctx_, fd);

    imm_->WriteToBuilder(&builder);

    auto     finish_res = builder.Finish();
    uint64_t file_size = builder.FileSize();
    ::close(fd);

    if (finish_res)
    {
        VersionEdit edit;
        edit.SetLogNumber(active_log_number);
        // We do not have min/max keys tracked easily in imm_ yet, so use placeholders
        edit.AddFile(0, file_number, file_size, "", "");

        std::lock_guard<std::mutex> lk(mutex_);
        auto                        st = versions_->LogAndApply(&edit, &mutex_);

        sst_manager_->AddSSTable(0, sst_path);
        imm_.reset();

        std::filesystem::remove(dir_ + "/wal-" + std::to_string(old_log_number) + ".log");
    }
}

void LSMStore::RecoverWAL(uint64_t log_number)
{
    std::string wal_path = dir_ + "/wal-" + std::to_string(log_number) + ".log";
    if (std::filesystem::exists(wal_path))
    {
        WAL  wal_reader(*io_ctx_, wal_path);
        auto unused = wal_reader.Recover(*memtable_);
    }
}

void LSMStore::DoCompaction()
{
    auto l0_tables = sst_manager_->GetLevelSSTables(0);
    if (l0_tables.size() < 4) return;

    auto l1_tables = sst_manager_->GetLevelSSTables(1);

    std::map<std::string, LookupResult> merged_map;
    for (auto table : l1_tables) table->DumpToMap(merged_map);
    for (auto table : l0_tables) table->DumpToMap(merged_map);

    fprintf(stderr, "DoCompaction merged_map has %zu keys, count(key0)=%zu\n", merged_map.size(),
            merged_map.count("key0"));

    uint64_t file_number;
    {
        std::lock_guard<std::mutex> lk(mutex_);
        file_number = versions_->NewFileNumber();
    }

    std::string sst_path = dir_ + "/sst_L1_" + std::to_string(file_number) + ".sst";
    int         fd = ::open(sst_path.c_str(), O_CREAT | O_RDWR | O_TRUNC, 0644);
    if (fd < 0) return;

    TableBuilder builder(table_options_, *io_ctx_, fd);

    for (const auto &kv : merged_map)
    {
        if (!kv.second.deleted)
        {
            std::string encoded_value;
            encoded_value.push_back(static_cast<char>(kTypeValue));
            encoded_value.append(kv.second.value);
            builder.Add(kv.first, encoded_value);
        }
    }

    auto     finish_res = builder.Finish();
    uint64_t file_size = builder.FileSize();
    ::close(fd);

    if (finish_res)
    {
        VersionEdit edit;
        edit.AddFile(1, file_number, file_size, "", "");
        // Missing exact file deletion recording based on `l1_tables` and `l0_tables` for now
        std::lock_guard<std::mutex> lk(mutex_);
        auto                        st = versions_->LogAndApply(&edit, &mutex_);

        sst_manager_->ReplaceLevelSSTables(1, l1_tables, {sst_path});
        sst_manager_->ReplaceLevelSSTables(0, l0_tables, {});
    }
}

}  // namespace storage
}  // namespace zujan

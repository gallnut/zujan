#include "lsm_store.h"

#include <fcntl.h>
#include <unistd.h>

#include <filesystem>
#include <algorithm>
#include <vector>
#include <queue>

#include "uring_io.h"
#include "write_batch.h"
#include "write_batch_internal.h"

namespace zujan
{
namespace storage
{

std::expected<std::unique_ptr<LSMStore>, Error> LSMStore::Open(const std::string &dir, const LSMStoreOptions &options) noexcept
{
    auto store = std::unique_ptr<LSMStore>(new LSMStore(dir, options));
    auto res = store->Init();
    if (!res) return std::unexpected(Error{ErrorCode::IOError, "LSMStore Init failed"});
    return std::move(store);
}

LSMStore::LSMStore(const std::string &dir, const LSMStoreOptions &options) : dir_(dir), options_(options) { io_ctx_ = std::make_unique<URingIOContext>(); }

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
    if (rec_res && !options_.disable_wal)
    {
        std::vector<uint64_t> logs_to_recover;

        // Clean up old obsolete WAL files and find logs to recover
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
                        else
                        {
                            logs_to_recover.push_back(num);
                        }
                    }
                    catch (...)
                    {
                    }
                }
            }
        }

        std::sort(logs_to_recover.begin(), logs_to_recover.end());
        for (uint64_t num : logs_to_recover)
        {
            RecoverWAL(num);
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
    if (!options_.disable_wal) {
        wal_ = std::make_unique<WAL>(*io_ctx_, dir_ + "/wal-" + std::to_string(logfile_number_) + ".log");
    }

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
    if (options_.disable_wal)
    {
        std::unique_lock<std::mutex> lk(mutex_);
        // Make room for write
        while (memtable_->EstimateSize() >= memtable_size_limit_ && imm_ != nullptr)
        {
            bg_cv_.wait(lk);
        }

        if (memtable_->EstimateSize() >= memtable_size_limit_)
        {
            imm_logfile_number_ = logfile_number_;
            imm_ = std::move(memtable_);
            memtable_ = std::make_unique<MemTable>();

            logfile_number_ = versions_->NewFileNumber();

            bg_compaction_scheduled_ = true;
            bg_cv_.notify_one();
        }

        uint64_t seq = versions_->LastSequence() + 1;
        WriteBatchInternal::SetSequence(updates, seq);
        versions_->SetLastSequence(seq + WriteBatchInternal::Count(updates) - 1);
        WriteBatchInternal::InsertInto(updates, memtable_.get());
        
        return {};
    }

    Writer w(updates);
    std::unique_lock<std::mutex> lk(mutex_);
    writers_.push_back(&w);

    while (!w.done && &w != writers_.front())
    {
        w.cv.wait(lk);
    }

    if (w.done)
    {
        return w.status;
    }

    // Make room for write
    while (memtable_->EstimateSize() >= memtable_size_limit_ && imm_ != nullptr)
    {
        bg_cv_.wait(lk);
    }

    if (memtable_->EstimateSize() >= memtable_size_limit_)
    {
        imm_logfile_number_ = logfile_number_;
        imm_ = std::move(memtable_);
        memtable_ = std::make_unique<MemTable>();

        logfile_number_ = versions_->NewFileNumber();
        wal_ = std::make_unique<WAL>(*io_ctx_, dir_ + "/wal-" + std::to_string(logfile_number_) + ".log");

        bg_compaction_scheduled_ = true;
        bg_cv_.notify_one();
    }

    // Build the Write Group
    uint64_t seq = versions_->LastSequence() + 1;
    uint32_t total_count = 0;
    WriteBatch group_batch;
    std::vector<Writer*> group;

    size_t size = 0;
    for (auto* writer : writers_)
    {
        group.push_back(writer);
        WriteBatchInternal::Append(&group_batch, writer->batch);
        total_count += WriteBatchInternal::Count(writer->batch);
        size += writer->batch->ApproximateSize();
        
        // Stop batching if it gets too large
        if (size > 1 * 1024 * 1024) break; 
    }

    WriteBatchInternal::SetSequence(&group_batch, seq);
    versions_->SetLastSequence(seq + total_count - 1);

    lk.unlock();

    // Write to WAL without holding the mutex
    auto w_res = wal_->Append(group_batch);
    if (!w_res)
    {
        w.status = std::unexpected(Error{ErrorCode::IOError, "WAL WriteBatch failed"});
    }
    else
    {
        // Insert to MemTable safely (we are the only writer since we are the Leader)
        WriteBatchInternal::InsertInto(&group_batch, memtable_.get());
        w.status = {};
    }

    lk.lock();

    // Finish the group and wake up followers
    for (auto* writer : group)
    {
        writer->done = true;
        writer->status = w.status;
        writer->cv.notify_one();
        writers_.pop_front();
    }

    // Wake up next leader if any
    if (!writers_.empty())
    {
        writers_.front()->cv.notify_one();
    }

    return w.status;
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

        bg_compaction_scheduled_ = false;

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

        if (!options_.disable_wal) {
            std::filesystem::remove(dir_ + "/wal-" + std::to_string(old_log_number) + ".log");
        }
    }
}

void LSMStore::RecoverWAL(uint64_t log_number)
{
    std::string wal_path = dir_ + "/wal-" + std::to_string(log_number) + ".log";
    if (std::filesystem::exists(wal_path))
    {
        WAL  wal_reader(*io_ctx_, wal_path);
        auto res = wal_reader.Recover(*memtable_);
        if (res && res.value() > versions_->LastSequence())
        {
            versions_->SetLastSequence(res.value());
        }
    }
}

namespace {
class MergingIterator {
public:
    MergingIterator(std::vector<std::unique_ptr<SSTableIterator>> iters) : iters_(std::move(iters)) {}

    void SeekToFirst() {
        for (auto& it : iters_) {
            it->SeekToFirst();
        }
        BuildHeap();
    }

    bool Valid() const {
        return !heap_.empty();
    }

    void Next() {
        if (heap_.empty()) return;
        HeapNode top = heap_.front();
        std::pop_heap(heap_.begin(), heap_.end(), HeapComp());
        heap_.pop_back();

        top.iter->Next();
        if (top.iter->Valid()) {
            heap_.push_back(top);
            std::push_heap(heap_.begin(), heap_.end(), HeapComp());
        }
    }

    std::string_view key() const {
        return heap_.front().iter->key();
    }

    std::string_view value() const {
        return heap_.front().iter->value();
    }

    bool IsDeleted() const {
        std::string_view v = value();
        if (!v.empty() && v[0] == kTypeDeletion) {
            return true;
        }
        return false;
    }
    
    std::string_view RealValue() const {
        std::string_view v = value();
        if (!v.empty() && v[0] == kTypeValue) {
            return v.substr(1);
        }
        return "";
    }

private:
    struct HeapNode {
        SSTableIterator* iter;
        size_t iter_idx; 
    };

    struct HeapComp {
        bool operator()(const HeapNode& a, const HeapNode& b) const {
            int cmp = a.iter->key().compare(b.iter->key());
            if (cmp != 0) {
                // Min heap: return true if a > b (so smallest key is at top)
                return cmp > 0;
            }
            // If keys are equal, we want the newer one. 
            // In iters_, lower iter_idx corresponds to newer data.
            // Min heap: return true if a > b. a is "greater" (less priority) if it represents older data (larger iter_idx).
            return a.iter_idx > b.iter_idx;
        }
    };

    void BuildHeap() {
        heap_.clear();
        for (size_t i = 0; i < iters_.size(); ++i) {
            if (iters_[i]->Valid()) {
                heap_.push_back({iters_[i].get(), i});
            }
        }
        std::make_heap(heap_.begin(), heap_.end(), HeapComp());
    }

    std::vector<std::unique_ptr<SSTableIterator>> iters_;
    std::vector<HeapNode> heap_;
};
} // namespace

void LSMStore::DoCompaction()
{
    auto l0_tables = sst_manager_->GetLevelSSTables(0);
    if (l0_tables.size() < 4) return;

    auto l1_tables = sst_manager_->GetLevelSSTables(1);

    std::vector<std::unique_ptr<SSTableIterator>> iters;
    // L0 blocks are newer. Push newest first (end of vector).
    for (auto it = l0_tables.rbegin(); it != l0_tables.rend(); ++it) {
        iters.push_back((*it)->NewIterator());
    }
    // L1 blocks are older. 
    for (auto it = l1_tables.rbegin(); it != l1_tables.rend(); ++it) {
        iters.push_back((*it)->NewIterator());
    }

    MergingIterator iter(std::move(iters));
    iter.SeekToFirst();

    uint64_t file_number;
    {
        std::lock_guard<std::mutex> lk(mutex_);
        file_number = versions_->NewFileNumber();
    }

    std::string sst_path = dir_ + "/sst_L1_" + std::to_string(file_number) + ".sst";
    int         fd = ::open(sst_path.c_str(), O_CREAT | O_RDWR | O_TRUNC, 0644);
    if (fd < 0) return;

    TableBuilder builder(table_options_, *io_ctx_, fd);

    std::string last_key = "";
    bool first = true;

    while (iter.Valid()) {
        std::string current_key(iter.key());

        if (first || current_key != last_key) {
            // New key
            if (!iter.IsDeleted()) {
                std::string encoded_value;
                encoded_value.push_back(static_cast<char>(kTypeValue));
                encoded_value.append(iter.RealValue());
                builder.Add(current_key, encoded_value);
            }
            last_key = current_key;
            first = false;
        }
        
        iter.Next();
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

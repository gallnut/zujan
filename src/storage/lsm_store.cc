#include "lsm_store.h"

#include <filesystem>

#include "uring_io.h"

namespace zujan
{
namespace storage
{

// Factory or Init might be implemented later, using Factory for LSMStore
// itself:
std::expected<std::unique_ptr<LSMStore>, Error> LSMStore::Open(const std::string &dir) noexcept
{
    auto store = std::unique_ptr<LSMStore>(new LSMStore(dir));
    auto res = store->Init();
    if (!res) return std::unexpected(Error{ErrorCode::IOError, "LSMStore Init failed"});
    return std::move(store);
}

LSMStore::LSMStore(const std::string &dir) : dir_(dir)
{
    // Use URing by default, could use Posix as fallback
    io_ctx_ = std::make_unique<URingIOContext>();
}

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
    wal_ = std::make_unique<WAL>(*io_ctx_, dir_ + "/wal.log");
    table_options_.filter_policy = std::make_shared<BloomFilterPolicy>(10);
    sst_manager_ = std::make_unique<SSTableManager>(*io_ctx_, table_options_);

    return {};
}

LSMStore::~LSMStore() = default;

std::expected<void, Error> LSMStore::Put(const std::string &key, const std::string &value) noexcept
{
    auto w_res = wal_->AppendPut(key, value);
    if (!w_res)
    {
        return std::unexpected(Error{ErrorCode::IOError, "WAL Put failed"});
    }
    memtable_->Put(key, value);
    return {};
}

std::expected<std::optional<std::string>, Error> LSMStore::Get(const std::string &key) noexcept
{
    // 1. MemTable GET
    if (auto val = memtable_->Get(key))
    {
        return *val;
    }

    // 2. SSTables GET
    ReadOptions ropt;
    auto        err = sst_manager_->Get(ropt, key);
    if (!err)
    {
        return std::unexpected(Error{ErrorCode::IOError, "SSTable Get failed"});
    }

    return *err;
}

std::expected<void, Error> LSMStore::Delete(const std::string &key) noexcept
{
    auto w_res = wal_->AppendDelete(key);
    if (!w_res)
    {
        return std::unexpected(Error{ErrorCode::IOError, "WAL Delete failed"});
    }
    memtable_->Delete(key);
    return {};
}

}  // namespace storage
}  // namespace zujan

#include "sstable.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <cstring>

#include "coding.h"
#include "memtable.h"

namespace zujan
{
namespace storage
{

SSTable::SSTable(IOContext &io_ctx, int fd, uint64_t file_size, const TableBuilderOptions &options)
    : io_ctx_(io_ctx), fd_(fd), file_size_(file_size), options_(options)
{
}

SSTable::~SSTable()
{
    if (fd_ >= 0)
    {
        ::close(fd_);
    }
}

std::expected<std::unique_ptr<SSTable>, Error> SSTable::Open(const TableBuilderOptions &options, IOContext &io_ctx,
                                                             const std::string &filepath)
{
    int fd = ::open(filepath.c_str(), O_RDONLY);
    if (fd < 0) return std::unexpected(Error{ErrorCode::IOError, "Failed to open SSTable file"});

    struct stat st;
    if (::fstat(fd, &st) != 0)
    {
        ::close(fd);
        return std::unexpected(Error{ErrorCode::IOError, "Failed to stat SSTable file"});
    }

    auto table = std::unique_ptr<SSTable>(new SSTable(io_ctx, fd, st.st_size, options));
    auto res = table->ReadFooterAndIndex();
    if (!res) return std::unexpected(res.error());
    return table;
}

std::expected<void, Error> SSTable::ReadFooterAndIndex()
{
    if (file_size_ < 48) return std::unexpected(Error{ErrorCode::IOError, "File too short to be an SSTable"});

    // Footer is 48 bytes (magic = 8, 40 bytes padding for 2 BlockHandles)
    uint64_t footer_offset = file_size_ - 48;
    // Use aligned IO, read the last block of bytes. Simplify to pread for exact
    // bytes.
    char    footer_buf[48];
    ssize_t n = ::pread(fd_, footer_buf, 48, footer_offset);
    if (n != 48) return std::unexpected(Error{ErrorCode::IOError, "Failed to read footer"});

    uint64_t magic = DecodeFixed64(footer_buf + 40);
    if (magic != 0xdb4775248b80fb57ull) return std::unexpected(Error{ErrorCode::IOError, "Bad magic number"});

    uint64_t filter_offset, filter_size;
    uint64_t index_offset, index_size;

    const char *p = footer_buf;
    p = GetVarint64Ptr(p, footer_buf + 20, &filter_offset);
    p = GetVarint64Ptr(p, footer_buf + 20, &filter_size);

    const char *p2 = footer_buf + 20;
    p2 = GetVarint64Ptr(p2, footer_buf + 40, &index_offset);
    p2 = GetVarint64Ptr(p2, footer_buf + 40, &index_size);

    // Read Filter Block
    if (filter_size > 0 && options_.filter_policy)
    {
        auto block_res = ReadBlock(filter_offset, filter_size);
        if (block_res)
        {
            filter_data_ = std::string((*block_res)->NewIterator()->value());  // Hack: get raw data string
            // Actually Block abstraction expects a restart array.
            // We should read directly:
            char *fbuf = new char[filter_size];
            ::pread(fd_, fbuf, filter_size, filter_offset);
            filter_data_.assign(fbuf, filter_size);
            delete[] fbuf;
            filter_ = std::make_unique<FilterBlockReader>(options_.filter_policy.get(), filter_data_);
        }
    }

    // Read Index Block
    auto index_res = ReadBlock(index_offset, index_size);
    if (!index_res) return std::unexpected(index_res.error());
    index_block_ = std::move(*index_res);

    return {};
}

std::expected<std::unique_ptr<Block>, Error> SSTable::ReadBlock(uint64_t offset, uint64_t size) const
{
    char   *buf = new char[size];
    ssize_t n = ::pread(fd_, buf, size, offset);
    if (n != size)
    {
        delete[] buf;
        return std::unexpected(Error{ErrorCode::IOError, "Failed to read block"});
    }
    std::string_view contents(buf, size);
    // Memory leak in straightforward Implementation. Should be wrapped in custom
    // Block that frees later. For standard usage, a Cache handles deletion. Here
    // we leak 'buf' if we don't handle it.
    class HeapBlock : public Block
    {
        char *buf_;

    public:
        HeapBlock(char *buf, size_t sz) : Block(std::string_view(buf, sz)), buf_(buf) {}
        ~HeapBlock() { delete[] buf_; }
    };
    return std::make_unique<HeapBlock>(buf, size);
}

std::expected<LookupResult, Error> SSTable::Get(const ReadOptions &options, std::string_view key, uint64_t seq) noexcept
{
    std::unique_ptr<Block::Iterator> iiter(index_block_->NewIterator());
    iiter->Seek(key);

    if (iiter->Valid())
    {
        std::string_view handle = iiter->value();
        uint64_t         block_offset, block_size;
        const char      *p = handle.data();
        p = GetVarint64Ptr(p, handle.data() + handle.size(), &block_offset);
        p = GetVarint64Ptr(p, handle.data() + handle.size(), &block_size);

        if (filter_ && !filter_->KeyMayMatch(block_offset, key))
        {
            return LookupResult{};
        }

        Block                 *block = nullptr;
        Cache::Handle         *cache_handle = nullptr;
        std::unique_ptr<Block> local_block;

        if (options_.block_cache && options.fill_cache)
        {
            char cache_key_buffer[16];
            EncodeFixed64(cache_key_buffer, cache_id_);
            EncodeFixed64(cache_key_buffer + 8, block_offset);
            std::string_view cache_key(cache_key_buffer, sizeof(cache_key_buffer));

            cache_handle = options_.block_cache->Lookup(cache_key);
            if (cache_handle != nullptr)
            {
                block = reinterpret_cast<Block *>(options_.block_cache->Value(cache_handle));
            }
            else
            {
                auto block_res = ReadBlock(block_offset, block_size);
                if (!block_res) return std::unexpected(block_res.error());

                block = block_res->release();
                cache_handle =
                    options_.block_cache->Insert(cache_key, block, block->size(), [](std::string_view /*k*/, void *v)
                                                 { delete reinterpret_cast<Block *>(v); });
            }
        }
        else
        {
            auto block_res = ReadBlock(block_offset, block_size);
            if (!block_res) return std::unexpected(block_res.error());
            local_block = std::move(*block_res);
            block = local_block.get();
        }

        std::unique_ptr<Block::Iterator> biter(block->NewIterator());
        biter->Seek(key);

        LookupResult result;
        if (biter->Valid() && biter->key() == key)
        {
            std::string_view raw = biter->value();
            if (!raw.empty())
            {
                ValueType type = static_cast<ValueType>(raw[0]);
                if (type == kTypeValue)
                {
                    result.found = true;
                    result.deleted = false;
                    result.value = std::string(raw.substr(1));
                }
                else if (type == kTypeDeletion)
                {
                    result.found = true;
                    result.deleted = true;
                }
            }
        }

        if (cache_handle != nullptr)
        {
            options_.block_cache->Release(cache_handle);
        }

        return result;
    }
    return LookupResult{};
}

void SSTable::DumpToMap(std::map<std::string, LookupResult> &out_map) const
{
    std::unique_ptr<Block::Iterator> iiter(index_block_->NewIterator());
    for (iiter->Seek(""); iiter->Valid(); iiter->Next())
    {
        std::string_view handle = iiter->value();
        uint64_t         block_offset, block_size;
        const char      *p = handle.data();
        p = GetVarint64Ptr(p, handle.data() + handle.size(), &block_offset);
        p = GetVarint64Ptr(p, handle.data() + handle.size(), &block_size);

        auto block_res = ReadBlock(block_offset, block_size);
        if (block_res)
        {
            std::unique_ptr<Block>           block = std::move(*block_res);
            std::unique_ptr<Block::Iterator> biter(block->NewIterator());
            for (biter->Seek(""); biter->Valid(); biter->Next())
            {
                std::string_view raw = biter->value();
                if (!raw.empty())
                {
                    ValueType    type = static_cast<ValueType>(raw[0]);
                    LookupResult res;
                    res.found = true;
                    if (type == kTypeValue)
                    {
                        res.deleted = false;
                        res.value = std::string(raw.substr(1));
                    }
                    else
                    {
                        res.deleted = true;
                    }
                    out_map[std::string(biter->key())] = res;
                }
            }
        }
    }
}

// Manager
SSTableManager::SSTableManager(IOContext &io_ctx, const TableBuilderOptions &opt) : io_ctx_(io_ctx), options_(opt) {}

std::expected<LookupResult, Error> SSTableManager::Get(const ReadOptions &options, std::string_view key,
                                                       uint64_t seq) noexcept
{
    std::shared_lock<std::shared_mutex> lock(rw_lock_);
    for (const auto &level : levels_)
    {
        for (const auto &sst : level)
        {
            auto res = sst->Get(options, key, seq);
            if (!res) return std::unexpected(res.error());
            if (res.value().found)
            {
                return res;
            }
        }
    }
    return LookupResult{};
}

void SSTableManager::AddSSTable(int level, std::string filepath)
{
    auto table_res = SSTable::Open(options_, io_ctx_, filepath);
    if (table_res)
    {
        std::unique_lock<std::shared_mutex> lock(rw_lock_);
        if (levels_.size() <= level) levels_.resize(level + 1);
        levels_[level].push_back(std::move(*table_res));
    }
}

std::vector<SSTable *> SSTableManager::GetLevelSSTables(int level)
{
    std::shared_lock<std::shared_mutex> lock(rw_lock_);
    std::vector<SSTable *>              result;
    if (level < levels_.size())
    {
        for (const auto &table : levels_[level])
        {
            result.push_back(table.get());
        }
    }
    return result;
}

void SSTableManager::ReplaceLevelSSTables(int level, const std::vector<SSTable *> &old_tables,
                                          std::vector<std::string> new_filepaths)
{
    std::vector<std::unique_ptr<SSTable>> new_ssts;
    for (const auto &path : new_filepaths)
    {
        auto table_res = SSTable::Open(options_, io_ctx_, path);
        if (table_res)
        {
            new_ssts.push_back(std::move(*table_res));
        }
    }

    std::unique_lock<std::shared_mutex> lock(rw_lock_);
    if (levels_.size() <= level) levels_.resize(level + 1);

    auto &level_tables = levels_[level];
    for (auto old_tbl : old_tables)
    {
        level_tables.erase(
            std::remove_if(level_tables.begin(), level_tables.end(),
                           [old_tbl](const std::unique_ptr<SSTable> &tbl) { return tbl.get() == old_tbl; }),
            level_tables.end());
    }

    for (auto &sst : new_ssts)
    {
        level_tables.push_back(std::move(sst));
    }
}

}  // namespace storage
}  // namespace zujan

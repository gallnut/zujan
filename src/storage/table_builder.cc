#include "table_builder.h"

#include <cassert>

#include "block_builder.h"
#include "coding.h"
#include "filter_block.h"

namespace zujan
{
namespace storage
{

struct TableBuilder::Rep
{
    TableBuilderOptions                 options;
    IOContext                          &io_ctx;
    int                                 fd;
    uint64_t                            offset;
    uint64_t                            num_entries;
    std::unique_ptr<BlockBuilder>       data_block;
    std::unique_ptr<BlockBuilder>       index_block;
    std::unique_ptr<FilterBlockBuilder> filter_block;
    std::string                         last_key;
    bool                                pending_index_entry;
    std::string                         pending_index_handle;  // string-encoded BlockHandle
    bool                                closed;
    BlockBuilderOptions                 block_options;

    Rep(const TableBuilderOptions &opt, IOContext &io, int f)
        : options(opt), io_ctx(io), fd(f), offset(0), num_entries(0), pending_index_entry(false), closed(false)
    {
        block_options.block_restart_interval = opt.block_restart_interval;
        data_block = std::make_unique<BlockBuilder>(&block_options);
        index_block = std::make_unique<BlockBuilder>(&block_options);
        if (opt.filter_policy != nullptr)
        {
            filter_block = std::make_unique<FilterBlockBuilder>(opt.filter_policy.get());
        }
    }
};

TableBuilder::TableBuilder(const TableBuilderOptions &options, IOContext &io_ctx, int fd)
    : rep_(std::make_unique<Rep>(options, io_ctx, fd))
{
}

TableBuilder::~TableBuilder() = default;

void TableBuilder::ChangeOptions(const TableBuilderOptions &options)
{
    assert(rep_->num_entries == 0);
    rep_->options = options;
    rep_->block_options.block_restart_interval = options.block_restart_interval;
    if (options.filter_policy != nullptr)
    {
        rep_->filter_block = std::make_unique<FilterBlockBuilder>(options.filter_policy.get());
    }
    else
    {
        rep_->filter_block.reset();
    }
}

void TableBuilder::Add(std::string_view key, std::string_view value)
{
    assert(!rep_->closed);
    if (rep_->pending_index_entry)
    {
        // The previous data block was flushed, and we need to add an index entry
        // to point to it. The key for the index entry can be any string between
        // the last key in the previous block and the first key in the current
        // block. For simplicity, we just use the first key in the current block.
        rep_->index_block->Add(key, rep_->pending_index_handle);
        rep_->pending_index_entry = false;
    }

    if (rep_->filter_block != nullptr)
    {
        rep_->filter_block->AddKey(key);
    }

    rep_->last_key = key;
    rep_->num_entries++;
    rep_->data_block->Add(key, value);

    const size_t estimated_block_size = rep_->data_block->CurrentSizeEstimate();
    if (estimated_block_size >= rep_->options.block_size)
    {
        Flush();
    }
}

void TableBuilder::Flush()
{
    assert(!rep_->closed);
    if (rep_->data_block->empty())
    {
        return;
    }

    std::string_view raw = rep_->data_block->Finish();

    // Write block to file
    auto res = rep_->io_ctx.WriteAligned(rep_->fd, std::span<const char>(raw.data(), raw.size()), rep_->offset);
    // Simplification: Not full error handling here across WriteAligned loops
    if (res)
    {
        uint64_t written = *res;
        // Encode block handle for the index
        std::string handle_encoding;
        PutVarint64(&handle_encoding, rep_->offset);
        PutVarint64(&handle_encoding, written);

        rep_->offset += written;
        rep_->pending_index_handle = handle_encoding;
        rep_->pending_index_entry = true;
    }

    rep_->data_block->Reset();

    // If using filter block, let it know we started a new block
    if (rep_->filter_block != nullptr)
    {
        rep_->filter_block->StartBlock(rep_->offset);
    }
}

std::expected<void, Error> TableBuilder::Finish()
{
    assert(!rep_->closed);
    Flush();
    rep_->closed = true;

    std::string filter_block_handle;
    if (rep_->filter_block != nullptr)
    {
        std::string_view filter_content = rep_->filter_block->Finish();
        auto             res = rep_->io_ctx.WriteAligned(
            rep_->fd, std::span<const char>(filter_content.data(), filter_content.size()), rep_->offset);
        if (!res) return std::unexpected(res.error());

        PutVarint64(&filter_block_handle, rep_->offset);
        PutVarint64(&filter_block_handle, filter_content.size());
        rep_->offset += filter_content.size();
    }

    if (rep_->pending_index_entry)
    {
        // Add the final index entry. Use last_key.
        rep_->index_block->Add(rep_->last_key, rep_->pending_index_handle);
        rep_->pending_index_entry = false;
    }

    std::string_view index_content = rep_->index_block->Finish();
    auto             idx_res = rep_->io_ctx.WriteAligned(
        rep_->fd, std::span<const char>(index_content.data(), index_content.size()), rep_->offset);
    if (!idx_res) return std::unexpected(idx_res.error());

    std::string index_block_handle;
    PutVarint64(&index_block_handle, rep_->offset);
    PutVarint64(&index_block_handle, index_content.size());
    rep_->offset += index_content.size();

    // Footer format:
    // filter_handle (varint64, varint64)
    // index_handle (varint64, varint64)
    // padding to 40 bytes
    // magic number (8 bytes)
    std::string footer;
    footer.append(filter_block_handle);
    footer.append(index_block_handle);
    footer.resize(2 * 20, 0);  // pad to 40 bytes (max 2 BlockHandles)

    uint64_t magic = 0xdb4775248b80fb57ull;
    PutFixed64(&footer, magic);

    auto footer_res =
        rep_->io_ctx.WriteAligned(rep_->fd, std::span<const char>(footer.data(), footer.size()), rep_->offset);
    if (!footer_res) return std::unexpected(footer_res.error());
    rep_->offset += footer.size();

    return {};
}

uint64_t TableBuilder::NumEntries() const { return rep_->num_entries; }

uint64_t TableBuilder::FileSize() const { return rep_->offset; }

}  // namespace storage
}  // namespace zujan

#pragma once

#include <cstdint>
#include <string>
#include <string_view>

namespace zujan
{
namespace storage
{

class Block
{
public:
    /**
     * @brief Initialize the block with the specified contents.
     * @param contents The block contents
     */
    explicit Block(std::string_view contents);

    ~Block() = default;

    Block(const Block &) = delete;
    Block &operator=(const Block &) = delete;

    size_t size() const { return size_; }

    class Iterator
    {
    public:
        Iterator(const char *data, uint32_t restarts, uint32_t num_restarts);

        bool             Valid() const { return current_ < restarts_; }
        std::string_view key() const { return key_; }
        std::string_view value() const { return value_; }

        void Next();
        void Seek(std::string_view target);

    private:
        void SeekToRestartPoint(uint32_t index);
        void ParseNextKey();

        const char      *data_;
        uint32_t         restarts_;
        uint32_t         num_restarts_;
        uint32_t         current_;
        uint32_t         restart_index_;
        std::string      key_;
        std::string_view value_;
    };

    Iterator *NewIterator() const;

private:
    uint32_t NumRestarts() const;

    const char *data_;
    size_t      size_;
    uint32_t    restart_offset_;  // Offset in data_ of restart array
};

}  // namespace storage
}  // namespace zujan

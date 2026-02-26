#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace zujan
{
namespace storage
{

struct BlockBuilderOptions
{
    int block_restart_interval = 16;
};

class BlockBuilder
{
public:
    explicit BlockBuilder(const BlockBuilderOptions *options);

    /**
     * @brief Reset the contents as if the BlockBuilder was just constructed.
     */
    void Reset();

    /**
     * @brief Add a key-value pair to the block.
     * REQUIRES: Finish() has not been called since the last call to Reset().
     * REQUIRES: key is larger than any previously added key.
     *
     * @param key The key to add
     * @param value The value to add
     */
    void Add(std::string_view key, std::string_view value);

    /**
     * @brief Finish building the block and return a slice that refers to the
     * block contents. The returned slice will remain valid for the
     * lifetime of this builder or until Reset() is called.
     *
     * @return std::string_view The finalized block contents
     */
    std::string_view Finish();

    /**
     * @brief Returns an estimate of the current (uncompressed) size of the block we are building.
     * @return size_t The estimated block size in bytes
     */
    size_t CurrentSizeEstimate() const;

    /**
     * @brief Return true if no entries have been added since the last Reset()
     * @return bool True if empty, false otherwise
     */
    bool empty() const { return buffer_.empty(); }

private:
    const BlockBuilderOptions *options_;
    std::string                buffer_;    // Destination buffer
    std::vector<uint32_t>      restarts_;  // Restart points
    int                        counter_;   // Number of entries emitted since restart
    bool                       finished_;  // Has Finish() been called?
    std::string                last_key_;

    // No copying allowed
    BlockBuilder(const BlockBuilder &);
    void operator=(const BlockBuilder &);
};

}  // namespace storage
}  // namespace zujan

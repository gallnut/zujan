#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "bloom_filter.h"

namespace zujan
{
namespace storage
{

class FilterBlockBuilder
{
public:
    explicit FilterBlockBuilder(const BloomFilterPolicy *policy);

    /**
     * @brief Start a new filter block
     * @param block_offset The current block offset
     */
    void StartBlock(uint64_t block_offset);

    /**
     * @brief Add a key to the current filter block
     * @param key The key to add
     */
    void AddKey(std::string_view key);

    /**
     * @brief Finish building the filter block
     * @return std::string_view The generated filter block contents
     */
    std::string_view Finish();

private:
    void GenerateFilter();

    const BloomFilterPolicy *policy_;
    std::string              keys_;    // Flattened key contents
    std::vector<size_t>      start_;   // Starting index in keys_ of each key
    std::string              result_;  // Filter data computed so far
    std::vector<uint32_t>    filter_offsets_;

    // No copying allowed
    FilterBlockBuilder(const FilterBlockBuilder &);
    void operator=(const FilterBlockBuilder &);
};

class FilterBlockReader
{
public:
    /**
     * @brief Construct a FilterBlockReader
     * REQUIRES: "contents" and *policy must stay live while *this is live.
     *
     * @param policy The BloomFilterPolicy used to generate the filter
     * @param contents The filter block contents
     */
    FilterBlockReader(const BloomFilterPolicy *policy, std::string_view contents);

    /**
     * @brief Check if a key may match the filter at the given block offset
     * @param block_offset The block offset to check
     * @param key The key to look for
     * @return bool True if the key may be present, false if definitely not
     */
    bool KeyMayMatch(uint64_t block_offset, std::string_view key);

private:
    const BloomFilterPolicy *policy_;
    const char              *data_;     // Pointer to filter data (at block-start)
    const char              *offset_;   // Pointer to beginning of offset array (at block-end)
    size_t                   num_;      // Number of entries in offset array
    size_t                   base_lg_;  // Encoding parameter (see kFilterBaseLg in .cc)
};

}  // namespace storage
}  // namespace zujan

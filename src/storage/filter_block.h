#pragma once

#include "bloom_filter.h"
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace zujan {
namespace storage {

class FilterBlockBuilder {
public:
  explicit FilterBlockBuilder(const BloomFilterPolicy *policy);

  void StartBlock(uint64_t block_offset);
  void AddKey(std::string_view key);
  std::string_view Finish();

private:
  void GenerateFilter();

  const BloomFilterPolicy *policy_;
  std::string keys_;          // Flattened key contents
  std::vector<size_t> start_; // Starting index in keys_ of each key
  std::string result_;        // Filter data computed so far
  std::vector<uint32_t> filter_offsets_;

  // No copying allowed
  FilterBlockBuilder(const FilterBlockBuilder &);
  void operator=(const FilterBlockBuilder &);
};

class FilterBlockReader {
public:
  // REQUIRES: "contents" and *policy must stay live while *this is live.
  FilterBlockReader(const BloomFilterPolicy *policy, std::string_view contents);
  bool KeyMayMatch(uint64_t block_offset, std::string_view key);

private:
  const BloomFilterPolicy *policy_;
  const char *data_;   // Pointer to filter data (at block-start)
  const char *offset_; // Pointer to beginning of offset array (at block-end)
  size_t num_;         // Number of entries in offset array
  size_t base_lg_;     // Encoding parameter (see kFilterBaseLg in .cc)
};

} // namespace storage
} // namespace zujan

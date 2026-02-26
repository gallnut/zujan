#pragma once
#include <string>
#include <string_view>
#include <vector>

namespace zujan
{
namespace storage
{

class BloomFilterPolicy
{
public:
    explicit BloomFilterPolicy(int bits_per_key);
    ~BloomFilterPolicy() = default;

    // Name of the filter policy. Used for compatibility checks.
    const char *Name() const;

    // Append a filter representing a set of keys to the dst string.
    // The given keys must be sorted and unique.
    void CreateFilter(const std::vector<std::string_view> &keys, std::string *dst) const;

    // Return true if the key may be in the filter, false if it's definitely not.
    bool KeyMayMatch(std::string_view key, std::string_view bloom_filter) const;

private:
    size_t bits_per_key_;
    size_t k_;  // Number of hash functions
};

}  // namespace storage
}  // namespace zujan

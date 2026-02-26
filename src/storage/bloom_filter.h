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

    /**
     * @brief Name of the filter policy. Used for compatibility checks.
     * @return const char* The policy name
     */
    const char *Name() const;

    /**
     * @brief Append a filter representing a set of keys to the dst string.
     * The given keys must be sorted and unique.
     *
     * @param keys The set of keys to add to the filter
     * @param dst The destination string to append the filter to
     */
    void CreateFilter(const std::vector<std::string_view> &keys, std::string *dst) const;

    /**
     * @brief Return true if the key may be in the filter, false if it's definitely not.
     *
     * @param key The key to check
     * @param bloom_filter The filter string to check against
     * @return bool True if key may match, false if it definitely does not
     */
    bool KeyMayMatch(std::string_view key, std::string_view bloom_filter) const;

private:
    size_t bits_per_key_;
    size_t k_;  // Number of hash functions
};

}  // namespace storage
}  // namespace zujan

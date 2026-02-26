#include "bloom_filter.h"

#include "hash.h"

namespace zujan
{
namespace storage
{

static uint32_t BloomHash(std::string_view key) { return Hash(key.data(), key.size(), 0xbc9f1d34); }

BloomFilterPolicy::BloomFilterPolicy(int bits_per_key) : bits_per_key_(bits_per_key)
{
    // We intentionally round down to reduce probing cost a little bit
    k_ = static_cast<size_t>(bits_per_key * 0.69);  // 0.69 =~ ln(2)
    if (k_ < 1) k_ = 1;
    if (k_ > 30) k_ = 30;
}

const char *BloomFilterPolicy::Name() const { return "zujan.BuiltinBloomFilter2"; }

void BloomFilterPolicy::CreateFilter(const std::vector<std::string_view> &keys, std::string *dst) const
{
    // Compute bloom filter size (in bytes) needed
    size_t bits = keys.size() * bits_per_key_;
    if (bits < 64) bits = 64;  // min sizes
    size_t bytes = (bits + 7) / 8;
    bits = bytes * 8;  // rounded bits

    const size_t init_size = dst->size();
    dst->resize(init_size + bytes, 0);
    dst->push_back(static_cast<char>(k_));  // Remember # of probes

    char *array = &(*dst)[init_size];
    for (size_t i = 0; i < keys.size(); i++)
    {
        // Use double-hashing to generate a sequence of hash values
        uint32_t       h = BloomHash(keys[i]);
        const uint32_t delta = (h >> 17) | (h << 15);
        for (size_t j = 0; j < k_; j++)
        {
            const uint32_t bitpos = h % bits;
            array[bitpos / 8] |= (1 << (bitpos % 8));
            h += delta;
        }
    }
}

bool BloomFilterPolicy::KeyMayMatch(std::string_view key, std::string_view bloom_filter) const
{
    const size_t len = bloom_filter.size();
    if (len < 2) return false;

    const char  *array = bloom_filter.data();
    const size_t bits = (len - 1) * 8;  // Sub 1 for k_
    const size_t k = array[len - 1];    // number of probes
    if (k > 30)
    {
        // Reserved for potentially new encodings
        return true;
    }

    uint32_t       h = BloomHash(key);
    const uint32_t delta = (h >> 17) | (h << 15);
    for (size_t j = 0; j < k; j++)
    {
        const uint32_t bitpos = h % bits;
        if ((array[bitpos / 8] & (1 << (bitpos % 8))) == 0) return false;
        h += delta;
    }
    return true;
}

}  // namespace storage
}  // namespace zujan

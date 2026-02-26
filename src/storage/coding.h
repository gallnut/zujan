#pragma once

#include <cstdint>
#include <string>

namespace zujan
{
namespace storage
{

/**
 * @brief Standard lower-level encoding/decoding implementations.
 * These are essential for keeping our data structures and SSTables compact
 * by taking advantage of variable-length integer encoding.
 */

void PutFixed32(std::string *dst, uint32_t value);
void PutFixed64(std::string *dst, uint64_t value);

void EncodeFixed32(char *dst, uint32_t value);
void EncodeFixed64(char *dst, uint64_t value);

inline uint32_t DecodeFixed32(const char *ptr)
{
    uint32_t result;
    // Use memcpy to avoid unaligned memory access
    __builtin_memcpy(&result, ptr, sizeof(result));
    return result;
}

inline uint64_t DecodeFixed64(const char *ptr)
{
    uint64_t result;
    __builtin_memcpy(&result, ptr, sizeof(result));
    return result;
}

void PutVarint32(std::string *dst, uint32_t value);
void PutVarint64(std::string *dst, uint64_t value);

char *EncodeVarint32(char *dst, uint32_t value);
char *EncodeVarint64(char *dst, uint64_t value);

const char        *GetVarint32PtrFallback(const char *p, const char *limit, uint32_t *value);
inline const char *GetVarint32Ptr(const char *p, const char *limit, uint32_t *value)
{
    if (p < limit)
    {
        uint32_t result = *(reinterpret_cast<const unsigned char *>(p));
        if ((result & 128) == 0)
        {
            *value = result;
            return p + 1;
        }
    }
    return GetVarint32PtrFallback(p, limit, value);
}

const char *GetVarint64Ptr(const char *p, const char *limit, uint64_t *value);

bool GetVarint32(std::string_view *input, uint32_t *value);
bool GetVarint64(std::string_view *input, uint64_t *value);

}  // namespace storage
}  // namespace zujan

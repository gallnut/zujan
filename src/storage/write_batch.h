#pragma once

#include <expected>
#include <string>
#include <string_view>

#include "status.h"

namespace zujan
{
namespace storage
{

class WriteBatchInternal;

class WriteBatch
{
public:
    WriteBatch();
    ~WriteBatch();
    WriteBatch(const WriteBatch&) = default;
    WriteBatch& operator=(const WriteBatch&) = default;

    void Put(std::string_view key, std::string_view value);
    void Delete(std::string_view key);
    void Clear();

    size_t ApproximateSize() const;

    class Handler
    {
    public:
        virtual ~Handler();
        virtual void Put(std::string_view key, std::string_view value) = 0;
        virtual void Delete(std::string_view key) = 0;
    };

    std::expected<void, Error> Iterate(Handler* handler) const;

private:
    friend class WriteBatchInternal;
    std::string rep_;
};

}  // namespace storage
}  // namespace zujan

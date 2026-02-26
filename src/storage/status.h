#pragma once

#include <string>

namespace zujan
{
namespace storage
{

enum class ErrorCode
{
    OK = 0,
    NotFound,
    Corruption,
    NotSupported,
    InvalidArgument,
    IOError,
    SystemError  // Mapped from OS errno
};

struct Error
{
    ErrorCode   code;
    std::string message;
};

}  // namespace storage
}  // namespace zujan

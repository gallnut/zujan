#pragma once

#include <string>

namespace zujan
{
namespace storage
{

/**
 * @brief Represents different types of errors that can occur in the storage engine
 */
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

/**
 * @brief A standard error structure wrapping an error code and a descriptive message
 */
struct Error
{
    ErrorCode   code;
    std::string message;
};

}  // namespace storage
}  // namespace zujan

#pragma once

#include <chrono>
#include <condition_variable>
#include <format>
#include <mutex>
#include <source_location>
#include <string>
#include <thread>
#include <vector>

namespace zujan
{
namespace utils
{

/**
 * @brief Log levels for the asynchronous logger
 */
enum class LogLevel
{
    DEBUG,
    INFO,
    WARN,
    ERROR,
    FATAL
};

/**
 * @brief A log message containing metadata and the formatted string
 */
struct LogMessage
{
    LogLevel                              level;
    std::chrono::system_clock::time_point timestamp;
    std::thread::id                       thread_id;
    std::string                           file;
    uint32_t                              line;
    std::string                           message;
};

/**
 * @brief Asynchronous logger singleton that buffers and flushes logs in a background thread
 */
class AsyncLogger
{
public:
    static AsyncLogger& GetInstance()
    {
        static AsyncLogger instance;
        return instance;
    }

    /**
     * @brief Start the background logging thread
     */
    void Start();

    /**
     * @brief Stop the background thread and flush remaining logs
     */
    void Stop();

    /**
     * @brief Push a log message to the active buffer
     * @param level The severity log level
     * @param loc The source code location
     * @param msg The formatted message
     */
    void PushLog(LogLevel level, const std::source_location& loc, std::string&& msg);

private:
    AsyncLogger() = default;
    ~AsyncLogger() { Stop(); }

    void BackgroundThreadFunc();

    std::vector<LogMessage> active_buffer_;
    std::vector<LogMessage> flush_buffer_;

    std::mutex              mutex_;
    std::condition_variable cv_;
    bool                    running_{false};
    std::thread             background_thread_;
};

// Formatting helpers
template <typename... Args>
void Log(LogLevel level, const std::source_location& loc, std::format_string<Args...> fmt, Args&&... args)
{
    AsyncLogger::GetInstance().PushLog(level, loc, std::format(fmt, std::forward<Args>(args)...));
}

}  // namespace utils
}  // namespace zujan

// Macros for easy logging
#define Z_LOG_DEBUG(...) \
    ::zujan::utils::Log(::zujan::utils::LogLevel::DEBUG, std::source_location::current(), __VA_ARGS__)

#define Z_LOG_INFO(...) \
    ::zujan::utils::Log(::zujan::utils::LogLevel::INFO, std::source_location::current(), __VA_ARGS__)

#define Z_LOG_WARN(...) \
    ::zujan::utils::Log(::zujan::utils::LogLevel::WARN, std::source_location::current(), __VA_ARGS__)

#define Z_LOG_ERROR(...) \
    ::zujan::utils::Log(::zujan::utils::LogLevel::ERROR, std::source_location::current(), __VA_ARGS__)

#define Z_LOG_FATAL(...) \
    ::zujan::utils::Log(::zujan::utils::LogLevel::FATAL, std::source_location::current(), __VA_ARGS__)

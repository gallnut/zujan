#include "logger.h"

#include <print>

namespace zujan
{
namespace utils
{

constexpr std::string_view LevelToString(LogLevel level)
{
    switch (level)
    {
        case LogLevel::DEBUG:
            return "DEBUG";
        case LogLevel::INFO:
            return "INFO ";
        case LogLevel::WARN:
            return "WARN ";
        case LogLevel::ERROR:
            return "ERROR";
        case LogLevel::FATAL:
            return "FATAL";
        default:
            return "UNKNOWN";
    }
}

void AsyncLogger::Start()
{
    std::lock_guard<std::mutex> lock(mutex_);
    if (running_) return;

    running_ = true;
    background_thread_ = std::thread(&AsyncLogger::BackgroundThreadFunc, this);
}

void AsyncLogger::Stop()
{
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!running_) return;
        running_ = false;
        cv_.notify_one();
    }
    if (background_thread_.joinable())
    {
        background_thread_.join();
    }
}

void AsyncLogger::PushLog(LogLevel level, const std::source_location& loc, std::string&& msg)
{
    std::lock_guard<std::mutex> lock(mutex_);
    if (!running_)
    {
        // Fallback to synchronous print if logger is not running
        std::println("[{}] [{}] {}:{} - {}", LevelToString(level), std::this_thread::get_id(), loc.file_name(),
                     loc.line(), msg);
        return;
    }

    active_buffer_.push_back(LogMessage{.level = level,
                                        .timestamp = std::chrono::system_clock::now(),
                                        .thread_id = std::this_thread::get_id(),
                                        .file = loc.file_name(),
                                        .line = loc.line(),
                                        .message = std::move(msg)});

    if (active_buffer_.size() > 1000)
    {
        cv_.notify_one();
    }
}

void AsyncLogger::BackgroundThreadFunc()
{
    while (true)
    {
        {
            std::unique_lock<std::mutex> lock(mutex_);
            cv_.wait_for(lock, std::chrono::milliseconds(50),
                         [this]() { return !running_ || !active_buffer_.empty(); });

            active_buffer_.swap(flush_buffer_);
            if (!running_ && flush_buffer_.empty())
            {
                break;
            }
        }

        for (const auto& log : flush_buffer_)
        {
            auto    ms = std::chrono::duration_cast<std::chrono::milliseconds>(log.timestamp.time_since_epoch()) % 1000;
            auto    time_t = std::chrono::system_clock::to_time_t(log.timestamp);
            std::tm tm;
            localtime_r(&time_t, &tm);

            std::string time_str = std::format("{:04}-{:02}-{:02} {:02}:{:02}:{:02}.{:03}", tm.tm_year + 1900,
                                               tm.tm_mon + 1, tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec, ms.count());

            std::string_view file_view(log.file);
            auto             last_slash = file_view.find_last_of("/\\");
            if (last_slash != std::string_view::npos)
            {
                file_view = file_view.substr(last_slash + 1);
            }

            std::println("[{}] [{}] [{}] {}:{} - {}", time_str, LevelToString(log.level), log.thread_id, file_view,
                         log.line, log.message);
        }
        flush_buffer_.clear();
    }
}

}  // namespace utils
}  // namespace zujan

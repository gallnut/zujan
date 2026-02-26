#pragma once

#include <cstdint>
#include <expected>
#include <memory>
#include <string>

#include "storage/io.h"

namespace zujan
{
namespace consensus
{

/**
 * @brief A lightweight persistence component for Raft's hard state.
 * Responsible for durably storing `currentTerm` and `votedFor`
 * to prevent split-brain after crashes.
 */
class RaftMeta
{
public:
    static std::expected<std::unique_ptr<RaftMeta>, storage::Error> Open(storage::IOContext &io_ctx,
                                                                         const std::string  &filepath);

    ~RaftMeta();

    RaftMeta(const RaftMeta &) = delete;
    RaftMeta &operator=(const RaftMeta &) = delete;

    /**
     * @brief Persist the current term and voted_for value to disk
     * @param current_term The current Raft term
     * @param voted_for The ID of the node voted for in this term
     * @return std::expected<void, storage::Error> Success or error status
     */
    std::expected<void, storage::Error> Save(uint64_t current_term, uint64_t voted_for);

    uint64_t CurrentTerm() const { return current_term_; }
    uint64_t VotedFor() const { return voted_for_; }

private:
    RaftMeta(storage::IOContext &io_ctx, int fd, const std::string &filepath);
    std::expected<void, storage::Error> Recover();

    storage::IOContext &io_ctx_;
    int                 fd_;
    std::string         filepath_;

    uint64_t current_term_{0};
    uint64_t voted_for_{0};  // 0 = null / unvoted
};

}  // namespace consensus
}  // namespace zujan

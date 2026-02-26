#pragma once

#include <atomic>
#include <chrono>
#include <future>
#include <map>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include "event.h"
#include "raft_log.h"
#include "raft_meta.h"
#include "storage/lsm_store.h"
#include "storage/posix_io.h"
#include "thread_safe_queue.h"
#include "zujan.grpc.pb.h"

// Forward declarations
namespace grpc
{
class Server;
}  // namespace grpc

namespace zujan
{
namespace consensus
{

class RaftServiceImpl;

enum class RaftState
{
    Follower,
    Candidate,
    Leader
};

class RaftNode
{
public:
    RaftNode(uint64_t id, const std::vector<std::string> &peer_addresses);
    ~RaftNode();

    /**
     * @brief Start the Raft node (EventLoop and gRPC Server)
     */
    void Start();

    /**
     * @brief Stop the Raft node
     */
    void Stop();

    bool IsLeader() const { return state_ == RaftState::Leader; }

    /**
     * @brief Propose a command to the state machine
     * @param key The key for the operation
     * @param value The value to insert if not a delete
     * @param is_delete Whether this proposal is a deletion
     * @return std::future<bool> Future indicating cluster-wide consensus success
     */
    std::future<bool> Propose(const std::string &key, const std::string &value, bool is_delete = false);

    std::expected<std::optional<std::string>, storage::Error> GetKV(const std::string &key);

    // Network message receivers (called by gRPC service thread)
    void OnRequestVote(const proto::RequestVoteRequest &req, std::function<void(proto::RequestVoteResponse)> cb);
    void OnAppendEntries(const proto::AppendEntriesRequest &req, std::function<void(proto::AppendEntriesResponse)> cb);

private:
    // Core Raft loop dispatching events
    void RunLoop();

    // Dedicated timer loop for tracking timeouts and pushing TimeoutEvents
    void TimerLoop();
    void ResetElectionTimeout();

    // Network broadcaster helpers
    void BroadcastRequestVote();
    void BroadcastAppendEntries();

    // Handlers for specific events
    void HandleTimeout(const TimeoutEvent &e);
    void HandleClientProposal(const ClientProposalEvent &e);
    void HandleRequestVote(const RequestVoteEvent &e);
    void HandleRequestVoteResponse(const RequestVoteResponseEvent &e);
    void HandleAppendEntries(const AppendEntriesEvent &e);
    void HandleAppendEntriesResponse(const AppendEntriesResponseEvent &e);

    // Apply committed log entries to the state machine
    void ApplyCommittedLogs();

    void BecomeFollower(uint64_t term);
    void BecomeCandidate();
    void BecomeLeader();

private:
    uint64_t                 id_;
    std::vector<std::string> peer_addresses_;
    RaftState                state_{RaftState::Follower};

    // Persistent state on all servers
    uint64_t                current_term_{0};
    std::optional<uint64_t> voted_for_;
    // std::vector<LogEntry> log_;

    // Volatile state on all servers
    uint64_t commit_index_{0};
    uint64_t last_applied_{0};

    // Volatile state on leaders
    std::vector<uint64_t> next_index_;
    std::vector<uint64_t> match_index_;

    // Volatile state on candidates
    int votes_received_{0};

    // Timer components
    std::thread                                        timer_thread_;
    std::atomic<std::chrono::steady_clock::time_point> next_timeout_;

    // Client proposal tracking
    std::map<uint64_t, std::promise<bool>> pending_proposals_;

    // Thread-safe pipeline ensuring lock-free sequential execution of the state
    // machine
    ThreadSafeQueue<RaftEvent> event_queue_;

    std::thread       loop_thread_;
    std::atomic<bool> running_{false};

    // Storage and Persistence
    std::unique_ptr<storage::PosixIOContext> io_ctx_;
    std::unique_ptr<RaftMeta>                raft_meta_;
    std::unique_ptr<RaftLog>                 raft_log_;
    std::unique_ptr<storage::LSMStore>       state_machine_;

    // gRPC Networking
    std::unique_ptr<RaftServiceImpl>                       grpc_service_;
    std::unique_ptr<grpc::Service>                         kv_service_;
    std::unique_ptr<grpc::Server>                          grpc_server_;
    std::vector<std::unique_ptr<proto::RaftService::Stub>> peers_;
};

}  // namespace consensus
}  // namespace zujan

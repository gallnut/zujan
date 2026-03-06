#include "raft.h"

#include <grpcpp/grpcpp.h>

#include <random>

#include "raft_service.h"
#include "utils/logger.h"

namespace zujan
{
namespace consensus
{

RaftNode::RaftNode(uint64_t id, const std::vector<std::string> &peer_addresses)
    : id_(id), peer_addresses_(peer_addresses)
{
    // Initialize storage
    io_ctx_ = std::make_unique<storage::PosixIOContext>();
    auto init_res = io_ctx_->Init();
    if (!init_res)
    {
        Z_LOG_ERROR("Failed to init PosixIOContext: {}", init_res.error().message);
    }

    std::string meta_file = "raft_meta_" + std::to_string(id_) + ".dat";
    std::string log_file = "raft_log_" + std::to_string(id_) + ".dat";

    auto meta_res = RaftMeta::Open(*io_ctx_, meta_file);
    if (meta_res) raft_meta_ = std::move(*meta_res);

    auto log_res = RaftLog::Open(*io_ctx_, log_file);
    if (log_res) raft_log_ = std::move(*log_res);

    if (raft_meta_)
    {
        current_term_ = raft_meta_->CurrentTerm();
        uint64_t vf = raft_meta_->VotedFor();
        if (vf != 0) voted_for_ = vf;
    }

    std::string lsm_dir = "raft_lsm_" + std::to_string(id_);
    auto        sm_res = storage::LSMStore::Open(lsm_dir);
    if (!sm_res)
    {
        Z_LOG_ERROR("Failed to open LSMStore state machine: {}", sm_res.error().message);
    }
    else
    {
        state_machine_ = std::move(*sm_res);
    }

    // Initialize gRPC stubs for peers
    for (const auto &addr : peer_addresses_)
    {
        auto channel = grpc::CreateChannel(addr, grpc::InsecureChannelCredentials());
        peers_.push_back(proto::RaftService::NewStub(channel));
    }
}

RaftNode::~RaftNode() { Stop(); }

void RaftNode::Start()
{
    bool expected = false;
    if (running_.compare_exchange_strong(expected, true))
    {
        ResetElectionTimeout();
        loop_thread_ = std::thread(&RaftNode::RunLoop, this);
        timer_thread_ = std::thread(&RaftNode::TimerLoop, this);

        // Bind to 0.0.0.0:50050 + id_ (e.g. 50051 for id 1)
        std::string server_address("0.0.0.0:" + std::to_string(50050 + id_));

        grpc_service_ = std::make_unique<RaftServiceImpl>(this);
        kv_service_ = std::make_unique<KVServiceImpl>(this);
        grpc::ServerBuilder builder;
        builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
        builder.RegisterService(grpc_service_.get());
        builder.RegisterService(kv_service_.get());
        grpc_server_ = builder.BuildAndStart();

        Z_LOG_INFO("RaftNode [{}] gRPC listening on {}", id_, server_address);
    }
}

void RaftNode::Stop()
{
    if (running_.exchange(false))
    {
        if (grpc_server_)
        {
            grpc_server_->Shutdown();
        }
        event_queue_.Stop();
        if (loop_thread_.joinable())
        {
            loop_thread_.join();
        }
        if (timer_thread_.joinable())
        {
            timer_thread_.join();
        }
    }
}

void RaftNode::RunLoop()
{
    while (running_)
    {
        auto event_opt = event_queue_.Pop();
        if (!event_opt)
        {
            break;  // Queue stopped
        }

        // Process the event serially without any global locks!
        std::visit(
            [this](auto &&arg)
            {
                using T = std::decay_t<decltype(arg)>;
                if constexpr (std::is_same_v<T, TimeoutEvent>)
                {
                    HandleTimeout(arg);
                }
                else if constexpr (std::is_same_v<T, ClientProposalEvent>)
                {
                    HandleClientProposal(arg);
                }
                else if constexpr (std::is_same_v<T, RequestVoteEvent>)
                {
                    HandleRequestVote(arg);
                }
                else if constexpr (std::is_same_v<T, RequestVoteResponseEvent>)
                {
                    HandleRequestVoteResponse(arg);
                }
                else if constexpr (std::is_same_v<T, AppendEntriesEvent>)
                {
                    HandleAppendEntries(arg);
                }
                else if constexpr (std::is_same_v<T, AppendEntriesResponseEvent>)
                {
                    HandleAppendEntriesResponse(arg);
                }
                else if constexpr (std::is_same_v<T, DiskWriteCompleteEvent>)
                {
                    HandleDiskWriteComplete(arg);
                }
            },
            *event_opt);
    }
}

std::future<bool> RaftNode::Propose(const std::string &key, const std::string &value, bool is_delete)
{
    // Push to queue, completely unblocking the caller immediately.
    auto              promise = std::make_shared<std::promise<bool>>();
    std::future<bool> future = promise->get_future();

    ClientProposalEvent e;
    e.key = key;
    e.value = value;
    e.is_delete = is_delete;
    e.promise = promise;
    event_queue_.Push(RaftEvent(std::move(e)));
    return future;
}

std::expected<std::optional<std::string>, storage::Error> RaftNode::GetKV(const std::string &key)
{
    if (state_machine_)
    {
        storage::ReadOptions ropt;
        return state_machine_->Get(ropt, key);
    }
    return std::unexpected(storage::Error{storage::ErrorCode::NotFound, "No statemachine"});
}

void RaftNode::OnRequestVote(const proto::RequestVoteRequest &req, std::function<void(proto::RequestVoteResponse)> cb)
{
    RequestVoteEvent e;
    e.request = req;
    e.callback = std::move(cb);
    event_queue_.Push(RaftEvent(std::move(e)));
}

void RaftNode::OnAppendEntries(const proto::AppendEntriesRequest                &req,
                               std::function<void(proto::AppendEntriesResponse)> cb)
{
    AppendEntriesEvent e;
    e.request = req;
    e.callback = std::move(cb);
    event_queue_.Push(RaftEvent(std::move(e)));
}

// Handler Stubs
void RaftNode::HandleTimeout(const TimeoutEvent &e)
{
    if (e.type == TimeoutEvent::Election)
    {
        Z_LOG_INFO("Node {} election timeout! Becoming candidate for term {}", id_, current_term_ + 1);
        BecomeCandidate();
    }
    else
    {
        // Heartbeat timeout => Send AppendEntries
        BroadcastAppendEntries();
    }
}

void RaftNode::HandleRequestVoteResponse(const RequestVoteResponseEvent &e)
{
    if (state_ != RaftState::Candidate || !e.rpc_success)
    {
        return;  // Old response or failed RPC
    }

    if (e.response.term() > current_term_)
    {
        // Step down
        BecomeFollower(e.response.term());
        return;
    }

    if (e.response.term() == current_term_ && e.response.vote_granted())
    {
        votes_received_++;
        // Cluster size = peers + self
        int majority = (peer_addresses_.size() + 1) / 2 + 1;
        if (votes_received_ >= majority)
        {
            Z_LOG_INFO("Node {} won election for term {}!", id_, current_term_);
            BecomeLeader();
        }
    }
}

void RaftNode::HandleAppendEntriesResponse(const AppendEntriesResponseEvent &e)
{
    if (state_ != RaftState::Leader || !e.rpc_success) return;

    if (e.response.term() > current_term_)
    {
        BecomeFollower(e.response.term());
        return;
    }

    if (e.response.success())
    {
        if (e.sent_last_index > 0)
            Z_LOG_INFO("Leader {} got SUCCESS AppendEntries from peer {} up to index {}", id_, e.peer_id,
                       e.sent_last_index);
        if (next_index_.size() > e.peer_id && match_index_.size() > e.peer_id)
        {
            match_index_[e.peer_id] = std::max(match_index_[e.peer_id], e.sent_last_index);
            next_index_[e.peer_id] = match_index_[e.peer_id] + 1;
        }

        // Try to advance commit_index_
        if (raft_log_)
        {
            for (uint64_t N = raft_log_->LastIndex(); N > commit_index_; --N)
            {
                auto entry_res = raft_log_->Get(N);
                if (!entry_res)
                {
                    Z_LOG_ERROR("Leader {} failed to Get({}): {}", id_, N, entry_res.error().message);
                    continue;
                }
                if (entry_res->term() != current_term_)
                {
                    Z_LOG_WARN("Leader {} skipped index {} due to different term: {} != {}", id_, N, entry_res->term(),
                               current_term_);
                    continue;
                }

                int match_count = 1;  // self
                for (size_t i = 0; i < match_index_.size(); ++i)
                {
                    if (match_index_[i] >= N) match_count++;
                }

                int majority = (peer_addresses_.size() + 1) / 2 + 1;
                if (match_count >= majority)
                {
                    commit_index_ = N;
                    Z_LOG_INFO("Leader {} advanced commit_index to {}", id_, commit_index_);
                    ApplyCommittedLogs();
                    break;
                }
            }
        }
    }
    else
    {
        // Failure due to log inconsistency, decrement next_index and retry
        if (next_index_.size() > e.peer_id && next_index_[e.peer_id] > 1)
        {
            next_index_[e.peer_id]--;
            // Let the next heartbeat retry with decremented index
        }
    }
}

void RaftNode::HandleClientProposal(const ClientProposalEvent &e)
{
    if (state_ != RaftState::Leader)
    {
        if (e.promise) e.promise->set_value(false);
        return;
    }

    Z_LOG_INFO("Leader {} received client proposal: {}={}", id_, e.key, e.value);

    // 1. Prepare LogEntry
    uint64_t        next_idx = (raft_log_ ? raft_log_->LastIndex() : 0) + 1;
    proto::LogEntry entry;
    entry.set_term(current_term_);
    entry.set_index(next_idx);
    entry.set_key(e.key);
    entry.set_value(e.value);
    entry.set_type(e.is_delete ? proto::LogEntry::DELETE : proto::LogEntry::PUT);

    // 2. Append to local log (synchronously for now to guarantee persistence
    // before networking)
    if (raft_log_)
    {
        [[maybe_unused]] auto res = raft_log_->Append({entry});
    }

    // 3. Update self state
    // match_index_[self] == LastIndex

    // 4. Dispatch Network AppendEntries async
    BroadcastAppendEntries();

    if (e.promise)
    {
        pending_proposals_.emplace(next_idx, std::move(*e.promise));
    }
}

void RaftNode::HandleRequestVote(const RequestVoteEvent &e)
{
    proto::RequestVoteResponse resp;
    resp.set_vote_granted(false);

    if (e.request.term() > current_term_)
    {
        BecomeFollower(e.request.term());
    }

    if (e.request.term() == current_term_)
    {
        if (!voted_for_.has_value() || voted_for_.value() == e.request.candidate_id())
        {
            uint64_t last_log_term = raft_log_ ? raft_log_->LastTerm() : 0;
            uint64_t last_log_index = raft_log_ ? raft_log_->LastIndex() : 0;

            bool log_ok = (e.request.last_log_term() > last_log_term) ||
                          (e.request.last_log_term() == last_log_term && e.request.last_log_index() >= last_log_index);

            if (log_ok)
            {
                voted_for_ = e.request.candidate_id();
                if (raft_meta_)
                {
                    auto res = raft_meta_->Save(current_term_, *voted_for_);
                    (void)res;
                }
                resp.set_vote_granted(true);
                ResetElectionTimeout();  // Reset timer when granting vote to another
                                         // candidate
            }
        }
    }

    resp.set_term(current_term_);
    if (e.callback) e.callback(resp);
}

void RaftNode::HandleAppendEntries(const AppendEntriesEvent &e)
{
    proto::AppendEntriesResponse resp;
    resp.set_term(current_term_);
    resp.set_success(false);

    if (e.request.term() < current_term_)
    {
        if (e.callback) e.callback(resp);
        return;
    }

    if (e.request.term() > current_term_ || state_ == RaftState::Candidate)
    {
        BecomeFollower(e.request.term());
        resp.set_term(current_term_);
    }

    ResetElectionTimeout();  // Hear from legitimate leader

    if (e.request.entries_size() > 0)
    {
        Z_LOG_INFO("Node {} received {} entries from Leader {} (prev_idx={})", id_, e.request.entries_size(),
                   e.request.leader_id(), e.request.prev_log_index());
    }

    // Log Matching
    uint64_t prev_log_index = e.request.prev_log_index();
    uint64_t prev_log_term = e.request.prev_log_term();

    if (raft_log_)
    {
        bool match = false;
        if (prev_log_index == 0)
        {
            match = true;
        }
        else if (prev_log_index <= raft_log_->LastIndex())
        {
            auto entry_res = raft_log_->Get(prev_log_index);
            if (entry_res && entry_res->term() == prev_log_term)
            {
                match = true;
            }
        }

        if (match)
        {
            resp.set_success(true);

            // Resolve conflicts and append
            int      i = 0;
            uint64_t current_idx = prev_log_index + 1;
            for (; i < e.request.entries_size(); ++i, ++current_idx)
            {
                if (current_idx <= raft_log_->LastIndex())
                {
                    auto existing = raft_log_->Get(current_idx);
                    if (existing && existing->term() != e.request.entries(i).term())
                    {
                        auto tgt_res = raft_log_->TruncateFrom(current_idx);
                        (void)tgt_res;
                        break;
                    }
                }
                else
                {
                    break;
                }
            }

            if (i < e.request.entries_size())
            {
                std::vector<proto::LogEntry> to_append;
                for (; i < e.request.entries_size(); ++i)
                {
                    to_append.push_back(e.request.entries(i));
                }
                auto app_res = raft_log_->Append(to_append);
                (void)app_res;
            }

            uint64_t last_new_index = prev_log_index + e.request.entries_size();
            if (e.request.leader_commit() > commit_index_)
            {
                commit_index_ = std::min(e.request.leader_commit(), last_new_index);
                ApplyCommittedLogs();
            }
        }
    }

    if (e.callback) e.callback(resp);
}

void RaftNode::BecomeFollower(uint64_t term)
{
    state_ = RaftState::Follower;
    current_term_ = term;
    voted_for_ = std::nullopt;

    // Fail pending proposals
    for (auto &pair : pending_proposals_)
    {
        pair.second.set_value(false);
    }
    pending_proposals_.clear();
}

void RaftNode::BecomeCandidate()
{
    state_ = RaftState::Candidate;
    current_term_++;
    voted_for_ = id_;
    votes_received_ = 1;  // Vote for self

    if (raft_meta_)
    {
        auto res = raft_meta_->Save(current_term_, *voted_for_);
        (void)res;
    }

    ResetElectionTimeout();
    BroadcastRequestVote();
}

void RaftNode::BecomeLeader()
{
    state_ = RaftState::Leader;

    // Initialize Leader volatile state
    next_index_.assign(peers_.size(), (raft_log_ ? raft_log_->LastIndex() : 0) + 1);
    match_index_.assign(peers_.size(), 0);

    // Send initial heartbeats immediately to establish authority
    TimeoutEvent e;
    e.type = TimeoutEvent::Heartbeat;
    event_queue_.Push(RaftEvent(std::move(e)));
}

void RaftNode::TimerLoop()
{
    while (running_)
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        if (std::chrono::steady_clock::now() >= next_timeout_.load())
        {
            TimeoutEvent e;
            e.type = (state_ == RaftState::Leader) ? TimeoutEvent::Heartbeat : TimeoutEvent::Election;
            event_queue_.Push(RaftEvent(std::move(e)));

            // Reset timeout immediately so we don't spam the queue
            if (state_ == RaftState::Leader)
            {
                next_timeout_ = std::chrono::steady_clock::now() + std::chrono::milliseconds(50);
            }
            else
            {
                ResetElectionTimeout();
            }
        }
    }
}

void RaftNode::ResetElectionTimeout()
{
    static thread_local std::mt19937   mt(std::random_device{}());
    std::uniform_int_distribution<int> dist(150, 300);
    next_timeout_ = std::chrono::steady_clock::now() + std::chrono::milliseconds(dist(mt));
}

void RaftNode::BroadcastRequestVote()
{
    proto::RequestVoteRequest req;
    req.set_term(current_term_);
    req.set_candidate_id(id_);
    req.set_last_log_index(raft_log_ ? raft_log_->LastIndex() : 0);
    req.set_last_log_term(raft_log_ ? raft_log_->LastTerm() : 0);

    for (size_t i = 0; i < peers_.size(); ++i)
    {
        auto stub = peers_[i].get();

        // Fire and forget RPCs async so we don't block the EventLoop
        std::thread(
            [this, stub, req, peer_id = i]()
            {
                grpc::ClientContext context;
                context.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(50));

                proto::RequestVoteResponse resp;
                grpc::Status               status = stub->RequestVote(&context, req, &resp);

                if (!status.ok())
                {
                    Z_LOG_ERROR("RequestVote to peer {} failed: {}", peer_id, status.error_message());
                }

                RequestVoteResponseEvent e;
                e.peer_id = peer_id;
                e.rpc_success = status.ok();
                e.response = resp;

                event_queue_.Push(RaftEvent(std::move(e)));
            })
            .detach();
    }
}

void RaftNode::BroadcastAppendEntries()
{
    for (size_t i = 0; i < peers_.size(); ++i)
    {
        proto::AppendEntriesRequest req;
        req.set_term(current_term_);
        req.set_leader_id(id_);
        req.set_leader_commit(commit_index_);

        uint64_t peer_next_idx = (next_index_.size() > i) ? next_index_[i] : 1;
        uint64_t prev_idx = peer_next_idx - 1;
        req.set_prev_log_index(prev_idx);

        uint64_t prev_term = 0;
        if (raft_log_ && prev_idx > 0 && prev_idx <= raft_log_->LastIndex())
        {
            auto res = raft_log_->Get(prev_idx);
            if (res) prev_term = res->term();
        }
        req.set_prev_log_term(prev_term);

        uint64_t last_idx = raft_log_ ? raft_log_->LastIndex() : 0;
        if (raft_log_ && last_idx >= peer_next_idx)
        {
            auto entries = raft_log_->GetRange(peer_next_idx, last_idx + 1);
            if (entries)
            {
                for (const auto &entry : *entries)
                {
                    *req.add_entries() = entry;
                }
            }
        }

        auto     stub = peers_[i].get();
        uint64_t sent_last_index = prev_idx + req.entries_size();

        std::thread(
            [this, stub, req, peer_id = i, sent_last_index]()
            {
                grpc::ClientContext context;
                context.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(50));

                proto::AppendEntriesResponse resp;
                grpc::Status                 status = stub->AppendEntries(&context, req, &resp);
                if (!status.ok())
                {
                    Z_LOG_ERROR("AppendEntries to peer {} failed: {} - {}", peer_id,
                                static_cast<int>(status.error_code()), status.error_message());
                }

                AppendEntriesResponseEvent e;
                e.peer_id = peer_id;
                e.rpc_success = status.ok();
                e.response = resp;
                e.sent_last_index = sent_last_index;

                event_queue_.Push(RaftEvent(std::move(e)));
            })
            .detach();
    }
}

void RaftNode::ApplyCommittedLogs()
{
    while (last_applied_ < commit_index_)
    {
        last_applied_++;
        if (raft_log_ && state_machine_)
        {
            auto entry_res = raft_log_->Get(last_applied_);
            if (entry_res)
            {
                const auto &entry = *entry_res;
                if (entry.type() == proto::LogEntry::PUT)
                {
                    auto res = state_machine_->Put(entry.key(), entry.value());
                    Z_LOG_INFO("Node {} applied PUT: {}={} (index {})", id_, entry.key(), entry.value(), last_applied_);
                    (void)res;
                }
                else if (entry.type() == proto::LogEntry::DELETE)
                {
                    auto res = state_machine_->Delete(entry.key());
                    Z_LOG_INFO("Node {} applied DELETE: {} (index {})", id_, entry.key(), last_applied_);
                    (void)res;
                }

                // Notify pending proposal if exists (only Leader will have it)
                auto it = pending_proposals_.find(last_applied_);
                if (it != pending_proposals_.end())
                {
                    it->second.set_value(true);
                    pending_proposals_.erase(it);
                }
            }
        }
    }
}

}  // namespace consensus
}  // namespace zujan

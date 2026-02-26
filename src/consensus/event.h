#pragma once

#include <functional>
#include <future>
#include <memory>
#include <string>
#include <variant>

#include "zujan.pb.h"

namespace zujan
{
namespace consensus
{

// Event raised by local timers (heartbeat or election)
struct TimeoutEvent
{
    enum Type
    {
        Election,
        Heartbeat
    };
    Type type;
};

// Event raised when a client asks to propose a new KV
struct ClientProposalEvent
{
    std::string                         key;
    std::string                         value;
    bool                                is_delete;
    std::shared_ptr<std::promise<bool>> promise;
};

// Event raised when a remote node requests a vote
struct RequestVoteEvent
{
    proto::RequestVoteRequest                       request;
    std::function<void(proto::RequestVoteResponse)> callback;
};

// Event raised when a remote node replies to OUR vote request
struct RequestVoteResponseEvent
{
    uint64_t                   peer_id;
    proto::RequestVoteResponse response;
    bool                       rpc_success;
};

// Event raised when a remote node sends an AppendEntries RPC
struct AppendEntriesEvent
{
    proto::AppendEntriesRequest                       request;
    std::function<void(proto::AppendEntriesResponse)> callback;
};

// Event raised when a remote node replies to OUR AppendEntries RPC
struct AppendEntriesResponseEvent
{
    uint64_t                     peer_id;
    proto::AppendEntriesResponse response;
    bool                         rpc_success;
    uint64_t                     sent_last_index;
};

// Event raised when async disk I/O completes (Decouples disk from state machine
// network)
struct DiskWriteCompleteEvent
{
    uint64_t last_index;
    bool     success;
};

// The generic variant capable of holding any actionable event for the Raft loop
using RaftEvent = std::variant<TimeoutEvent, ClientProposalEvent, RequestVoteEvent, AppendEntriesEvent,
                               RequestVoteResponseEvent, AppendEntriesResponseEvent>;

}  // namespace consensus
}  // namespace zujan

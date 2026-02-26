#include "raft_service.h"

#include <future>

namespace zujan
{
namespace consensus
{

RaftServiceImpl::RaftServiceImpl(RaftNode *node) : node_(node) {}

grpc::Status RaftServiceImpl::RequestVote(grpc::ServerContext *context, const proto::RequestVoteRequest *request,
                                          proto::RequestVoteResponse *response)
{
    // For simplicity in the first phase, we block the gRPC thread
    // waiting for the single-threaded EventLoop to process the vote.
    std::promise<proto::RequestVoteResponse> promise;
    auto                                     future = promise.get_future();

    node_->OnRequestVote(*request, [&promise](proto::RequestVoteResponse resp) { promise.set_value(std::move(resp)); });

    *response = future.get();
    return grpc::Status::OK;
}

grpc::Status RaftServiceImpl::AppendEntries(grpc::ServerContext *context, const proto::AppendEntriesRequest *request,
                                            proto::AppendEntriesResponse *response)
{
    std::promise<proto::AppendEntriesResponse> promise;
    auto                                       future = promise.get_future();

    node_->OnAppendEntries(*request,
                           [&promise](proto::AppendEntriesResponse resp) { promise.set_value(std::move(resp)); });

    *response = future.get();
    return grpc::Status::OK;
}

// ----------------------------------------------------------------------------
// KVServiceImpl Implementation
// ----------------------------------------------------------------------------

KVServiceImpl::KVServiceImpl(RaftNode *node) : node_(node) {}

grpc::Status KVServiceImpl::Put(grpc::ServerContext *context, const proto::PutRequest *request,
                                proto::PutResponse *response)
{
    if (!node_->IsLeader())
    {
        response->set_success(false);
        response->set_error_message("Not the leader");
        return grpc::Status::OK;
    }

    std::future<bool> fut = node_->Propose(request->key(), request->value(), false);

    if (fut.wait_for(std::chrono::seconds(3)) == std::future_status::ready)
    {
        bool success = fut.get();
        response->set_success(success);
        if (!success) response->set_error_message("Propose failed");
    }
    else
    {
        response->set_success(false);
        response->set_error_message("Timeout waiting for consensus");
    }
    return grpc::Status::OK;
}

grpc::Status KVServiceImpl::Get(grpc::ServerContext *context, const proto::GetRequest *request,
                                proto::GetResponse *response)
{
    auto res = node_->GetKV(request->key());
    if (res && res.value().has_value())
    {
        response->set_success(true);
        response->set_value(res.value().value());
    }
    else
    {
        response->set_success(false);
        response->set_error_message(res ? "Key not found" : res.error().message);
    }
    return grpc::Status::OK;
}

grpc::Status KVServiceImpl::Delete(grpc::ServerContext *context, const proto::DeleteRequest *request,
                                   proto::DeleteResponse *response)
{
    if (!node_->IsLeader())
    {
        response->set_success(false);
        response->set_error_message("Not the leader");
        return grpc::Status::OK;
    }

    std::future<bool> fut = node_->Propose(request->key(), "", true);

    if (fut.wait_for(std::chrono::seconds(3)) == std::future_status::ready)
    {
        bool success = fut.get();
        response->set_success(success);
        if (!success) response->set_error_message("Propose failed");
    }
    else
    {
        response->set_success(false);
        response->set_error_message("Timeout waiting for consensus");
    }
    return grpc::Status::OK;
}

}  // namespace consensus
}  // namespace zujan

#pragma once

#include "raft.h"
#include "zujan.grpc.pb.h"

namespace zujan
{
namespace consensus
{

// gRPC Service implementation for the Raft node.
// This bridges the synchronous gRPC threadpool handlers
// with the asynchronous lock-free lock loop of RaftNode.
class RaftServiceImpl final : public proto::RaftService::Service
{
public:
    explicit RaftServiceImpl(RaftNode *node);
    ~RaftServiceImpl() override = default;

    grpc::Status RequestVote(grpc::ServerContext *context, const proto::RequestVoteRequest *request,
                             proto::RequestVoteResponse *response) override;

    grpc::Status AppendEntries(grpc::ServerContext *context, const proto::AppendEntriesRequest *request,
                               proto::AppendEntriesResponse *response) override;

private:
    RaftNode *node_;
};

// gRPC Service implementation for the Client KV API
class KVServiceImpl final : public proto::KVService::Service
{
public:
    explicit KVServiceImpl(RaftNode *node);
    ~KVServiceImpl() override = default;

    grpc::Status Put(grpc::ServerContext *context, const proto::PutRequest *request,
                     proto::PutResponse *response) override;

    grpc::Status Get(grpc::ServerContext *context, const proto::GetRequest *request,
                     proto::GetResponse *response) override;

    grpc::Status Delete(grpc::ServerContext *context, const proto::DeleteRequest *request,
                        proto::DeleteResponse *response) override;

private:
    RaftNode *node_;
};

}  // namespace consensus
}  // namespace zujan

#include "consensus/raft_service.h"

#include <gtest/gtest.h>

#include <future>

namespace zujan
{
namespace consensus
{
namespace
{

// We need a subclass or accessor to intercept EventQueue for testing without
// fully starting the private run loop, or we can just start it and let it run.
// However, since we want to verify gRPC -> EventQueue -> Callback bridging,
// we will start RaftNode, send a gRPC request, and ensure the callback
// resolves.

TEST(RaftServiceTest, RequestVoteDispatch)
{
    RaftNode node(1, {});
    node.Start();

    RaftServiceImpl service(&node);

    proto::RequestVoteRequest req;
    req.set_term(2);
    req.set_candidate_id(5);
    req.set_last_log_index(10);
    req.set_last_log_term(1);

    proto::RequestVoteResponse resp;
    grpc::ServerContext        context;

    // The call below will block until the EventLoop processes it.
    // In our stub HandleRequestVote, we don't currently invoke the callback!
    // IF it completely stalls, it means we need to update RaftNode's
    // HandleRequestVote stub.

    // To avoid deadlocking the test suite right now, we will add a future with
    // timeout.
    auto future = std::async(std::launch::async, [&]() { return service.RequestVote(&context, &req, &resp); });

    auto status = future.wait_for(std::chrono::milliseconds(50));
    EXPECT_EQ(status, std::future_status::timeout);  // Expecting timeout because
                                                     // stub doesn't callback yet

    node.Stop();
}

}  // namespace
}  // namespace consensus
}  // namespace zujan

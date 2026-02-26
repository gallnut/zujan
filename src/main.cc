#include <csignal>
#include <cstdlib>
#include <future>
#include <sstream>
#include <string>
#include <vector>

#include "consensus/raft.h"
#include "utils/logger.h"

std::promise<void> exit_signal;

void SignalHandler(int signum)
{
    Z_LOG_INFO("Interrupt signal ({}) received.", signum);
    try
    {
        exit_signal.set_value();
    }
    catch (...)
    {
    }
}

int main(int argc, char* argv[])
{
    const char* env_id = std::getenv("RAFT_NODE_ID");
    const char* env_peers = std::getenv("RAFT_PEERS");

    if (!env_id)
    {
        Z_LOG_FATAL("Error: RAFT_NODE_ID environment variable not set.");
        return 1;
    }

    uint64_t                 node_id = std::stoull(env_id);
    std::vector<std::string> peers;

    if (env_peers)
    {
        std::stringstream ss(env_peers);
        std::string       peer;
        while (std::getline(ss, peer, ','))
        {
            if (!peer.empty())
            {
                peers.push_back(peer);
            }
        }
    }

    zujan::utils::AsyncLogger::GetInstance().Start();
    Z_LOG_INFO("Starting Raft Node {} with {} peers.", node_id, peers.size());

    // Setup signal handlers
    std::signal(SIGINT, SignalHandler);
    std::signal(SIGTERM, SignalHandler);

    // Initialize and start RaftNode
    zujan::consensus::RaftNode node(node_id, peers);
    node.Start();

    // Block until a signal is received
    auto future = exit_signal.get_future();
    future.wait();

    Z_LOG_INFO("Stopping Raft Node...");
    node.Stop();
    Z_LOG_INFO("Raft Node stopped.");

    return 0;
}
#include <csignal>
#include <cstdlib>
#include <future>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

#include "consensus/raft.h"

std::promise<void> exit_signal;

void SignalHandler(int signum)
{
    std::cout << "Interrupt signal (" << signum << ") received.\n";
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
        std::cerr << "Error: RAFT_NODE_ID environment variable not set.\n";
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

    std::cout << "Starting Raft Node " << node_id << " with " << peers.size() << " peers.\n";

    // Setup signal handlers
    std::signal(SIGINT, SignalHandler);
    std::signal(SIGTERM, SignalHandler);

    // Initialize and start RaftNode
    zujan::consensus::RaftNode node(node_id, peers);
    node.Start();

    // Block until a signal is received
    auto future = exit_signal.get_future();
    future.wait();

    std::cout << "Stopping Raft Node...\n";
    node.Stop();
    std::cout << "Raft Node stopped.\n";

    return 0;
}
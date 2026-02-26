#include <chrono>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include "consensus/raft.h"

using namespace zujan::consensus;

int main(int argc, char *argv[])
{
    if (argc < 2)
    {
        std::cerr << "Usage: " << argv[0] << " <node_id> [peer_address...]" << std::endl;
        return 1;
    }

    uint64_t                 id = std::stoull(argv[1]);
    std::vector<std::string> peers;
    for (int i = 2; i < argc; ++i)
    {
        peers.push_back(argv[i]);
    }

    RaftNode node(id, peers);
    node.Start();

    std::cout << "Node " << id << " waiting for elections and heartbeats..." << std::endl;

    std::atomic<bool> alive{true};
    std::thread       client_thread(
        [&]()
        {
            int counter = 0;
            while (alive)
            {
                std::this_thread::sleep_for(std::chrono::seconds(2));
                // Try proposing. Since only the Leader accepts proposals, this will
                // silently fail on Follower/Candidate
                std::string key = "node" + std::to_string(id) + "_key" + std::to_string(counter);
                std::string val = "data_" + std::to_string(counter);
                node.Propose(key, val, false);
                counter++;
            }
        });

    // Keep the process alive
    // Run for 15 seconds then exit naturally for the test
    std::this_thread::sleep_for(std::chrono::seconds(15));
    alive = false;
    client_thread.join();

    node.Stop();
    std::cout << "Node " << id << " shut down naturally." << std::endl;
    return 0;
}

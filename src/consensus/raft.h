#pragma once

#include <mutex>
#include <optional>
#include <string>
#include <vector>
// #include "zujan.grpc.pb.h" // Will include after proto compilation

namespace zujan {
namespace consensus {

enum class RaftState { Follower, Candidate, Leader };

class RaftNode {
public:
  RaftNode(uint64_t id, const std::vector<std::string> &peer_addresses);
  ~RaftNode();

  // Start the Raft node
  void Start();

  // Stop the Raft node
  void Stop();

  // Propose a command to the state machine
  bool Propose(const std::string &key, const std::string &value,
               bool is_delete = false);

private:
  // Core Raft loop (e.g., tick timer and election timeout)
  void RunLoop();
  void BecomeFollower(uint64_t term);
  void BecomeCandidate();
  void BecomeLeader();

private:
  uint64_t id_;
  RaftState state_{RaftState::Follower};

  // Persistent state on all servers
  uint64_t current_term_{0};
  std::optional<uint64_t> voted_for_;
  // std::vector<LogEntry> log_;

  // Volatile state on all servers
  uint64_t commit_index_{0};
  uint64_t last_applied_{0};

  // Volatile state on leaders
  std::vector<uint64_t> next_index_;
  std::vector<uint64_t> match_index_;

  std::mutex mutex_;
  bool running_{false};
};

} // namespace consensus
} // namespace zujan

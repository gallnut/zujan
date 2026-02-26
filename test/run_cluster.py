import subprocess
import time
import sys
import signal
import os

def run_cluster():
    # Clear old data files
    os.system("rm -f raft_meta_*.dat raft_log_*.dat")
    
    executable = "./bin/raft_node_main"
    
    # Node 1: ID 1, peers 127.0.0.1:50052, 127.0.0.1:50053
    # Node 2: ID 2, peers 127.0.0.1:50051, 127.0.0.1:50053
    # Node 3: ID 3, peers 127.0.0.1:50051, 127.0.0.1:50052
    
    nodes = [
        {"id": "1", "peers": ["127.0.0.1:50052", "127.0.0.1:50053"]},
        {"id": "2", "peers": ["127.0.0.1:50051", "127.0.0.1:50053"]},
        {"id": "3", "peers": ["127.0.0.1:50051", "127.0.0.1:50052"]}
    ]
    
    processes = []
    
    print("Starting Raft cluster (3 nodes)...")
    for node in nodes:
        cmd = [executable, node["id"]] + node["peers"]
        # Output to terminal
        p = subprocess.Popen(cmd, stdout=sys.stdout, stderr=sys.stderr)
        processes.append(p)
    
    try:
        # Let the cluster run for 5 seconds to establish a leader and send heartbeats
        time.sleep(5)
    except KeyboardInterrupt:
        pass
    finally:
        print("\nShutting down cluster...")
        for p in processes:
            p.send_signal(signal.SIGINT)
            p.wait()
        print("Cluster shutdown complete.")

if __name__ == "__main__":
    run_cluster()

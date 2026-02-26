import grpc
import sys
import time
import subprocess
import signal
import os

from zujan_pb2 import PutRequest, GetRequest, DeleteRequest
from zujan_pb2_grpc import KVServiceStub

def run_test():
    os.system("rm -f raft_meta_*.dat raft_lsm_*/* wal.log")
    
    executable = "./bin/raft_node_main"
    nodes = [
        {"id": "1", "peers": ["127.0.0.1:50052", "127.0.0.1:50053"]},
        {"id": "2", "peers": ["127.0.0.1:50051", "127.0.0.1:50053"]},
        {"id": "3", "peers": ["127.0.0.1:50051", "127.0.0.1:50052"]}
    ]
    processes = []
    print("Starting Raft cluster (3 nodes)...")
    for node in nodes:
        cmd = [executable, node["id"]] + node["peers"]
        p = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        processes.append(p)
    # Attempt to connect to Node 1 (port 50051), Node 2 (port 50052), or Node 3 (port 50053)
    # until we find the leader and put a value.
    ports = [50051, 50052, 50053]
    
    put_success = False
    leader_port = None
    
    print("Wait 3 seconds for leader election...")
    time.sleep(3)
    
    for port in ports:
        print(f"Trying to PUT on node at port {port}...")
        channel = grpc.insecure_channel(f'localhost:{port}')
        stub = KVServiceStub(channel)
        
        req = PutRequest(key="test_key", value="test_value")
        resp = stub.Put(req)
        
        if resp.success:
            print(f"Success! Node at {port} is the leader.")
            put_success = True
            leader_port = port
            break
        else:
            print(f"Failed on {port}: {resp.error_message}")
            
    if not put_success:
        print("Could not find leader or PUT failed on all nodes.")
        sys.exit(1)
        
    print("\nTrying to GET the value back from the leader...")
    channel = grpc.insecure_channel(f'localhost:{leader_port}')
    stub = KVServiceStub(channel)
    req = GetRequest(key="test_key")
    resp = stub.Get(req)
    
    if resp.success:
        print(f"GET Success! Value = {resp.value}")
        if resp.value != "test_value":
            print("ERROR: Value does not match.")
            sys.exit(1)
    else:
        print(f"GET Failed: {resp.error_message}")
        sys.exit(1)
        
    print("\nTests passed successfully!")

    print("\nShutting down cluster...")
    for p in processes:
        p.send_signal(signal.SIGKILL)
        p.wait()

if __name__ == '__main__':
    run_test()

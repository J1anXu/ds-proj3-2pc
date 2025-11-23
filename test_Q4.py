import sys
sys.path.append("./proto")
from proto import raft_pb2
from proto import raft_pb2_grpc

import grpc
import time
import random


NODES = [
    "node1:50051",
    "node2:50051",
    "node3:50051",
    "node4:50051",
    "node5:50051"
]


def find_leader():
    """
    尝试向任意节点发送 ClientRequest，
    follower 会返回:
        success = False
        message = "Not leader"
        leader_id = <leader>

    leader 会返回 success=True
    """

    for addr in NODES:
        try:
            stub = raft_pb2_grpc.RaftNodeStub(grpc.insecure_channel(addr))
            reply = stub.ClientRequest(
                raft_pb2.ClientRequestMessage(
                    client_id=999,
                    operation="probe"
                )
            )

            # 如果返回 success=True，则它是 leader
            if reply.success:
                print(f"Leader found: {addr}")
                return addr

            # follower 返回 leader_id
            if reply.leader_id > 0:
                leader_addr = f"node{reply.leader_id}:50051"
                print(f"Follower {addr} tells leader = {leader_addr}")
                return leader_addr

        except Exception as e:
            pass

    return None


def send_operation(leader_addr, op):
    print(f"\n➡️ Sending CLIENT REQUEST '{op}' to LEADER {leader_addr}")

    stub = raft_pb2_grpc.RaftNodeStub(grpc.insecure_channel(leader_addr))

    reply = stub.ClientRequest(
        raft_pb2.ClientRequestMessage(
            client_id=1,
            operation=op
        )
    )

    print("📥 Leader replied:")
    print(reply)
    print("")


if __name__ == "__main__":

    print("=== Q4: Testing ClientRequest + Log Replication + Commit ===\n")
    print("⏳ Step 1: Waiting for leader election...\n")

    leader = None
    for _ in range(15):
        leader = find_leader()
        if leader:
            break
        print("Waiting for leader election...")
        time.sleep(1)

    if not leader:
        print("❌ Leader not found — cannot continue.")
        exit(1)

    print("\n✅ Leader is ready!\n")

    operations = [
        "put x 5",
        "add x 3",
        "multiply x 10"
    ]

    for op in operations:
        send_operation(leader, op)
        time.sleep(2)  # 给 heartbeat 复制 + followers apply 的时间

    print("\n⏳ Waiting 3 seconds for followers to APPLY logs...")
    time.sleep(3)

    print("\n=== Now check docker logs ===")
    print("""
-------------------------------------------
LEADER:
Node X sends RPC AppendEntries to Node Y
Node X EXECUTE op='put x 5' index=0
Node X EXECUTE op='add x 3' index=1
Node X EXECUTE op='multiply x 10' index=2

FOLLOWERS:
Node Y runs RPC AppendEntries called by Node X
Node Y EXECUTE op='put x 5' index=0
Node Y EXECUTE op='add x 3' index=1
Node Y EXECUTE op='multiply x 10' index=2
-------------------------------------------
""")

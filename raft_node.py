import sys
sys.path.append("./proto")

import grpc
import time
import threading
import random
from concurrent import futures

from proto import raft_pb2, raft_pb2_grpc



FOLLOWER = 0
CANDIDATE = 1
LEADER = 2

HEARTBEAT_INTERVAL = 1.0
ELECTION_TIMEOUT_RANGE = (1.5, 3.0)


class RaftNode(raft_pb2_grpc.RaftNodeServicer):

    def __init__(self, node_id, peers):
        self.node_id = node_id
        self.peers = peers

        self.state = FOLLOWER
        self.current_term = 0
        self.voted_for = None

        self.log = []    # {index, term, operation}
        self.commit_index = -1

        self.lock = threading.Lock()
        self.reset_election_timeout()

    # -------------------------------------------------------------
    def reset_election_timeout(self):
        self.election_timeout = time.time() + random.uniform(*ELECTION_TIMEOUT_RANGE)

    def is_majority(self, count):
        return count > len(self.peers) // 2

    # -------------------------------------------------------------
    # RPC：RequestVote
    # -------------------------------------------------------------
    def RequestVote(self, request, context):
        print(f"Node {self.node_id} runs RPC RequestVote called by Node {request.candidate_id}")

        with self.lock:
            granted = False

            if request.term > self.current_term:
                self.current_term = request.term
                self.state = FOLLOWER
                self.voted_for = None

            if (self.voted_for in [None, request.candidate_id]) and request.term >= self.current_term:
                granted = True
                self.voted_for = request.candidate_id
                self.reset_election_timeout()

        return raft_pb2.RequestVoteReply(
            term=self.current_term,
            vote_granted=granted
        )

    # -------------------------------------------------------------
    # RPC：AppendEntries（心跳 + 日志复制）
    # -------------------------------------------------------------
    def AppendEntries(self, request, context):
        print(f"Node {self.node_id} runs RPC AppendEntries called by Node {request.leader_id}")

        with self.lock:
            self.reset_election_timeout()
            self.state = FOLLOWER
            self.current_term = request.term

            # 覆盖日志（作业要求：leader 发送 entire log）
            self.log = []
            for e in request.entries:
                self.log.append({
                    "index": e.index,
                    "term": e.term,
                    "operation": e.operation
                })

            self.commit_index = request.commit_index

        return raft_pb2.AppendEntriesReply(
            term=self.current_term,
            success=True,
            follower_id=self.node_id
        )
    def ClientRequest(self, request, context):
        print(
            f"Node {self.node_id} runs RPC ClientRequest called by client {request.client_id}"
        )

        with self.lock:
            # 不是 Leader → 告诉 client 现在谁是 leader
            if self.state != LEADER:
                return raft_pb2.ClientReplyMessage(
                    success=False,
                    message="Not leader",
                    leader_id=self.node_id if self.state == LEADER else -1
                )

            # Leader 接受客户端操作 o
            new_index = len(self.log)
            entry = {
                "index": new_index,
                "term": self.current_term,
                "operation": request.operation
            }
            self.log.append(entry)

            print(
                f"Node {self.node_id} LEADER accepted op='{request.operation}' "
                f"as index={new_index}"
            )

        # 复制到所有 follower
        acks = 1
        entries_proto = [
            raft_pb2.LogEntry(
                index=e["index"],
                term=e["term"],
                operation=e["operation"]
            )
            for e in self.log
        ]

        for peer_id, address in self.peers.items():
            try:
                stub = raft_pb2_grpc.RaftNodeStub(grpc.insecure_channel(address))
                reply = stub.AppendEntries(
                    raft_pb2.AppendEntriesRequest(
                        term=self.current_term,
                        leader_id=self.node_id,
                        entries=entries_proto,
                        commit_index=self.commit_index
                    )
                )
                if reply.success:
                    acks += 1
            except Exception as e:
                print(f"Leader failed AppendEntries to Node {peer_id}: {e}")

        # 判断是否多数派
        if self.is_majority(acks):
            with self.lock:
                self.commit_index = len(self.log) - 1
                print(
                    f"Node {self.node_id} committed op='{request.operation}' "
                    f"at index={self.commit_index}"
                )

            return raft_pb2.ClientReplyMessage(
                success=True,
                message="Operation committed",
                leader_id=self.node_id
            )

        else:
            return raft_pb2.ClientReplyMessage(
                success=False,
                message="Failed to reach majority",
                leader_id=self.node_id
            )

    # -------------------------------------------------------------
    # RPC server
    # -------------------------------------------------------------
    def start_server(self):
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        raft_pb2_grpc.add_RaftNodeServicer_to_server(self, server)
        server.add_insecure_port("[::]:50051")
        server.start()
        print(f"Node {self.node_id} RPC server started at port 50051")
        return server

    # -------------------------------------------------------------
    # Leader 心跳线程
    # -------------------------------------------------------------
    def heartbeat_thread(self):
        while True:
            time.sleep(HEARTBEAT_INTERVAL)
            if self.state != LEADER:
                continue

            for peer_id, address in self.peers.items():
                print(f"Node {self.node_id} sends RPC AppendEntries to Node {peer_id}")

                stub = raft_pb2_grpc.RaftNodeStub(grpc.insecure_channel(address))

                # Send full log
                entries = [
                    raft_pb2.LogEntry(
                        index=e["index"],
                        term=e["term"],
                        operation=e["operation"]
                    )
                    for e in self.log
                ]

                try:
                    stub.AppendEntries(
                        raft_pb2.AppendEntriesRequest(
                            term=self.current_term,
                            leader_id=self.node_id,
                            entries=entries,
                            commit_index=self.commit_index
                        )
                    )
                except:
                    pass

    # -------------------------------------------------------------
    # 选举线程
    # -------------------------------------------------------------
    def election_thread(self):
        while True:
            time.sleep(0.1)

            with self.lock:
                if time.time() < self.election_timeout:
                    continue

                self.state = CANDIDATE
                self.current_term += 1
                self.voted_for = self.node_id
                self.reset_election_timeout()

            votes = 1

            for peer_id, address in self.peers.items():
                print(f"Node {self.node_id} sends RPC RequestVote to Node {peer_id}")

                try:
                    stub = raft_pb2_grpc.RaftNodeStub(grpc.insecure_channel(address))
                    reply = stub.RequestVote(
                        raft_pb2.RequestVoteRequest(
                            term=self.current_term,
                            candidate_id=self.node_id
                        )
                    )
                    if reply.vote_granted:
                        votes += 1
                except:
                    pass

            if self.is_majority(votes):
                print(f"Node {self.node_id} becomes LEADER at term {self.current_term}")
                self.state = LEADER

    # -------------------------------------------------------------
    def run(self):
        srv = self.start_server()

        threading.Thread(target=self.election_thread, daemon=True).start()
        threading.Thread(target=self.heartbeat_thread, daemon=True).start()

        srv.wait_for_termination()


if __name__ == "__main__":
    import os
    node_id = int(os.environ["NODE_ID"])

    peers = {i: f"node{i}:50051" for i in range(1, 6) if i != node_id}

    node = RaftNode(node_id, peers)
    node.run()

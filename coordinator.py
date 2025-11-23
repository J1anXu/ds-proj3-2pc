import grpc
import uuid
import time
import sys
import consensus_pb2
import consensus_pb2_grpc
import os
import logging
logging.basicConfig(level=logging.INFO)
COORD_ID = int(os.getenv("NODE_ID", "1"))

# participants addresses
participants = {
    2: "node2:50051",
    3: "node3:50051",
    4: "node4:50051",
    5: "node5:50051"
}

# --------------------- Phase 1: Voting ---------------------
def do_vote_phase(tx_id):
    votes = {}

    for node_id, addr in participants.items():
        logging.info(f"Phase VOTE of Node {COORD_ID} sends RPC Vote to Phase VOTE of Node {node_id}")

        channel = grpc.insecure_channel(addr)
        stub = consensus_pb2_grpc.ConsensusServiceStub(channel)

        resp = stub.Vote(consensus_pb2.VoteRequest(
            tx_id=tx_id,
            sender_id=COORD_ID
        ))

        votes[node_id] = resp.vote
        logging.info(f"[Coordinator] Node {node_id} voted {resp.vote}")

    return votes

# --------------------- Phase 2: Global Decision ---------------------
def do_decision_phase(tx_id, votes):
    final_decision = "commit" if all(v == "commit" for v in votes.values()) else "abort"

    logging.info(f"\n[Coordinator] Final decision for tx {tx_id}: {final_decision.upper()}")

    for node_id, addr in participants.items():
        logging.info(f"Phase DECISION of Node {COORD_ID} sends RPC GlobalDecision to Phase DECISION of Node {node_id}")

        channel = grpc.insecure_channel(addr)
        stub = consensus_pb2_grpc.ConsensusServiceStub(channel)

        stub.GlobalDecision(consensus_pb2.GlobalDecisionRequest(
            tx_id=tx_id,
            decision=final_decision,
            sender_id=COORD_ID
        ))

    # internal self RPC
    logging.info(f"Phase DECISION of Node {COORD_ID} sends RPC SelfDecision to Phase DECISION of Node {COORD_ID}")
    stub.SelfDecision(consensus_pb2.GlobalDecisionRequest(
        tx_id=tx_id,
        decision=final_decision,
        sender_id=COORD_ID
    ))

# --------------------- Main Flow ---------------------
def run_2pc():
    tx_id = str(uuid.uuid4())

    logging.info(f"\n===== Starting 2PC for tx={tx_id} =====\n")

    votes = do_vote_phase(tx_id)
    do_decision_phase(tx_id, votes)

if __name__ == "__main__":
    time.sleep(3)
    run_2pc()

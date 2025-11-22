import grpc
from concurrent import futures
import time
import os

import consensus_pb2
import consensus_pb2_grpc

NODE_ID = int(os.getenv("NODE_ID", "1"))

class ConsensusService(consensus_pb2_grpc.ConsensusServiceServicer):

    # ---------------------- Phase 1: Vote ----------------------
    def Vote(self, request, context):
        print(f"Phase VOTE of Node {NODE_ID} receives RPC Vote from Phase VOTE of Node {request.sender_id}")

        # Always vote commit for now
        return consensus_pb2.VoteResponse(
            tx_id=request.tx_id,
            vote="commit"
        )

    # ---------------------- Phase 2: Global Decision ----------------------
    def GlobalDecision(self, request, context):
        print(f"Phase DECISION of Node {NODE_ID} receives RPC GlobalDecision from Phase DECISION of Node {request.sender_id}")
        print(f"Node {NODE_ID} performs local {request.decision.upper()} for tx {request.tx_id}")

        return consensus_pb2.Ack(
            tx_id=request.tx_id,
            status="received"
        )

    # ---------------------- Node-internal (Self) RPC ----------------------
    def SelfDecision(self, request, context):
        print(f"Phase DECISION of Node {NODE_ID} receives RPC SelfDecision from SAME NODE")
        
        return consensus_pb2.Ack(
            tx_id=request.tx_id,
            status="self-received"
        )

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    consensus_pb2_grpc.add_ConsensusServiceServicer_to_server(ConsensusService(), server)
    server.add_insecure_port("[::]:50051")
    server.start()
    print(f"Node {NODE_ID} gRPC server started on port 50051")
    server.wait_for_termination()

if __name__ == "__main__":
    serve()

# Import for converting string IDs back to ObjectId
from bson.objectid import ObjectId
from flask import Flask, request, render_template, redirect, url_for, flash, jsonify, session
from flask_cors import CORS
from flask_pymongo import PyMongo
from flask_socketio import SocketIO, emit
from flask_session import Session
from datetime import datetime
import random
import os
import redis
import uuid
import requests
import logging

# ==== gRPC ====
import grpc
from concurrent import futures
import two_pc_pb2
import two_pc_pb2_grpc


# -----------------------------
#   LOGGING SETUP
# -----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s:%(message)s"
)


# -----------------------------
#        NODE ROLE SETUP
# -----------------------------
ROLE = os.getenv("ROLE", "participant")        # coordinator / participant
NODE_ID = os.getenv("NODE_ID", "unknown")

PARTICIPANTS = os.getenv("PARTICIPANTS", "")
if PARTICIPANTS:
    PARTICIPANTS = [p.strip() for p in PARTICIPANTS.split(",") if p.strip()]


# -----------------------------
#        FLASK SETUP
# -----------------------------
app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})

app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', 'default_secret')
app.config['SESSION_TYPE'] = os.environ.get('SESSION_TYPE', 'filesystem')
SESSION_REDIS_URL = os.environ.get('SESSION_REDIS')

app.config["MONGO_URI"] = os.environ.get("MONGO_URI", "mongodb://localhost:27017/fallback_db")

if app.config['SESSION_TYPE'] == 'redis' and SESSION_REDIS_URL:
    try:
        app.config['SESSION_REDIS'] = redis.from_url(SESSION_REDIS_URL)
        app.logger.info("Redis client initialized successfully from URL.")
    except Exception as e:
        app.logger.info(f"ERROR: Could not initialize Redis client: {e}")

server_session = Session(app)

socketio = SocketIO(
    app,
    cors_allowed_origins="*",
    manage_session=False,
    message_queue='redis://redis:6379/0',
    async_mode='eventlet',
    logger=True,
    engineio_logger=True
)

mongo = PyMongo(app)


# -----------------------------
# HOME ROUTE
# -----------------------------
@app.route('/', methods=['GET'])
def index():
    return render_template('login.html')


# -----------------------------
#         2PC PARTICIPANT
# -----------------------------
@app.route('/vote', methods=['POST'])
def vote():
    """Voting Phase: Participant receives vote request"""
    data = request.get_json(force=True)
    tx_id = data.get("tx_id")

    vote_result = "commit"

    logging.info(
        f"Phase Voting of Node {NODE_ID} receives RPC VoteRequest "
        f"from Phase Voting of Node coordinator."
    )
    logging.info(f"[PARTICIPANT] vote for tx={tx_id} -> {vote_result}")

    return jsonify({"tx_id": tx_id, "vote": vote_result})


# -----------------------------
#      2PC DECISION PHASE (gRPC)
# -----------------------------
class DecisionPhaseServicer(two_pc_pb2_grpc.DecisionServiceServicer):

    def GlobalCommit(self, request, context):
        tx_id = request.tx_id
        logging.info(
            f"Phase Decision of Node {NODE_ID} receives RPC GlobalCommit "
            f"from Phase Decision of Node coordinator."
        )
        logging.info(f"[PARTICIPANT] locally COMMIT tx={tx_id}")
        return two_pc_pb2.DecisionReply(status="ack-commit")

    def GlobalAbort(self, request, context):
        tx_id = request.tx_id
        logging.info(
            f"Phase Decision of Node {NODE_ID} receives RPC GlobalAbort "
            f"from Phase Decision of Node coordinator."
        )
        logging.info(f"[PARTICIPANT] locally ABORT tx={tx_id}")
        return two_pc_pb2.DecisionReply(status="ack-abort")


def start_grpc_server():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    two_pc_pb2_grpc.add_DecisionServiceServicer_to_server(
        DecisionPhaseServicer(), server
    )
    server.add_insecure_port("[::]:6000")
    server.start()
    logging.info("Decision Phase gRPC running on port 6000")
    server.wait_for_termination()


# -----------------------------
#      SEND GLOBAL DECISION
# -----------------------------
def send_global_decision(decision, tx_id):
    for p in PARTICIPANTS:
        channel = grpc.insecure_channel(f"{p}:6000")
        stub = two_pc_pb2_grpc.DecisionServiceStub(channel)

        request = two_pc_pb2.DecisionRequest(tx_id=tx_id)

        if decision == "commit":
            logging.info(
                f"Phase Decision of Node coordinator sends RPC GlobalCommit "
                f"to Phase Decision of Node {p}."
            )
            stub.GlobalCommit(request)
        else:
            logging.info(
                f"Phase Decision of Node coordinator sends RPC GlobalAbort "
                f"to Phase Decision of Node {p}."
            )
            stub.GlobalAbort(request)



# -----------------------------
#         2PC COORDINATOR
# -----------------------------
@app.route('/start_tx', methods=['POST'])
def start_tx():

    if ROLE != "coordinator":
        return jsonify({"error": "Only coordinator can start tx"}), 403

    data = request.get_json(force=True)
    tx_id = data.get("tx_id", str(uuid.uuid4()))
    payload = data.get("payload", {})

    votes = []
    all_commit = True

    # Voting Phase
    for p in PARTICIPANTS:

        logging.info(
            f"Phase Voting of Node {NODE_ID} sends RPC VoteRequest "
            f"to Phase Voting of Node {p}."
        )

        try:
            resp = requests.post(f"http://{p}:5000/vote",
                                 json={"tx_id": tx_id, "payload": payload},
                                 timeout=2)
            result = resp.json().get("vote", "abort")

            votes.append({"participant": p, "vote": result})
            if result != "commit":
                all_commit = False

        except Exception as e:
            logging.error(f"[COORDINATOR] ERROR contacting {p}: {e}")
            votes.append({"participant": p, "vote": "abort"})
            all_commit = False

    decision = "commit" if all_commit else "abort"

    logging.info(f"[COORDINATOR] tx={tx_id} decision={decision}")

    # Decision Phase
    send_global_decision(decision, tx_id)

    return jsonify({"tx_id": tx_id, "decision": decision, "votes": votes})


# -----------------------------
#             MAIN
# -----------------------------
if __name__ == '__main__':
    import threading

    threading.Thread(target=start_grpc_server, daemon=True).start()
    socketio.run(app, host="0.0.0.0", port=5000)

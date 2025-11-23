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

# ============================================
# LOGGING
# ============================================
logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(message)s")

# ============================================
# 2PC NODE ROLE SETUP
# ============================================
ROLE = os.getenv("ROLE", "participant")        # coordinator / participant
NODE_ID = os.getenv("NODE_ID", "unknown")

PARTICIPANTS = os.getenv("PARTICIPANTS", "")
if PARTICIPANTS:
    PARTICIPANTS = [p.strip() for p in PARTICIPANTS.split(",") if p.strip()]

# ============================================
# FLASK SETUP
# ============================================
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

# ============================================
# GLOBALS
# ============================================
BATTLE_QUEUE = []
BATTLES_IN_PROGRESS = {}
XP_PER_WIN = 100
LEVEL_UP_XP = 100

# ============================================
# ORIGINAL: USER + LOGIN SYSTEM
# ============================================
@app.route('/', methods=['GET'])
def index():
    return render_template('login.html')

@app.route('/register', methods=['POST'])
def register():

    email = request.form.get('email')
    password = request.form.get('password')
    confirm_password = request.form.get('confirm_password')

    if password != confirm_password:
        flash('Passwords do not match.', 'error')
        return redirect(url_for('index'))

    existing_user = mongo.db.players.find_one({'email': email})
    if existing_user:
        flash('Email already registered. Please log in.', 'error')
        return redirect(url_for('index'))

    user_data = {
        "email": email,
        "password": password,
        "level": 1,
        "xp": 0,
        "wins": 0,
        "losses": 0,
        "created_at": datetime.now()
    }

    mongo.db.players.insert_one(user_data)
    flash(f'Account created for {email}! Please log in.', 'success')
    return redirect(url_for('index'))

@app.route('/login', methods=['GET', 'POST'])
def login():

    email = request.form.get('email')
    password = request.form.get('password')

    if not email or not password:
        flash('Both email and password are required for login.', 'error')
        return redirect(url_for('index'))

    user = mongo.db.players.find_one({'email': email})
    if user and user['password'] == password:
        session['email'] = email
        flash(f'Successfully logged in as {email}!', 'success')
        return redirect(url_for('home'))
    
    flash('Invalid email or password.', 'error')
    return redirect(url_for('index'))

@app.route('/logout')
def logout():
    session.pop('email', None)
    return redirect(url_for('index'))

@app.route('/home')
def home():
    return render_template('home.html')

# ============================================
# ORIGINAL: INVENTORY APIs
# ============================================

@app.route('/api/inventory', methods=['GET'])
def get_inventory():
    inventory = list(mongo.db.pokemon.find({'player': session.get('email')}))
    return jsonify(inventory)

@app.route('/api/release', methods=['DELETE'])
def release_pokemon():
    data = request.get_json()
    pokemon_ids_to_release = data.get('ids', [])

    if not pokemon_ids_to_release:
        return jsonify({"message": "No Pokemon IDs provided."}), 400

    object_ids = [ObjectId(p_id) for p_id in pokemon_ids_to_release]

    result = mongo.db.pokemon.delete_many({'_id': {'$in': object_ids}})

    return jsonify({
        "message": f"Successfully released {result.deleted_count} Pokemon!",
        "deleted_count": result.deleted_count
    })

@app.route('/api/gatcha', methods=['POST'])
def run_gatcha():

    gatcha_pool = [
        {"player":session.get('email'), "name": "Squirtle", "atk": 48, "def": 65, "hp": 44, "locked":False, "image": "https://raw.githubusercontent.com/PokeAPI/sprites/master/sprites/pokemon/7.png"},
        {"player":session.get('email'), "name": "Jigglypuff", "atk": 45, "def": 20, "hp": 115, "locked":False, "image": "https://raw.githubusercontent.com/PokeAPI/sprites/master/sprites/pokemon/39.png"},
        {"player":session.get('email'), "name": "Snorlax", "atk": 110, "def": 65, "hp": 160, "locked":False, "image": "https://raw.githubusercontent.com/PokeAPI/sprites/master/sprites/pokemon/143.png"}
    ]
    
    new_pokemon = random.choice(gatcha_pool)
    mongo.db.pokemon.insert_one(new_pokemon)

    return jsonify({"message": "Gatcha successful!", "new_pokemon": new_pokemon})


# ============================================
# TRADE SYSTEM
# ============================================

@app.route('/api/trade', methods=['GET'])
def get_trade_menu_data():

    available_inventory = list(mongo.db.pokemon.find({"player": session.get('email'), "locked": False}))

    pending_trades = []
    for doc in mongo.db.trade.find({"status": "pending"}):
        doc['id'] = str(doc.pop('_id'))

        offered_details = []
        for p_id in doc['offering_ids']:
            p_doc = mongo.db.pokemon.find_one({'_id': ObjectId(p_id)})
            if p_doc:
                offered_details.append({"name": p_doc['name'], "image": p_doc['image']})

        doc['offered_details'] = offered_details
        pending_trades.append(doc)

    return jsonify({
        "inventory": available_inventory,
        "pending_trades": pending_trades,
        "current_player": session.get('email')
    })


@app.route('/api/trade/create', methods=['POST'])
def create_trade():

    data = request.get_json()
    offering_ids = data.get('offering_ids', [])

    if len(offering_ids) != 1:
        return jsonify({"message": "You must offer exactly one Pokémon."}), 400

    object_id = ObjectId(offering_ids[0])

    lock_result = mongo.db.pokemon.update_one(
        {'_id': object_id, 'player': session.get('email'), 'locked': False},
        {'$set': {'locked': True}}
    )

    if lock_result.modified_count != 1:
        return jsonify({"message": "Could not lock Pokémon."}), 409

    trade_request = {
        "creator": session.get('email'),
        "offering_ids": offering_ids,
        "looking_for_count": 1,
        "status": "pending",
        "timestamp": datetime.now()
    }

    mongo.db.trade.insert_one(trade_request)

    return jsonify({"message": "Trade request created!", "locked": 1})


@app.route('/api/trade/fulfill', methods=['PUT'])
def fulfill_trade():

    data = request.get_json()
    trade_id = data.get('trade_id')
    fulfilling_ids = data.get('fulfilling_ids', [])

    if not trade_id or len(fulfilling_ids) != 1:
        return jsonify({"message": "Invalid trade request"}), 400

    trade = mongo.db.trade.find_one({'_id': ObjectId(trade_id)})

    if not trade or trade.get('looking_for_count') != 1:
        return jsonify({"message": "Trade not found"}), 404

    original_id = ObjectId(trade['offering_ids'][0])
    fulfilling_obj_id = ObjectId(fulfilling_ids[0])

    # Swap
    mongo.db.pokemon.update_one(
        {'_id': fulfilling_obj_id, 'player': session.get('email')},
        {'$set': {'player': trade['creator'], 'locked': False}}
    )

    mongo.db.pokemon.update_one(
        {'_id': original_id},
        {'$set': {'player': session.get('email'), 'locked': False}}
    )

    mongo.db.trade.delete_one({'_id': ObjectId(trade_id)})

    return jsonify({
        "message": "Trade successful!",
        "traded_in": str(original_id),
        "traded_out": str(fulfilling_obj_id)
    })


# ============================================
# USER INFO API
# ============================================

@app.route('/api/user/info', methods=['GET'])
def get_user_info():

    user = mongo.db.players.find_one({'email': session.get('email')})
    if not user:
        return jsonify({"message": "User not found"}), 404
    
    return jsonify({
        "email": user['email'],
        "level": user['level'],
        "xp": user['xp'],
        "xp_to_next_level": LEVEL_UP_XP - user['xp'],
        "wins": user['wins'],
        "losses": user['losses']
    })


# ============================================
# BATTLE SYSTEM (SocketIO)
# ============================================

def simulate_battle(player_mon, opponent_mon):

    p1 = {"doc": player_mon, "hp": player_mon['hp']}
    p2 = {"doc": opponent_mon, "hp": opponent_mon['hp']}

    attacker = p1 if p1['doc']['atk'] >= p2['doc']['atk'] else p2
    defender = p2 if attacker is p1 else p1

    for _ in range(20):
        if p1['hp'] <= 0 or p2['hp'] <= 0:
            break

        dmg = max(1, attacker['doc']['atk'] - (defender['doc']['def'] // 2))
        defender['hp'] -= dmg

        attacker, defender = defender, attacker

    return p1['hp'] > 0


def update_user_xp(user_name, is_winner):

    user = mongo.db.players.find_one({"email": user_name})
    if not user:
        return 0, 0

    xp_gained = XP_PER_WIN if is_winner else 10
    new_xp = user['xp'] + xp_gained
    new_level = user['level']
    level_increase = 0

    while new_xp >= LEVEL_UP_XP:
        new_xp -= LEVEL_UP_XP
        new_level += 1
        level_increase += 1

    mongo.db.players.update_one(
        {"email": user_name},
        {
            '$set': {"level": new_level, "xp": new_xp},
            '$inc': {'wins': 1 if is_winner else 0, 'losses': 1 if not is_winner else 0}
        }
    )

    return xp_gained, level_increase


@socketio.on('connect')
def handle_connect():
    user_email = session.get('email')
    app.logger.info(f"Client Connected: SID={request.sid}, User={user_email}")


@socketio.on('disconnect')
def handle_disconnect():
    global BATTLE_QUEUE
    BATTLE_QUEUE = [p for p in BATTLE_QUEUE if p['sid'] != request.sid]


@socketio.on('join_queue')
def handle_join_queue():
    global BATTLE_QUEUE

    user = session.get('email')
    sid = request.sid

    if any(p['sid'] == sid for p in BATTLE_QUEUE):
        return

    BATTLE_QUEUE.append({'name': user, 'sid': sid})

    if len(BATTLE_QUEUE) < 2:
        pos = len(BATTLE_QUEUE)
        emit('queue_update', {'message': "Searching...", 'position': pos}, room=sid)
        return
    
    # Match found
    p1 = BATTLE_QUEUE.pop(0)
    p2 = BATTLE_QUEUE.pop(0)

    p1_mons = list(mongo.db.pokemon.find({"player": p1['name'], "locked": False}))
    p2_mons = list(mongo.db.pokemon.find({"player": p2['name'], "locked": False}))

    if not p1_mons or not p2_mons:
        emit('queue_error', {'message': "No available Pokemon!"}, room=p1['sid'])
        return

    m1 = random.choice(p1_mons)
    m2 = random.choice(p2_mons)

    winner_is_p1 = simulate_battle(m1, m2)

    p1_xp, p1_lvl = update_user_xp(p1['name'], winner_is_p1)
    p2_xp, p2_lvl = update_user_xp(p2['name'], not winner_is_p1)

    winner_name = p1['name'] if winner_is_p1 else p2['name']

    for player in [p1, p2]:
        is_win = player['name'] == winner_name

        payload = {
            "status": "complete",
            "message": "WIN!" if is_win else "LOSS!",
            "result": {
                "player_mon": m1['name'] if player is p1 else m2['name'],
                "opponent_mon": m2['name'] if player is p1 else m1['name'],
                "player_xp_gain": p1_xp if player is p1 else p2_xp,
                "player_level_up": p1_lvl if player is p1 else p2_lvl,
                "result_message": "WIN!" if is_win else "LOSS!"
            }
        }

        socketio.emit('battle_result', payload, room=player['sid'])

# ============================================
# 2PC PARTICIPANT: /vote (HTTP)
# ============================================

@app.route('/vote', methods=['POST'])
def vote():

    data = request.get_json(force=True)
    tx_id = data.get("tx_id")

    logging.info(
        f"Phase Voting of Node {NODE_ID} receives RPC VoteRequest "
        f"from Phase Voting of Node coordinator."
    )

    return jsonify({"tx_id": tx_id, "vote": "commit"})


# ============================================
# 2PC PARTICIPANT: gRPC Decision Phase
# ============================================

class DecisionPhaseServicer(two_pc_pb2_grpc.DecisionServiceServicer):

    def GlobalCommit(self, request, context):
        logging.info(
            f"Phase Decision of Node {NODE_ID} receives RPC GlobalCommit "
            f"from Phase Decision of Node coordinator."
        )
        return two_pc_pb2.DecisionReply(status="ack-commit")

    def GlobalAbort(self, request, context):
        logging.info(
            f"Phase Decision of Node {NODE_ID} receives RPC GlobalAbort "
            f"from Phase Decision of Node coordinator."
        )
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


# ============================================
# 2PC COORDINATOR: /start_tx
# ============================================

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
            v = resp.json().get("vote", "abort")

            votes.append({"participant": p, "vote": v})
            if v != "commit":
                all_commit = False

        except Exception as e:
            logging.error(f"[COORDINATOR] ERROR contacting {p}: {e}")
            votes.append({"participant": p, "vote": "abort"})
            all_commit = False

    decision = "commit" if all_commit else "abort"
    logging.info(f"[COORDINATOR] tx={tx_id} decision={decision}")

    send_global_decision(decision, tx_id)

    return jsonify({"tx_id": tx_id, "decision": decision, "votes": votes})


# ============================================
# 2PC: SEND GLOBAL DECISION TO PARTICIPANTS
# ============================================

def send_global_decision(decision, tx_id):

    for p in PARTICIPANTS:
        channel = grpc.insecure_channel(f"{p}:6000")
        stub = two_pc_pb2_grpc.DecisionServiceStub(channel)

        req = two_pc_pb2.DecisionRequest(tx_id=tx_id)

        if decision == "commit":
            logging.info(
                f"Phase Decision of Node coordinator sends RPC GlobalCommit "
                f"to Phase Decision of Node {p}."
            )
            stub.GlobalCommit(req)
        else:
            logging.info(
                f"Phase Decision of Node coordinator sends RPC GlobalAbort "
                f"to Phase Decision of Node {p}."
            )
            stub.GlobalAbort(req)


# ============================================
# MAIN ENTRY
# ============================================

if __name__ == '__main__':
    import threading
    threading.Thread(target=start_grpc_server, daemon=True).start()
    socketio.run(app, host="0.0.0.0", port=5000)

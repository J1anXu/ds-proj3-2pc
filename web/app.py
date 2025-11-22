# Import for converting string IDs back to ObjectId
from bson.objectid import ObjectId 
from flask import Flask, request, render_template, redirect, url_for, flash, jsonify, session
from flask_cors import CORS
from flask_pymongo import PyMongo
from flask_socketio import SocketIO, emit
from flask_session import Session 
from math import ceil 
from datetime import datetime
import random
import os
import redis
import uuid
import requests

# -----------------------------
#        NODE ROLE SETUP
# -----------------------------
ROLE = os.getenv("ROLE", "participant")  # "coordinator" or "participant"
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
        app.logger.info(f"ERROR: Could not initialize Redis client from URL: {e}")

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
#   GLOBAL GAME CONFIG
# -----------------------------
BATTLE_QUEUE = [] 
BATTLES_IN_PROGRESS = {} 
XP_PER_WIN = 100
LEVEL_UP_XP = 100

# -----------------------------
#         ROUTES
# -----------------------------

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

    try:
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
    except Exception as e:
        flash(f'An error occurred during registration: {e}', 'error')

    return redirect(url_for('index'))

@app.route('/login', methods=['POST'])
def login():
    email = request.form.get('email')
    password = request.form.get('password')

    if not email or not password:
        flash('Both email and password are required.', 'error')
        return redirect(url_for('index'))

    user = mongo.db.players.find_one({'email': email})

    if user and user['password'] == password:
        session['email'] = email   
        flash(f'Successfully logged in as {email}!', 'success')
        return redirect(url_for('home'))
    else:
        flash('Invalid email or password.', 'error')

    return redirect(url_for('index'))

@app.route('/logout')
def logout():
    session.pop('email', None)
    return redirect(url_for('login'))

@app.route('/home')
def home():
    return render_template('home.html')

@app.route('/api/inventory', methods=['GET'])
def get_inventory():
    inventory = list(mongo.db.pokemon.find({'player': session.get('email')}))
    return jsonify(inventory)

@app.route('/api/release', methods=['DELETE'])
def release_pokemon():
    data = request.get_json()
    ids = data.get('ids', [])
    object_ids = [ObjectId(x) for x in ids]

    result = mongo.db.pokemon.delete_many({'_id': {'$in': object_ids}})

    return jsonify({"message": "OK", "deleted": result.deleted_count})

@app.route('/api/gatcha', methods=['POST'])
def run_gatcha():
    gatcha_pool = [
        {"player":session.get('email'), "name": "Squirtle", "atk": 48, "def": 65, "hp": 44, "locked":False, "image": "https://raw.githubusercontent.com/PokeAPI/sprites/master/sprites/pokemon/7.png"},
        {"player":session.get('email'), "name": "Jigglypuff", "atk": 45, "def": 20, "hp": 115, "locked":False, "image": "https://raw.githubusercontent.com/PokeAPI/sprites/master/sprites/pokemon/39.png"},
        {"player":session.get('email'), "name": "Snorlax", "atk": 110, "def": 65, "hp": 160, "locked":False, "image": "https://raw.githubusercontent.com/PokeAPI/sprites/master/sprites/pokemon/143.png"}
    ]
    new_poke = random.choice(gatcha_pool)
    mongo.db.pokemon.insert_one(new_poke)

    return jsonify({"message": "Gatcha successful!", "new_pokemon": new_poke})

# -----------------------------
#         2PC PARTICIPANT
# -----------------------------

@app.route('/vote', methods=['POST'])
def vote():
    """参与者的投票接口"""
    data = request.get_json(force=True)
    tx_id = data.get("tx_id")

    # 你可以加入任何本地检查逻辑
    vote_result = "commit"

    print(f"[PARTICIPANT] vote for tx={tx_id} -> {vote_result}")
    return jsonify({"tx_id": tx_id, "vote": vote_result})


# -----------------------------
#         2PC COORDINATOR
# -----------------------------

@app.route('/start_tx', methods=['POST'])
def start_tx():
    """协调者发起投票阶段"""
    if ROLE != "coordinator":
        return jsonify({"error": "Only coordinator can start tx"}), 403

    data = request.get_json(force=True)
    tx_id = data.get("tx_id", str(uuid.uuid4()))
    payload = data.get("payload", {})

    votes = []
    all_commit = True

    for p in PARTICIPANTS:
        url = f"http://{p}/vote"
        try:
            resp = requests.post(url, json={"tx_id": tx_id, "payload": payload}, timeout=2)
            result = resp.json().get("vote", "abort")
            votes.append({"participant": p, "vote": result})

            if result != "commit":
                all_commit = False
        except Exception as e:
            print(f"[COORDINATOR] ERROR contacting {p}: {e}")
            votes.append({"participant": p, "vote": "abort"})
            all_commit = False

    decision = "commit" if all_commit else "abort"
    print(f"[COORDINATOR] tx={tx_id} decision={decision}")

    return jsonify({"tx_id": tx_id, "decision": decision, "votes": votes})


# -----------------------------
#       SOCKET.IO EVENTS
# -----------------------------
@socketio.on('connect')
def handle_connect():
    user_email = session.get('email')
    app.logger.info(f"Client Connected: SID={request.sid}, User={user_email}")

@socketio.on('disconnect')
def handle_disconnect():
    global BATTLE_QUEUE
    player_name = session.get('email')
    if player_name:
        BATTLE_QUEUE = [p for p in BATTLE_QUEUE if p['sid'] != request.sid]

# (省略：你的 match/battle 逻辑原样保留，不变)

# -----------------------------
#          MAIN
# -----------------------------
if __name__ == '__main__':
    socketio.run(app, host="0.0.0.0", port=5000)

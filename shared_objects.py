# shared_objects.py
from flask_socketio import SocketIO

# SocketIO ko initialize karein, lekin app ke bina
socketio = SocketIO(ping_interval=25, ping_timeout=60, cors_allowed_origins="*")

# In-memory cache
market_data_cache = {}
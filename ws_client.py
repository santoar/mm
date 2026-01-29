import os
import csv
import time
import json
import struct
import asyncio
import traceback
import threading
import websockets
from datetime import datetime
from helper import get_setting, find_open_position, save_trade_data
from shared_objects import socketio, market_data_cache
from supabase import create_client
from master_data import index_symbol_to_id
from time_utils import get_current_ist_time, format_ist_time, convert_utc_to_ist

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)


active_option_ids = set()
last_printed_ltp = {}

def set_socketio(socketio_instance):
    global socketio
    socketio = socketio_instance



def get_dhan_client_id(app):
    with app.app_context():
        return get_setting("dhan_client_id")

def get_dhan_access_token(app):
    with app.app_context():
        return get_setting("dhan_access_token")

def get_index_security_ids():
    return [sec_id for sec_id in index_symbol_to_id.values() if sec_id is not None]


def get_last_ltp(security_id):
    return market_data_cache.get(security_id)

def get_active_option_security_ids(app):
    from datetime import date
    today = date.today().isoformat()
    resp = supabase.table("trade_log") \
        .select("option_security_id") \
        .eq("order_status", "open") \
        .eq("timestamp", today) \
        .execute()
    trades = resp.data or []
    option_security_ids = {trade["option_security_id"] for trade in trades if trade.get("option_security_id")}
    return list(option_security_ids)

def refresh_active_option_ids(app):
    global active_option_ids
    with app.app_context():
        active_option_ids = set(get_active_option_security_ids(app))
        print("get_active_option_security_ids output:", active_option_ids)
print_counter = 0

last_printed_ltp = {}

def decode_message(msg_bytes):
    global print_counter
    try:
        if len(msg_bytes) < 8:
            return

        resp_code = msg_bytes[0]
        security_id = struct.unpack('<i', msg_bytes[4:8])[0]
        security_id_str = str(security_id)

        if resp_code in [2, 3, 4]: 
            ltp = round(struct.unpack('<f', msg_bytes[8:12])[0], 2)
            market_data_cache[security_id_str] = ltp
            print_counter += 1

            if security_id_str not in last_printed_ltp or last_printed_ltp[security_id_str] != ltp:
                if security_id_str == '13':
                    print(f"NIFTY: {ltp:.2f}")
                else:
                    print(f"Option {security_id_str}: {ltp}")
                
                last_printed_ltp[security_id_str] = ltp
            if socketio:
                
                if security_id_str == '13' or security_id_str in active_option_ids:
                    socketio.emit(
                        'ltp_update',
                        {security_id_str: {"ltp": ltp, "pnl": 0.0}},
                        namespace='/io'
                    )
        
    except Exception as e:
        print(f"[ERROR] decode failed: {e}")

    

async def subscribe_and_listen(app): 
    last_subscribed_ids = set()
    last_db_check_time = 0 
    check_interval = 10 
    
    while True:
        try:
            token = get_dhan_access_token(app)
            client_id = get_dhan_client_id(app)
            ws_url = f"wss://api-feed.dhan.co?version=2&token={token}&clientId={client_id}&authType=2"
            
            async with websockets.connect(ws_url) as ws:
                print("[INFO] WebSocket connected successfully with fresh token")
                last_subscribed_ids = set()
                
                while True:
                    current_time = time.time()
                    if current_time - last_db_check_time > check_interval:
                        refresh_active_option_ids(app)
                        last_db_check_time = current_time

                    index_ids = {str(sid) for sid in index_symbol_to_id.values() if sid}
                    if not index_ids:
                        index_ids = {"13"} 
                    
                    combined_ids = active_option_ids.union(index_ids)

                    if combined_ids and (combined_ids != last_subscribed_ids):
                        instrument_list = []
                        for sid in combined_ids:
                            
                            seg = "IDX_I" if sid == "13" else "NSE_FNO"
                            instrument_list.append({
                                "ExchangeSegment": seg,
                                "SecurityId": str(sid)
                            })

                        subscribe_msg = {
                            "RequestCode": 15,
                            "InstrumentCount": len(instrument_list),
                            "InstrumentList": instrument_list
                        }
                        
                        await ws.send(json.dumps(subscribe_msg))
                        print(f"[DEBUG] Subscribed to: {combined_ids}")
                        last_subscribed_ids = set(combined_ids)

                    try:
                        message = await asyncio.wait_for(ws.recv(), timeout=5)
                        decode_message(message)
                    except asyncio.TimeoutError:
                        await ws.ping()
                        
        except Exception as e:
            print(f"[ERROR] Connection lost: {e}. Retrying with fresh credentials in 5s...")
            await asyncio.sleep(5)

def start_ws_loop(app):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    loop.run_until_complete(subscribe_and_listen(app)) 


def start_ws_thread(app):
    print("Starting WebSocket client thread...")
    t = threading.Thread(target=start_ws_loop, daemon=True)
    t.start()
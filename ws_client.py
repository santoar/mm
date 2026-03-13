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
manual_option_ids = set()

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

def get_last_ltp(security_id):
    return market_data_cache.get(security_id)

def subscribe_symbols(symbols_list):
    global manual_option_ids
    try:
        updated = False
        for sid in symbols_list:
            sid_str = str(sid)
            if sid_str not in manual_option_ids and sid_str not in active_option_ids:
                manual_option_ids.add(sid_str)
                updated = True
        
        if updated:
            print(f"[INFO] New Subscription Added: {symbols_list}")
    except Exception as e:
        print(f"[ERROR] Failed to add symbols: {e}")

def get_active_option_security_ids(app):
    from datetime import date
    today = date.today().isoformat()
    try:
        resp = supabase.table("trade_log") \
            .select("option_security_id") \
            .eq("order_status", "open") \
            .eq("timestamp", today) \
            .execute()
        trades = resp.data or []
        return {str(trade["option_security_id"]) for trade in trades if trade.get("option_security_id")}
    except Exception as e:
        print(f"[WS CLIENT] DB Error: {e}")
        return set()
        
def refresh_active_option_ids(app):
    global active_option_ids, manual_option_ids
    with app.app_context():
        db_ids = get_active_option_security_ids(app)
        active_option_ids = set(db_ids)
        manual_option_ids = manual_option_ids - active_option_ids

def decode_message(msg_bytes):
    try:
        if len(msg_bytes) < 8: return
        resp_code = msg_bytes[0]
        if resp_code in [2, 3, 4]: 
            security_id = struct.unpack('<i', msg_bytes[4:8])[0]
            ltp = round(struct.unpack('<f', msg_bytes[8:12])[0], 2)
            sec_str = str(security_id)
            
            
            market_data_cache[sec_str] = ltp
            
            all_index_ids = {str(v) for v in index_symbol_to_id.values() if v}
            
            if sec_str not in last_printed_ltp or last_printed_ltp[sec_str] != ltp:
                if sec_str in all_index_ids:
                    
                    name = next((k for k, v in index_symbol_to_id.items() if str(v) == sec_str), "INDEX")
                    print(f"{name} ({sec_str}): {ltp:.2f}")
                else:
                    print(f"OPTION ({sec_str}): {ltp:.2f}")
                last_printed_ltp[sec_str] = ltp
                
            if socketio:
                socketio.emit('ltp_update', {sec_str: {"ltp": ltp, "pnl": 0.0}}, namespace='/io')
    except Exception:
        pass

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
                print("[INFO] WebSocket Connected!")
                last_subscribed_ids = set()
                
                while True:
                    current_time = time.time()
                                        
                    if current_time - last_db_check_time > check_interval:
                        refresh_active_option_ids(app)
                        last_db_check_time = current_time

                    index_ids = {str(sid) for sid in index_symbol_to_id.values() if sid is not None}
                              
                    combined_ids = active_option_ids.union(index_ids).union(manual_option_ids)

                    if combined_ids and (combined_ids != last_subscribed_ids):
                        instrument_list = []
                        for sid in combined_ids:
                            if sid in index_ids:
                                seg = "IDX_I"
                            elif int(sid) > 500000: 
                                seg = "BSE_FNO"
                            else:
                                seg = "NSE_FNO"
                                
                            instrument_list.append({"ExchangeSegment": seg, "SecurityId": str(sid)})

                        req = {"RequestCode": 15, "InstrumentCount": len(instrument_list), "InstrumentList": instrument_list}
                        await ws.send(json.dumps(req))
                        print(f"[DEBUG] Subscribed Total: {len(combined_ids)} | Indices: {len(index_ids)}")
                        last_subscribed_ids = set(combined_ids)

                    try:
                        message = await asyncio.wait_for(ws.recv(), timeout=1)
                        decode_message(message)
                    except asyncio.TimeoutError:
                        pass
                        
        except Exception as e:
            print(f"[ERROR] WS Disconnected: {e}. Retrying in 5s...")
            await asyncio.sleep(5)


def start_ws_loop(app):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(subscribe_and_listen(app)) 


def start_ws_thread(app):
    print("Starting WebSocket client thread...")
    t = threading.Thread(target=start_ws_loop, daemon=True)
    t.start()

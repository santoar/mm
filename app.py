import os
import time
import pytz
import logging
import threading
import traceback
from threading import Timer
import requests
import websockets
import pandas as pd
from datetime import datetime, time as dt_time
from flask import Flask, request, jsonify, render_template, send_from_directory
from supabase import create_client
from functools import wraps
from google.oauth2 import service_account
from googleapiclient.discovery import build
from extensions import db
from master_data import (
    load_master_data,
    get_lot_size,
    trading_symbols,
    get_expiry_date,
    get_atm_strike,
    loaded_expiry_date,
    check_and_reload_expiry,
    cached_find_option_security,
    find_option_security_id_fast,
    get_symbol_from_security_id,
    get_lot_size_for_security,
    get_strike_for_security,
    get_option_type_for_security
)
from helper import (
    place_order,
    check_order_status,
    is_duplicate_trade,
    load_global_settings_optimized,
    refresh_settings_cache,
    get_dhan_access_token,
    get_dhan_client_id,
    global_settings_cache,
    update_setting,
    round_to_0_05,
    get_expiry_date,
    get_index_ltp,
    get_best_bid_ask,
    get_setting,
    find_open_position,
    find_open_position_with_broker_sync,
    build_trade_data_for_order,
    periodic_cache_refresh,
    check_broker_open_position,
    place_order_with_confirmation,
    parse_executed_price,
    exit_position_core_logic,
    save_or_update_trade_data,
    make_broker_style_symbol,
    fetch_broker_positions_list    
)
from ws_client import refresh_active_option_ids
from time_utils import get_current_ist_time, format_ist_time, convert_utc_to_ist
from shared_objects import socketio, market_data_cache

logging.basicConfig(level=logging.WARNING)
logging.getLogger('werkzeug').setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger('httpx').setLevel(logging.WARNING)   
logging.getLogger('websockets').setLevel(logging.WARNING)

SUPABASE_URL = "https://iiweksphelhuapiwrtle.supabase.co"
SUPABASE_KEY = "sb_secret_42JXsFElc315ThoG966u0g_paImAF-Y"

if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("SUPABASE_URL and SUPABASE_KEY must be set as environment variables.")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def token_refresh_loop():
    while True:
        refresh_settings_cache()
        time.sleep(3600)

#--- Flask Setup ---
app = Flask(__name__)

batched_trades = []
atm_option_security_cache = {}


def handle_exit(symbol, option_type=None, strike=None):
    paper_trade = get_setting("paper_trade")
    if isinstance(paper_trade, str):
        paper_trade = paper_trade.lower() == 'true'

    open_pos = None
    if not strike:
        if option_type:
            open_pos = find_open_position(symbol, option_type=option_type)
        else:
            open_pos = find_open_position(symbol)
        if not open_pos:
            return {"status": "No open position to exit"}
        
        option_type_db = open_pos.get("option_type")
        strike_db = open_pos.get("strike")
    else:
        open_pos = find_open_position(symbol, option_type, strike)
        if not open_pos:
            return {"status": "No matching open position"}
        
        option_type_db = option_type
        strike_db = strike

    if paper_trade:
        response, status_code = exit_position_core_logic(symbol, option_type_db, strike_db)
        return {"status": response, "code": status_code}
    else:
        open_pos_live = check_broker_open_position(symbol, option_type_db, strike_db)
        if not open_pos_live:
            return {"status": "Position closed or not found in broker; no exit order"}
        response, status_code = exit_position_core_logic(symbol, option_type_db, strike_db)
        return {"status": response, "code": status_code}


def update_atm_option_cache_for_symbol(symbol, ltp, expiry, strike_selection=0):
   
    try:
        ce_sec_id, ce_strike, ce_symbol = find_option_security_id_fast(symbol, expiry, ltp, 'CE', strike_selection)
        if ce_sec_id:
            atm_option_security_cache[(symbol, "CE")] = {
                'SECURITY_ID': ce_sec_id,
                'STRIKE': ce_strike,
                'SYMBOL_NAME': ce_symbol
            }

        pe_sec_id, pe_strike, pe_symbol = find_option_security_id_fast(symbol, expiry, ltp, 'PE', strike_selection)
        if pe_sec_id:
            atm_option_security_cache[(symbol, "PE")] = {
                'SECURITY_ID': pe_sec_id,
                'STRIKE': pe_strike,
                'SYMBOL_NAME': pe_symbol
            }
        print(f"ATM Cached for {symbol}: CE Security ID = {ce_sec_id}, PE Security ID = {pe_sec_id}")
        logging.info(f"Cached ATM options for {symbol}: CE {ce_sec_id}, PE {pe_sec_id}")
    except Exception as e:
        logging.error(f"Error updating ATM option cache for {symbol}: {e}")

def update_all_atm_option_cache():
    expiry = get_expiry_date()
    if not expiry:
        logging.warning("Expiry date not found, skipping ATM cache update.")
        return
    for symbol in trading_symbols:
        ltp = get_index_ltp(symbol)
        if ltp:
            update_atm_option_cache_for_symbol(symbol, ltp, expiry)
    logging.info("ATM option security cache updated for all symbols.")

def start_atm_cache_updater(interval_sec=120):
    def run():
        while True:
            try:
                update_all_atm_option_cache()
            except Exception as e:
                logging.error(f"ATM cache update error: {e}")
            time.sleep(interval_sec)
    thread = threading.Thread(target=run, daemon=True)
    thread.start()

def expiry_check_loop():
    while True:
        try:
            check_and_reload_expiry()
        except Exception as e:
            logging.error(f"Error in expiry_check_loop: {e}")
        time.sleep(600)  

def add_trade_to_batch(trade_data, force_save=False):
    batched_trades.append(trade_data)
    if force_save or len(batched_trades) >= 1:
        batch = batched_trades.copy()
        batched_trades.clear()
        try:
            supabase.table("trade_log").insert(batch).execute()
            #logging.info(f"Batch saved {len(batch)} trades.")
        except Exception as e:
            logging.error(f"Batch save failed: {e}")

def get_dhan_access_token():
    resp = supabase.table("settings").select("value").eq("key", "dhan_access_token").execute()
    data = resp.data
    if data and len(data) > 0:
        return data[0]["value"]
    else:
        return None

def place_order_background(opt_sec_id, total_quantity, trade_data, signal):
    def task():
        try:
            trade_id = trade_data.get("id")
            if not trade_id:
                logging.error("Missing 'id' in trade_data, cannot update database.")
                return

            if trade_data.get("trade_type") == "paper":
                supabase.table("trade_log")\
                    .update({"order_status": "open"})\
                    .eq("id", trade_id).execute()
            else:
                supabase.table("trade_log")\
                    .update({"order_status": "transit"})\
                    .eq("id", trade_id).execute()

                order_response = place_order(opt_sec_id, "BUY", qty=total_quantity)
                if not order_response or "orderId" not in order_response:
                    supabase.table("trade_log")\
                        .update({"order_status": "failed"})\
                        .eq("id", trade_id).execute()
                    return

                order_id = order_response["orderId"]
                status = check_order_status(order_id)

                if status == "CONFIRM":
                    executed_price = trade_data.get("entry_price") if trade_data else None

                    update_data = {
                        "order_status": "open",
                        "order_id": order_id
                    }
                    if executed_price is not None:
                        if trade_data.get("trade_type") == "paper":
                            capital_used = executed_price * trade_data.get("quantity", 1) * trade_data.get("lot_size", 1)
                        else:
                            capital_used = executed_price * trade_data.get("quantity", 1)
                        update_data["capital_used"] = capital_used
    
                    supabase.table("trade_log")\
                        .update(update_data)\
                        .eq("id", trade_id).execute()

                elif status in ["FAILED", "REJECTED"]:
                    supabase.table("trade_log")\
                        .update({"order_status": "failed"})\
                        .eq("id", trade_id).execute()

                else:
                    supabase.table("trade_log")\
                        .update({"order_status": "transit"})\
                        .eq("id", trade_id).execute()

        except Exception as e:
            logging.error(f"Background order placement error: {e}")
            trade_id = trade_data.get("id")
            if trade_id:
                supabase.table("trade_log")\
                    .update({"order_status": "failed"})\
                    .eq("id", trade_id).execute()

    threading.Thread(target=task, daemon=True).start()

def is_market_open():
    ist = pytz.timezone('Asia/Kolkata')
    now = datetime.now(ist).time()

    market_open_time = dt_time(9, 15)  
    market_close_time = dt_time(15, 30) 

    if market_open_time <= now <= market_close_time:
        return True
    else:
        return False

def get_active_option_security_ids():
    today = datetime.now().date().isoformat()
    resp = supabase.table("trade_log") \
        .select("option_security_id") \
        .eq("order_status", "open") \
        .eq("timestamp", today) \
        .execute()
    trades = resp.data or []
    option_security_ids = {trade["option_security_id"] for trade in trades if trade.get("option_security_id")}
    return list(option_security_ids)

def get_all_settings():
    resp = supabase.table("settings").select("*").execute()
    settings_list = resp.data or []
    settings_dict = {item['key']: item['value'] for item in settings_list}
    return settings_dict

def authenticate(admin_only=False):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                auth = request.authorization
                
                db_username = get_setting("USERNAME")
                db_password = get_setting("PASSWORD")
                
                if not auth:
                    return jsonify({"error": "Unauthorized"}), 401
                if auth.username != db_username or auth.password != db_password:
                    return jsonify({"error": "Unauthorized"}), 401

                return func(*args, **kwargs)
            except (KeyError, AttributeError):
                return jsonify({"error": "Authentication settings not found."}), 500
        wrapper.__name__ = func.__name__ + "_auth_wrapper"
        return wrapper
    return decorator

def polling_loop():
    while True:
        poll_and_update_transit_orders()
        time.sleep(20)  

def poll_and_update_transit_orders():
    logging.info("Polling started")
    today = datetime.now().date().isoformat()
    try:
        resp = supabase.table("trade_log").select("*")\
            .in_("order_status", ["open", "transit", "pending"])\
            .in_("trade_type", ["live", "paper"]).execute()
        trades = resp.data or []
    except Exception as e:
        logging.error(f"Failed to fetch trades from DB: {e}")
        return False

    logging.info(f"Found {len(trades)} open/pending/transit trades")
    updated = False

    access_token = get_setting("dhan_access_token")
    client_id = get_setting("dhan_client_id")

    for trade in trades:
        if trade.get("trade_type") == "paper":
            trade_id = trade.get("id") 
            logging.info(f"Paper trade detected, trade_id: {trade_id}. Skipping API order_id check.")
            continue 
        else:
            order_id = trade.get("order_id")
            security_id = trade.get("option_security_id")
            logging.info(f"Checking order_id: {order_id}")
            if not order_id:
                logging.warning(f"Missing order_id for live trade, skipping: {trade}")
                continue

        executed_price = None
        status = None
        max_retries = 3

        for i in range(max_retries):
            url = f"https://api.dhan.co/v2/orders/{order_id}"
            headers = {
                "Content-Type": "application/json",
                "access-token": access_token,
                "client-id": client_id,
            }
            try:
                response = requests.get(url, headers=headers, timeout=5)
                if response.status_code != 200:
                    logging.error(f"Failed to fetch order details for {order_id}: {response.status_code}")
                    break
                data = response.json()
                if isinstance(data, list):
                    data = data[0] if data else {}
                status = data.get("orderStatus", "").upper()
                executed_price = parse_executed_price(data, str(security_id))
            except Exception as e:
                logging.error(f"Exception fetching order status for {order_id}: {e}")
                break

            logging.info(f"Order {order_id} status: {status}, executed price: {executed_price}")
            
            def map_status(status):
                if status in ["REJECTED", "FAILED", "CANCELLED"]:
                    return "failed"
                if status == "CONFIRM":
                    return "pending"
                if status in ["TRADED", "PART_TRADED"]:
                    return "open"
                return "unknown"
            
            mapped_status = map_status(status)
            
            if mapped_status == "failed":
                try:
                    resp = supabase.table("trade_log").update({"order_status": "failed"}).eq("id", trade["id"]).execute()
                    if hasattr(resp, "error") and resp.error is not None:
                        logging.error(f"DB update failed for failed order {order_id}: {resp.error}")
                except Exception as e:
                    logging.error(f"DB update exception for failed order {order_id}: {e}")
                updated = True
                break
            
            elif mapped_status == "pending":
            
                try:
                    resp = supabase.table("trade_log").update({
                        "order_status": "pending",
                    }).eq("id", trade["id"]).execute()
                    if hasattr(resp, "error") and resp.error is not None:
                        logging.error(f"DB update failed for failed order {order_id}: {resp.error}")
                    
                    if hasattr(resp, "error") and resp.error is not None:
                        logging.error(f"DB update failed for pending order {order_id}: {resp.error}")
                    socketio.emit('order_update', {
                        'trade_id': trade["id"],
                        'order_status': "pending",
                    })
                except Exception as e:
                    logging.error(f"DB update/socket emit exception for pending order {order_id}: {e}")
                updated = True
                break
            
            elif mapped_status == "open":
                if executed_price and float(executed_price) > 0:
                    try:
                        resp = supabase.table("trade_log").update({
                            "order_status": "open",
                            "entry_price": float(executed_price),
                        }).eq("id", trade["id"]).execute()
                        if hasattr(resp, "error") and resp.error is not None:
                            logging.error(f"DB update failed for open order {order_id}: {resp.error}")
                        socketio.emit('order_update', {
                            'trade_id': trade["id"],
                            'order_status': "open",
                            'entry_price': executed_price
                        })
                        updated = True
                        break
                    except Exception as e:
                        logging.error(f"DB update/socket emit exception for open order {order_id}: {e}")
                else:
                    logging.warning(f"Retry {i+1}/{max_retries} waiting for executed price")
                    time.sleep(3)

       
    try:
        broker_positions = fetch_broker_positions_list()
        broker_order_ids = {str(pos.get("orderId")): pos for pos in broker_positions if pos.get("orderId")}

        open_trades = supabase.table("trade_log").select("*")\
            .in_("order_status", ["open", "pending"])\
            .eq("trade_type", "live").execute().data or []

        for trade in open_trades:
            order_id = str(trade.get("order_id", "")).strip()
            broker_pos = broker_order_ids.get(order_id)
            
            if broker_pos and broker_pos.get("netQty", 0) != 0:
                if trade.get("order_status") != "open":
                    supabase.table("trade_log").update({
                        "order_status": "open",
                        "exit_price": 0,
                        "exit_time": "00:00:00",
                        "reason": ""
                    }).eq("id", trade["id"]).execute()
                    socketio.emit('order_update', {
                        'trade_id': trade["id"],
                        'order_status': "open",
                    })
                continue

            if broker_pos and (broker_pos.get("netQty", 0) == 0 or broker_pos.get("positionType", "").strip().upper() == "CLOSED"):
                retries = 2
                found_closed = True
                for i in range(retries):
                    time.sleep(2)  # Small delay for re-poll
                    fresh_positions = fetch_broker_positions_list()
                    fresh_pos = {str(pos.get("orderId")): pos for pos in fresh_positions if pos.get("orderId")}
                    bp = fresh_pos.get(order_id)
                    if bp and (bp.get("netQty", 0) != 0 and bp.get("positionType", "").strip().upper() != "CLOSED"):
                        found_closed = False
                        break
                if found_closed:
                    logging.info(f"Verified closing trade ID {trade['id']} (order_id {order_id}) at broker.")
                    supabase.table("trade_log").update({
                        "order_status": "closed",
                        "exit_time": broker_pos.get("sellTime", ""),
                        "exit_price": broker_pos.get("sellAvg", 0)
                    }).eq("id", trade["id"]).execute()
                    socketio.emit('order_update', {
                        'trade_id': trade["id"],
                        'order_status': "closed"
                    })
                    continue

            if order_id not in broker_order_ids:
                logging.info(f"Trade id {trade['id']} with order_id {order_id} not found in broker positions. No status update being made.")
                continue

    except Exception as e:
        logging.error(f"Error during open-trade synchronization: {e}")

    logging.info("Polling completed")
    return updated

# --- Routes ---
@app.route('/bot/status')
def bot_status():
    return jsonify({"running": True})


@app.route("/reload_settings", methods=["POST"])
def reload_settings_handler():
    try:
        refresh_settings_cache() 
        return jsonify({"status": "Settings reloaded successfully", "settings": global_settings_cache}), 200
    except Exception as e:
        logging.error(f"Failed to reload settings: {e}")
        return jsonify({"error": f"Failed to reload settings: {e}"}), 500

@app.route('/reset_deployment', methods=['POST'])
@authenticate(admin_only=True)   
def reset_deployment():
    try:
        SERVICE_ACCOUNT_FILE = './santoar-8429b8925bb5.json'
        PROJECT_ID = 'santoar'
        REGION = 'europe-west1'
        SERVICE_NAME = 'dhan-bot-service'

        credentials = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE,
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        
        run_client = build('run', 'v1', credentials=credentials)
        parent = f"projects/{PROJECT_ID}/locations/{REGION}/services/{SERVICE_NAME}"
        service = run_client.projects().locations().services().get(name=parent).execute()
        
        container = service["spec"]["template"]["spec"]["containers"][0]
        env_vars = {e['name']: e['value'] for e in container.get("env", [])}
        env_vars["RESET_TRIGGER"] = str(int(time.time()))
        container["env"] = [{"name": k, "value": v} for k, v in env_vars.items()]

        service["spec"]["template"]["spec"]["containers"][0] = container
        op = run_client.projects().locations().services().replaceService(
            name=parent, body=service
        ).execute()
        return jsonify({"status": "Deployment restart triggered", "operation": op.get("metadata", {})}), 200
    except Exception as e:
        logging.error(f"Failed to restart deployment: {e}")
        return jsonify({"error": str(e)}), 500

@app.route("/")
def serve_dashboard():
    return render_template("dashboard.html")

@app.route("/poll_orders", methods=["POST"])
def poll_orders():
    poll_and_update_transit_orders()  
    return jsonify({"status": "polled and updated trade statuses"}), 200

@app.route('/update_ltp', methods=['GET'])
def update_ltp():
    today = datetime.now().date().isoformat()
    resp = supabase.table("trade_log").select(
        "*"
    ).in_("order_status", ["open", "paper"]).eq("timestamp", today).execute()
    live_trades = resp.data or []
    latest_ltps = {}
    import ws_client
    
    for trade in live_trades:
        sec_id = trade.get("option_security_id")
        current_ltp = ws_client.get_last_ltp(sec_id)
        if current_ltp is not None:
            pnl = (current_ltp - trade["entry_price"]) * trade["quantity"] * trade["lot_size"]
            print(f"Emitting LTP update for security_id {sec_id}: LTP {current_ltp}, PNL {pnl}")
            socketio.emit(
                'ltp_update',
                {str(sec_id): {"ltp": current_ltp, "pnl": pnl}},
                namespace='/io'
            )
    return jsonify({"status": "LTP emitted to dashboard."}), 200
    
@app.route("/positions/live", methods=["GET"])
def get_live_positions():
    today = datetime.now().date().isoformat()
    try:
        # Paper trades
        paper_resp = supabase.table("trade_log").select("*")\
            .in_("trade_type", ["paper"])\
            .in_("order_status", ["open", "pending", "transit"])\
            .eq("timestamp", today).execute()
        paper_trades = paper_resp.data or []
        paper_response = []
        try:
            from ws_client import market_data_cache
        except ImportError:
            market_data_cache = {}
        for trade in paper_trades:
            sec_id = str(trade.get("option_security_id"))
            current_ltp = market_data_cache.get(sec_id, 0)
            try:
                entry_price = float(trade.get("entry_price", 0))
                qty = int(trade.get("quantity", 1))
                lot_size = int(trade.get("lot_size", 1))
                pnl = (current_ltp - entry_price) * lot_size * qty 
            except Exception:
                pnl = 0
            actual_quantity = qty 
            paper_response.append({
                "id": trade.get("id"),
                "timestamp": trade.get("timestamp"),
                "symbol": trade.get("symbol"),
                "trading_symbol": trade.get("trading_symbol", ""),
                "option_type": trade.get("option_type"),
                "strike": trade.get("strike"),        
                "quantity": qty,        
                "lot_size": lot_size,
                "entry_time": trade.get("entry_time"),
                "exit_time": trade.get("exit_time"),
                "capital_used": trade.get("capital_used"),
                "entry_price": trade.get("entry_price"),
                "exit_price": trade.get("exit_price"),
                "ltp": current_ltp,
                "pnl": round(pnl, 2),
                "order_status": trade.get("order_status"),
                "exit": ""
            })
        broker_positions = fetch_broker_positions_list() 
        def normalize_symbol(s):
            return str(s).lower().replace("-", "").replace("_", "").split('nov')[0].strip()

        def match_broker_position(trade, broker_positions):
            t_sym = normalize_symbol(trade.get("symbol", ""))    # trade side
            t_opt = str(trade.get("option_type", "")).upper().strip()
            for bpos in broker_positions:
                b_sym = normalize_symbol(bpos.get("tradingSymbol", ""))   # broker side
                b_opt = str(bpos.get("drvOptionType", "")).upper().strip()
                if (b_sym == t_sym) and (b_opt == t_opt) and \
                    (bpos.get("positionType", "").upper() in ["OPEN", "LONG"]) and int(bpos.get("netQty", 0)) > 0:
                    return True
            return False
                
        live_resp = supabase.table("trade_log").select("*")\
            .in_("trade_type", ["live"])\
            .in_("order_status", ["open", "pending", "transit"])\
            .eq("timestamp", today).execute()
        live_trades = live_resp.data or []
        live_response = []
        
        for trade in live_trades:
            if not match_broker_position(trade, broker_positions):
                logging.warning(f"MARKING AS CLOSED - No match on broker: {trade.get('trading_symbol')}/{trade.get('strike')}/{trade.get('option_type')}")
                broker_closed = {
                    "exit_time": get_current_ist_time().strftime("%H:%M:%S"),
                    "exit_price": None
                }
                supabase.table("trade_log").update({
                    "order_status": "closed",
                    "reason": "broker_manual_exit",
                    "exit_time": broker_closed.get("exit_time"),
                    "exit_price": broker_closed.get("exit_price"),
                }).eq("id", trade["id"]).execute()
                continue
            
            sec_id = str(trade.get("option_security_id"))
            current_ltp = market_data_cache.get(sec_id, 0)
            try:
                entry_price = float(trade.get("entry_price", 0))
                qty = int(trade.get("quantity", 1))
                lot_size = int(trade.get("lot_size", 1))
                pnl = (current_ltp - entry_price) * lot_size * qty 
            except Exception:
                pnl = 0
            actual_quantity = qty
            live_response.append({
                "id": trade.get("id"),
                "timestamp": trade.get("timestamp"),
                "symbol": trade.get("symbol"),
                "trading_symbol": trade.get("trading_symbol", ""),
                "option_type": trade.get("option_type"),
                "strike": trade.get("strike"),        
                "quantity": qty,        
                "lot_size": lot_size,
                "entry_time": trade.get("entry_time"),
                "exit_time": trade.get("exit_time"),
                "capital_used": trade.get("capital_used"),
                "entry_price": trade.get("entry_price"),
                "exit_price": trade.get("exit_price"),
                "ltp": current_ltp,
                "pnl": round(pnl, 2),
                "order_status": trade.get("order_status"),
                "exit": ""
            })

        return jsonify({
            "live_positions": live_response,
            "paper_positions": paper_response
        })

    except Exception as e:
        logging.error(f"Error fetching positions: {e}", exc_info=True)
        return jsonify({"error": "Unable to fetch positions"}), 500


@app.route("/positions/closed", methods=["GET"])
def get_closed_positions():
    today = datetime.now().date().isoformat()
    response = []
    try:
        resp = supabase.table("trade_log").select("*")\
            .in_("order_status", ["closed", "failed", "rejected"])\
            .eq("timestamp", today).execute()

        if resp is None or not hasattr(resp, "data"):
            logging.error(f"Supabase query returned no data or invalid response")
            return jsonify({"error": "Unable to fetch closed trades"}), 500

        closed_positions = resp.data or []
        for trade in closed_positions:
            response.append({
                "id": trade.get("id"),
                "timestamp": trade.get("timestamp"),
                "symbol": trade.get("symbol"),
                "trading_symbol": trade.get("trading_symbol", ""),
                "option_type": trade.get("option_type"),
                "strike": trade.get("strike"),
                "quantity": trade.get("quantity"),
                "lot_size": trade.get("lot_size"),
                "trade_type": trade.get("trade_type"),
                "order_status": trade.get("order_status"),
                "entry_time": trade.get("entry_time"),
                "entry_price": trade.get("entry_price"),
                "exit_price": trade.get("exit_price"),
                "exit_time": trade.get("exit_time"),
                "reason": trade.get("reason"),
                "pnl": trade.get("pnl"),
                "capital_used": trade.get("capital_used"),
                "option_security_id": trade.get("option_security_id"),
                "order_id": trade.get("order_id")
            })
        return jsonify(response)

    except Exception as e:
        logging.error(f"Error fetching closed trades: {e}", exc_info=True)
        return jsonify({"error": "Unable to fetch closed trades"}), 500


@app.route('/webhook', methods=['POST'])
def webhook():
    try:
        refresh_settings_cache()
    except Exception as e:
        logging.error(f"Failed to refresh settings cache in webhook: {e}")
        return jsonify({"error": "Failed to refresh settings"}), 500
    
    data = request.get_json(silent=True) or request.form
    if not data:
        result = process_webhook_data(data) 
        return jsonify(result)
    symbol = data.get('symbol', '').strip().upper().replace('_', '-')
    action = data.get('action', '').strip().lower()
    option_type = data.get('option_type', '').strip().upper()
    strike = data.get('strike')
    
    try:
        strike = int(strike) if strike is not None else None
    except Exception:
        return jsonify({"error": "Invalid strike value"}), 400
    
    if action == "buy":
        option_type = data.get('option_type', '').strip().upper()
        if option_type == "CE":
            signal = "long"
        elif option_type == "PE":
            signal = "short"
        else:
            signal = ""
    elif action == "exit":
        signal = "exit"
    else:
        signal = ""
    
    if not symbol or not signal:
        return jsonify({"error": "Missing symbol or signal"}), 400
            
    try:
        entry_enabled = str(get_setting("entry_enabled", "false")).lower() == "true"
        paper_trade = str(get_setting("paper_trade", "false")).lower() == "true"
        max_trades = int(get_setting("max_trades_per_day", "0"))
        max_trades_per_symbol = int(get_setting("max_trades_per_symbol", "0"))
        max_capital = float(get_setting("max_capital", "0"))
        quantity = int(get_setting("quantity", "1"))  
        
        if 'qty' in data:
            try:
                quantity = int(data['qty'])
            except Exception:
                quantity = 1
        expiry_date_str = get_setting("expiry_date", "")
        strike_sel = int(get_setting("strike_selection", "0"))
    except (ValueError, TypeError) as e:
        return jsonify({"error": f"Error loading dynamic settings: {e}"}), 500
    
    today_date = datetime.now().date().isoformat()
        
    if not paper_trade and not is_market_open():
        return jsonify({
            "error": "Market is closed. Live orders only allowed during market hours."
        }), 400
    
    if signal == "exit":
        if paper_trade:
            open_pos = find_open_position(symbol, option_type, strike)
            if not open_pos:
                return jsonify({"status": "No open position to exit in paper trade"}), 200
            trading_symbol = open_pos.get("trading_symbol", "")
            response, status_code = exit_position_core_logic(symbol, option_type, strike)
            return jsonify(response), status_code
        else:
            broker_pos = check_broker_open_position(symbol, option_type, strike)
            if not broker_pos:
                return jsonify({
                    "status": "No open position found at broker; no exit order placed"
                }), 200
            strike = broker_pos.get("strike", strike)
            trading_symbol = broker_pos.get("tradingSymbol", "") or ""
            response, status_code = exit_position_core_logic(symbol, option_type, strike)
            return jsonify(response), status_code
                        
    if signal in ["long", "short"]:
        open_pos = find_open_position_with_broker_sync(symbol, option_type, strike)
    if open_pos:
        return jsonify({
            "status": "Duplicate order prevented",
            "message": f"An open position already exists for {symbol}. New order will not be placed."
        }), 200
    
    if is_duplicate_trade(symbol, option_type, strike_sel, today_date):
        return jsonify({"status": "Duplicate trade prevented"}), 200
    
    resp = supabase.table("trade_log")\
        .select("id")\
        .eq("timestamp", today_date)\
        .in_("order_status", ["open", "live", "paper", "closed"])\
        .execute()
    trades_today = len(resp.data or [])
    
    resp_symbol = supabase.table("trade_log")\
        .select("id")\
        .eq("timestamp", today_date)\
        .eq("symbol", symbol)\
        .in_("order_status", ["open", "live", "paper", "closed"])\
        .execute()
    trades_for_symbol_today = len(resp_symbol.data or [])
    
    if trades_today >= max_trades:
        return jsonify({"status": "Limit Reached", "message": f"Daily trade limit ({max_trades}) reached."}), 200
    
    if trades_for_symbol_today >= max_trades_per_symbol:
        return jsonify({"status": "Limit Reached", "message": f"Daily trade limit for {symbol} ({max_trades_per_symbol}) reached."}), 200
        
    if signal in ["long", "short"]:
        option_type = "CE" if signal == "long" else "PE"
        try:
            index_ltp = get_index_ltp(symbol)
            if index_ltp is None:
                return jsonify({"error": f"Could not get LTP for base symbol {symbol}"}), 400
        except Exception as e:
            logging.error(f"Error fetching LTP for {symbol}: {e}")
            return jsonify({"error": "Failed to fetch market data"}), 500
        
        expiry_date = None
        if expiry_date_str:
            try:
                expiry_date = datetime.strptime(expiry_date_str, "%Y-%m-%d").date()
            except ValueError:
                try:
                    expiry_date = datetime.strptime(expiry_date_str, "%m/%d/%Y").date()
                except Exception as e:
                    print(f"Expiry date parsing error: {e}")
        else:
            return jsonify({"error": "No valid expiry found"}), 400
        
        if not expiry_date:
            return jsonify({"error": "No valid expiry found"}), 400
        
        option_info = atm_option_security_cache.get((symbol, option_type))
        if option_info:
            opt_sec_id = option_info['SECURITY_ID']
            sel_strike = option_info['STRIKE']
            full_opt_symbol = option_info['SYMBOL_NAME']
        else:
            opt_sec_id, sel_strike, full_opt_symbol = find_option_security_id_fast(symbol, expiry_date, index_ltp, option_type, strike_sel)
        
        lot_size = get_lot_size(symbol, expiry_date, sel_strike, option_type)
        total_quantity = lot_size * quantity
       
        best_bid, best_ask = get_best_bid_ask(opt_sec_id)
        #logging.info(f"Best Bid: {best_bid}, Best Ask: {best_ask} for security {opt_sec_id}")

        if best_bid is None or best_ask is None or best_bid == 0 or best_ask == 0:
            return jsonify({"error": "Invalid market depth prices received. Cannot calculate capital."}), 400

        avg_price = (best_bid + best_ask) / 2
        rounded_price = round_to_0_05(avg_price)

        if rounded_price == 0:
            return jsonify({"error": "Rounded entry price is zero, possibly invalid data."}), 400

        estimated_trade_capital = rounded_price * total_quantity
        resp = supabase.table("trade_log")\
            .select("capital_used")\
            .in_("order_status", ["open", "live", "paper"])\
            .execute()
        capital_used_total = sum(float(trade.get("capital_used", 0)) for trade in (resp.data or []))
        
        if max_capital > 0 and (capital_used_total + estimated_trade_capital) > max_capital:
            return jsonify({
                "status": "Capital Limit Reached",
                "message": f"Placing this trade would exceed the maximum capital limit of {max_capital}."
            }), 200
        
        trade_type = "paper" if paper_trade else "live"
        
        if trade_type == "live":
            try:
                order_resp = place_order_with_confirmation(opt_sec_id, "BUY", total_quantity)
                if not order_resp or not order_resp.get("orderId"):
                    return jsonify({"error": "Order placement failed or missing orderId"}), 500
                                    
                capital_used = rounded_price * total_quantity   
                return jsonify({
                    "status": f"Order placed; awaiting confirmation for {option_type} option",
                    "symbol": symbol,
                    "trading_symbol": full_opt_symbol,
                    "order_status": "pending",
                    "entry_price": rounded_price,
                    "quantity": total_quantity,
                    "order_id": order_resp.get("orderId")
                }), 200
            except Exception as e:
                logging.error(f"Error in order placement or saving trade data: {e}")
                return jsonify({"error": "Internal server error"}), 500
        
        elif trade_type == "paper":
            actual_quantity = int(quantity) * int(lot_size)
            trade_data = {
                "timestamp": get_current_ist_time().strftime("%Y-%m-%d"),
                "symbol": symbol,
                "trading_symbol": full_opt_symbol,
                "option_type": option_type,
                "strike": int(sel_strike),
                "quantity": int(quantity),
                "lot_size": int(lot_size),
                "trade_type": trade_type,  
                "order_status": "open",    
                "entry_time": get_current_ist_time().strftime("%H:%M:%S"),
                "entry_price": rounded_price,
                "exit_price": 0.0,
                "exit_time": "00:00:00",
                "reason": "",
                "pnl": None,
                "capital_used": rounded_price * lot_size * quantity,
                "option_security_id": int(opt_sec_id),
                "order_id": None
            }
            trade_record_id = save_or_update_trade_data(trade_data)  
            
            if not trade_record_id:
                return jsonify({"error": "Failed to save paper trade data"}), 500
            socketio.emit('new_position', {'status': 'New paper trade initiated'})
            return jsonify({
                "status": f"{option_type} paper trade initiated",
                "symbol": symbol,
                "order_status": "open",
                "entry_price": rounded_price,
                "quantity": total_quantity,
                "trade_record_id": trade_record_id
            }), 200
        return jsonify({"error": "Invalid signal"}), 400
        
@app.route('/positions/exit', methods=['POST'], endpoint='exit_position_api')
def exit_position_api():
    data = request.json
    symbol = data.get("symbol", "").strip().upper()
    option_type = data.get("option_type", "").strip().upper()
    strike = data.get("strike", None)
    logging.info(f"Exit API received: symbol={symbol}, option_type={option_type}, strike={strike}")
    
    if not symbol:
        return jsonify({"error": "Missing symbol"}), 400    
    
    try:
        result = handle_exit(symbol, option_type, strike)
        return jsonify(result)
    except Exception as e:
        logging.error(f"Error in exit_position_api: {e}", exc_info=True)
        return jsonify({"error": "Internal error"}), 500
    
    if not symbol or not option_type or not strike:
        return jsonify({"error": "Missing symbol, option_type or strike"}), 400

    try:
        paper_trade = get_setting("paper_trade")
        if isinstance(paper_trade, str):
            paper_trade = paper_trade.lower() == 'true'

        if not paper_trade:
            open_pos = check_broker_open_position(symbol, option_type, strike)
            if not open_pos:
                return jsonify({"status": "Position closed or not found in broker; no exit order"}), 200
            
        response, status_code = exit_position_core_logic(symbol, option_type, strike)
        return jsonify(response), status_code

    except Exception as e:
        logging.error(f"Error in exit_position_api: {e}", exc_info=True)
        return jsonify({"error": "Internal error"}), 500

@app.route("/settings", methods=["GET", "POST"])
@authenticate(admin_only=True)
def manage_settings():
    try:
        if request.method == "GET":
            settings = get_all_settings()
            return jsonify(settings), 200

        elif request.method == "POST":
            data = request.get_json()
            if not data or not isinstance(data, dict):
                logging.error("No data or data is not a dict in POST /settings")
                return jsonify({"error": "No valid data received"}), 400

            for key, value in data.items():
                try:
                    update_setting(key, value)
                except Exception as e:
                    logging.error(f"Failed to update setting {key}: {e}")
                    return jsonify({"error": f"Failed to update {key}: {str(e)}"}), 500
            refresh_settings_cache()
            return jsonify({"status": "Settings updated"}), 200

        return jsonify({"error": "Invalid HTTP method"}), 405

    except Exception as err:
        logging.error(traceback.format_exc())
        return jsonify({"error": str(err), "trace": traceback.format_exc()}), 500

@app.route("/dashboard/summary", methods=["GET"])
def dashboard_summary():
    import ws_client
    today = datetime.now().date().isoformat()
    resp = supabase.table("trade_log").select("*").eq("timestamp", today).execute()
    live_trades = resp.data or []
    live_ltps = {}
    for trade in live_trades:
        sec_id = trade.get('option_security_id')
        if sec_id not in live_ltps:
            try:
                live_ltps[sec_id] = ws_client.get_last_ltp(sec_id)
            except Exception:
                live_ltps[sec_id] = None  

    total_trades = len(live_trades)
    live_positions = sum(1 for t in live_trades if t.get('order_status') == 'open')
    closed_positions = sum(1 for t in live_trades if t.get('order_status') == 'closed')
    closed_pnl = sum(float(t.get('pnl', 0) or 0) for t in live_trades if t.get('order_status') == 'closed')
    
    floating_pnl = 0.0
    
    for trade in live_trades:
        if trade.get('order_status') == 'open':
            current_ltp = live_ltps.get(trade.get('option_security_id'))
            if current_ltp is not None:
                try:
                    entry_price = float(trade.get('entry_price', 0) or 0)
                    quantity = int(trade.get('quantity', 0) or 0)
                    lot_size = int(trade.get('lot_size', 1) or 1)
                    trade_type = trade.get('trade_type', 'live')

                    if trade_type == "paper":
                        floating_pnl += (current_ltp - entry_price) * quantity * lot_size
                    else:
                        floating_pnl += (current_ltp - entry_price) * quantity
                except Exception as e:
                    logging.error(f"Error calculating floating PNL for trade ID {trade.get('id')}: {e}")
    
    entry_enabled = get_setting("entry_enabled") or 'false'
    
    logging.debug(f"Total Trades: {total_trades}, Live Positions: {live_positions}, Closed Positions: {closed_positions}")
    logging.debug(f"Closed PnL: {closed_pnl}, Floating PnL: {floating_pnl}")
    
    return jsonify({
        "total_trades": total_trades,
        "live_positions": live_positions,
        "closed_positions": closed_positions,
        "total_pnl": closed_pnl + floating_pnl,
        "entry_enabled": entry_enabled
    })

@app.route("/toggle_entry", methods=["POST"])
def toggle_entry():
    enable_entry = request.json.get("enable", True)
    update_setting("entry_enabled", str(enable_entry).lower())
    return jsonify({"status": "success", "entry_enabled": str(enable_entry).lower()})

@app.route('/favicon.ico')
def favicon():
    return send_from_directory(os.path.join(app.root_path, 'static'),
                               'favicon.ico', mimetype='image/vnd.microsoft.icon')

@app.route("/debug/refresh_cache", methods=["GET"])
def debug_refresh_cache():
    refresh_settings_cache()
    logging.info(f"Cache after manual refresh: {global_settings_cache}")
    return jsonify({"cache": global_settings_cache}), 200
from flask_socketio import SocketIO

if __name__ == "__main__":
    with app.app_context():
        refresh_active_option_ids(app)
        load_master_data(force_reload=True)
        start_atm_cache_updater(120)
        periodic_cache_refresh()
        
        logging.info(f"Startup PID: {os.getpid()}, Cache ID: {id(global_settings_cache)}, Cache keys: {list(global_settings_cache.keys())}")
        
    t_expiry = threading.Thread(target=expiry_check_loop, daemon=True)
    t_expiry.start()
    
    socketio.init_app(app)
    import ws_client
    ws_client.set_socketio(socketio)
    security_ids = [13] 
    t = threading.Thread(target=ws_client.start_ws_loop, args=(app,), daemon=True)
    t.start()
    
    polling_thread = threading.Thread(target=polling_loop, daemon=True)
    polling_thread.start()
    port = int(os.environ.get("PORT", 8080))
    socketio.run(app, host="0.0.0.0", port=port, debug=True, use_reloader=False)
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
    find_option_security_id_fast
)
from helper import (
    place_order,
    cancel_order,
    check_order_status,
    is_duplicate_trade,
    get_setting,
    update_setting,
    save_trade_data,
    round_to_0_05,
    find_open_position,
    get_equity_ltp,
    get_best_bid_ask,
    get_client_id_cached,
    get_access_token_cached,
    save_or_update_trade_data,
    get_all_settings
)
from ws_client import refresh_active_option_ids
from time_utils import get_current_ist_time, format_ist_time, convert_utc_to_ist
from shared_objects import socketio, market_data_cache
from flask import send_from_directory

logging.basicConfig(level=logging.INFO)
logging.getLogger('werkzeug').setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger('httpx').setLevel(logging.WARNING)   
logging.getLogger('websockets').setLevel(logging.WARNING)
# --- Time Utilities ---


# --- Supabase Setup ---
SUPABASE_URL = "https://iiweksphelhuapiwrtle.supabase.co"
SUPABASE_KEY = "sb_secret_42JXsFElc315ThoG966u0g_paImAF-Y"

if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("SUPABASE_URL and SUPABASE_KEY must be set as environment variables.")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)


#--- Flask Setup ---
app = Flask(__name__)


# Global trade batch list
batched_trades = []
global_settings_cache = {}
atm_option_security_cache = {}

def load_global_settings_optimized():
    settings_keys = [
        "expiry_selection",
        "strike_selection",
        "quantity",
        "max_trades_per_symbol",
        "expiry_date",
        "paper_trade",
        "max_trades_per_day",
        "max_capital",
        "USERNAME",
        "PASSWORD",
        "entry_enabled"
    ]

    response = supabase.table("settings").select("*").in_("key", settings_keys).execute()
    rows = response.data or []
    settings = {row['key']: row['value'] for row in rows}

    try:
        settings["strike_selection"] = int(settings.get("strike_selection", 0))
        settings["quantity"] = int(settings.get("quantity", 0))
        settings["max_trades_per_symbol"] = int(settings.get("max_trades_per_symbol", 0))
        settings["max_trades_per_day"] = int(settings.get("max_trades_per_day", 0))
        settings["max_capital"] = float(settings.get("max_capital", 0))
        settings["paper_trade"] = str(settings.get("paper_trade", "false")).lower() == "true"
        settings["entry_enabled"] = str(settings.get("entry_enabled", "false")).lower() == "true"
    except Exception as e:
        logging.error(f"Error parsing global settings: {e}")
    return settings
global_settings_cache = load_global_settings_optimized()

def refresh_settings_cache():
    global global_settings_cache
    global_settings_cache = load_global_settings_optimized()
    Timer(600, refresh_settings_cache).start()


refresh_settings_cache()

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
        logging.info(f"Cached ATM options for {symbol}: CE {ce_sec_id}, PE {pe_sec_id}")
    except Exception as e:
        logging.error(f"Error updating ATM option cache for {symbol}: {e}")


def update_all_atm_option_cache():
    from helper import get_equity_ltp
    expiry = get_expiry_date()
    if not expiry:
        logging.warning("Expiry date not found, skipping ATM cache update.")
        return
    for symbol in trading_symbols:
        ltp = get_equity_ltp(symbol)
        if ltp:
            update_atm_option_cache_for_symbol(symbol, ltp, expiry)
    logging.info("ATM option security cache updated for all symbols.")


def start_atm_cache_updater(interval_sec=60):
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
        time.sleep(120)  # check every 2 minutes


def add_trade_to_batch(trade_data, force_save=False):
    batched_trades.append(trade_data)
    if force_save or len(batched_trades) >= 1:
        batch = batched_trades.copy()
        batched_trades.clear()
        try:
            supabase.table("trade_log").insert(batch).execute()
            logging.info(f"Batch saved {len(batch)} trades.")
        except Exception as e:
            logging.error(f"Batch save failed: {e}")

def get_dhan_access_token():
    access_token = get_access_token_cached()
    if access_token:
        return access_token
    else:
        resp = supabase.table("settings").select("value").eq("key", "dhan_access_token").execute()
        data = resp.data
        if data and len(data) > 0:
            return data[0]["value"]
        else:
            return None

def get_dhan_client_id():
    client_id = get_client_id_cached()
    if client_id:
        return client_id
    else:
        resp = supabase.table("settings").select("value").eq("key", "dhan_client_id").execute()
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
                    supabase.table("trade_log")\
                        .update({
                            "order_status": "open",
                            "capital_used": trade_data["entry_price"] * trade_data["lot_size"] * trade_data["quantity"],
                            "order_id": order_id
                        })\
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
                if not auth:
                    return jsonify({"error": "Unauthorized"}), 401

                db_username = get_setting("USERNAME")
                db_password = get_setting("PASSWORD")
                
                if auth.username != db_username or auth.password != db_password:
                    return jsonify({"error": "Unauthorized"}), 401

                return func(*args, **kwargs)
            except (KeyError, AttributeError):
                return jsonify({"error": "Authentication settings not found."}), 500
        wrapper.__name__ = func.__name__ + "_auth_wrapper"
        return wrapper
    return decorator



def poll_and_update_transit_orders():
    logging.info("Polling started")
    today = datetime.now().date().isoformat()
    resp = supabase.table("trade_log").select("*").in_("order_status", ["transit", "pending"]).execute()
    trades = resp.data or []
    logging.info(f"Found {len(trades)} transit trades")
    updated = False
    for trade in trades:
        order_id = trade.get("order_id")
        logging.info(f"Checking order_id: {order_id}")
        if not order_id:
            continue
        
        url = f"https://api.dhan.co/v2/orders/{order_id}"
        headers = {
            "Content-Type": "application/json",
            "access-token": get_access_token_cached(),
            "client-id": get_client_id_cached(),
        }
        try:
            response = requests.get(url, headers=headers, timeout=5)
            if response.status_code != 200:
                logging.error(f"Failed to fetch order details for {order_id}: {response.status_code}")
                continue
            data = response.json()
            status = data.get("orderStatus", "").upper()
            executed_price = data.get("averageTradedPrice")
        except Exception as e:
            logging.error(f"Exception fetching order status for {order_id}: {e}")
            continue

        logging.info(f"Status for order {order_id}: {status}, Executed price: {executed_price}")

        if status in ["REJECTED", "FAILED", "CANCELLED"]:
            supabase.table("trade_log").update({"order_status": "failed"}).eq("id", trade["id"]).execute()
            updated = True
        elif status in ["CONFIRM", "TRADED", "PART_TRADED"]:
            supabase.table("trade_log").update({
                "order_status": "open",
                "entry_price": executed_price or trade.get("entry_price"),
            }).eq("id", trade["id"]).execute()
            socketio.emit('order_update', {
                'trade_id': trade["id"],
                'order_status': "open",
                'entry_price': executed_price
            })
            updated = True
    logging.info("Polling completed")
    return updated



def exit_position_core_logic(symbol):
    from ws_client import get_last_ltp
    exit_order_resp = None
    open_pos = find_open_position(symbol)
    if not open_pos:
        return {"status": "No open position to exit"}, 200
    
    opt_sec_id = open_pos.get("option_security_id")
    if not opt_sec_id:
        return {"error": "Option security id missing"}, 400
    
    last_ltp = get_last_ltp(str(opt_sec_id))
    if last_ltp is None or last_ltp == 0:
        best_bid, best_ask = get_best_bid_ask(int(opt_sec_id))
        if best_bid is None or best_ask is None or best_bid == 0 or best_ask == 0:
            return {"error": "Could not get LTP from WebSocket or API for exit"}, 400
        exit_price = round_to_0_05((best_bid + best_ask) / 2)
    else:
        exit_price = round_to_0_05(last_ltp)
    
    entry_price = float(open_pos.get('entry_price', 0))
    if entry_price == 0:
        return {"error": "Entry price not found for this trade. Cannot calculate PnL."}, 400
    
    lot_size = int(open_pos.get('lot_size', 1))
    quantity_multiplier = int(open_pos.get('quantity', 1))
    total_quantity = lot_size * quantity_multiplier
    pnl_points = exit_price - entry_price
    actual_pnl = pnl_points * total_quantity
    
    paper_trade = get_setting("paper_trade")
    if isinstance(paper_trade, str):
        paper_trade = paper_trade.lower() == 'true'
        
    if not paper_trade:
        try:
            exit_order_resp = place_order(opt_sec_id, "SELL", total_quantity)
            if not exit_order_resp or exit_order_resp.get("orderStatus", "").upper() in ["REJECTED", "FAILED"]:
                return {"error": "Exit order rejected or failed"}, 400
            supabase.table("trade_log").update({
                "order_status": "closed",
                "capital_used": 0,
                "exit_price": exit_price,
                "exit_time": get_current_ist_time().strftime("%H:%M:%S"),
                "reason": "exited",
                "pnl": actual_pnl
            }).eq("id", int(open_pos.get('id'))).execute()
        except Exception as e:
            return {"error": f"DB update failed: {e}"}, 500
    else:
        order_id_val = exit_order_resp.get("orderId") if exit_order_resp else None
        supabase.table("trade_log").update({
            "order_status": "closed",
            "capital_used": 0,
            "exit_price": exit_price,
            "exit_time": get_current_ist_time().strftime("%H:%M:%S"),
            "reason": "exited",
            "pnl": actual_pnl,
            "order_id": exit_order_resp.get("orderId") if exit_order_resp else None
        }).eq("id", int(open_pos.get('id'))).execute()
    return {"status": "Exit successful"}, 200


# --- Routes ---

@app.route('/bot/status')
def bot_status():
    return jsonify({"running": True})


@app.route("/reload_settings", methods=["POST"])
def reload_settings():
    global global_settings
    try:
        global_settings = load_global_settings_optimized()
        return jsonify({"status": "Settings reloaded successfully", "settings": global_settings}), 200
    except Exception as e:
        return jsonify({"error": f"Failed to reload settings: {e}"}), 500


@app.route("/")
def serve_dashboard():
    return render_template("dashboard.html")

@app.route("/poll_orders", methods=["POST"])
def poll_orders():
    poll_and_update_transit_orders()  
    return jsonify({"status": "polled and updated trade statuses"}), 200


# --- update_ltp ---
@app.route('/update_ltp', methods=['GET'])
def update_ltp():
    today = datetime.now().date().isoformat()
    resp = supabase.table("trade_log").select(
        "*"
    ).in_("order_status", ["open", "paper"]).eq("timestamp", today).execute()
    live_trades = resp.data or []
    latest_ltps = {}
    import ws_client
    updated_count = 0
    for trade in live_trades:
        sec_id = trade.get("option_security_id")
        if sec_id in latest_ltps:
            current_ltp = latest_ltps[sec_id]
        else:
            current_ltp = ws_client.get_last_ltp(sec_id)
            latest_ltps[sec_id] = current_ltp
        if current_ltp is not None:
            try:
                pnl = (current_ltp - trade["entry_price"]) * trade["quantity"] * trade["lot_size"]
                supabase.table("trade_log").update({
                    "ltp": round(current_ltp, 2),
                    "pnl": round(pnl, 2)
                }).eq("id", trade["id"]).execute()
                updated_count += 1
            except Exception:
                continue
    return jsonify({"status": f"{updated_count} positions updated successfully."}), 200

@app.route("/positions/live", methods=["GET"])
def get_live_positions():
    today = datetime.now().date().isoformat()
    resp = supabase.table("trade_log").select("*")\
        .in_("order_status", ["open", "pending"]).eq("timestamp", today).execute()
    live_trades = resp.data or []
    response = []
    if market_data_cache:
        print(f"type of cache keys collection: {type(market_data_cache.keys())}")
        first_key = next(iter(market_data_cache.keys()), None)
        print(f"type of individual cache key: {type(first_key)}")
    for trade in live_trades:
        sec_id = str(trade.get("option_security_id"))
        current_ltp = market_data_cache.get(str(sec_id), 0)
        print(f"SEC_ID: {sec_id} | Cached LTP: {current_ltp}")
        try:
            pnl = (current_ltp - trade["entry_price"]) * trade["quantity"] * trade["lot_size"]
        except Exception:
            pnl = 0
        response.append({
            "id": trade.get("id"),
            "timestamp": trade.get("timestamp"),
            "symbol": trade.get("symbol"),
            "option_type": trade.get("option_type"),
            "strike": trade.get("strike"),
            "quantity": trade.get("quantity"),
            "lot_size": trade.get("lot_size"),
            "trade_type": trade.get("trade_type"),
            "order_status": trade.get("order_status"),
            "entry_time": trade.get("entry_time"),
            "entry_price": trade.get("entry_price"),
            "ltp": current_ltp,
            "exit_price": trade.get("exit_price"),
            "exit_time": trade.get("exit_time"),
            "reason": trade.get("reason"),
            "pnl": round(pnl, 2),
            "capital_used": trade.get("capital_used"),
            "option_security_id": trade.get("option_security_id"),
            "order_id": trade.get("order_id")
        })
    return jsonify(response)

@app.route("/positions/closed", methods=["GET"])
def get_closed_positions():
    today = datetime.now().date().isoformat()
    resp = supabase.table("trade_log").select("*").eq("order_status", "closed").eq("timestamp", today).execute()
    closed_trades = resp.data or []
    response = []
    for trade in closed_trades:
        response.append({
            "id": trade.get("id"),
            "timestamp": trade.get("timestamp"),
            "symbol": trade.get("symbol"),
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

@app.route('/webhook', methods=['POST'])
def webhook():
    start_time = time.time()
    alert_received_time = time.time()
    data = request.get_json(silent=True) or request.form
    
    if not data:
        return jsonify({"error": "Missing data"}), 400
    symbol = data.get('symbol', '').strip().upper().replace('_', '-')
    action = data.get('action', '').strip().lower()
    signal_start = time.time()
    
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
    
    def safe_get_setting(key, default=None):
        val = get_setting(key)
        if val is None:
            return default
        return val
    
    try:
        paper_trade = global_settings_cache.get("paper_trade", False)
        max_trades = int(global_settings_cache.get("max_trades_per_day", "0"))
        max_trades_per_symbol = int(global_settings_cache.get("max_trades_per_symbol", "0"))
        max_capital = float(global_settings_cache.get("max_capital", "0"))
        quantity = int(global_settings_cache.get("quantity", "1"))
        expiry_date_str = global_settings_cache.get("expiry_date", "")
        strike_sel = int(global_settings_cache.get("strike_selection", "0"))
    except (ValueError, TypeError) as e:
        return jsonify({"error": f"Error loading dynamic settings: {e}"}), 500
    
    from datetime import datetime
    today_date = datetime.now().date().isoformat()
    
    
    if not paper_trade and not is_market_open():
        return jsonify({
            "error": "Market is closed. Live orders only allowed during market hours."
        }), 400
    
    if signal == "exit":
        open_pos = find_open_position(symbol)
        if not open_pos:
            return jsonify({"status": "Position already closed, no sell order placed"}), 200
        response, status_code = exit_position_core_logic(symbol)
        return jsonify(response), status_code
                        
    if signal in ["long", "short"]:
        open_pos = find_open_position(symbol)
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
        equity_ltp = get_equity_ltp(symbol)
        if equity_ltp is None:
            return jsonify({"error": f"Could not get LTP for base symbol {symbol}"}), 400
        
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
            opt_sec_id, sel_strike, full_opt_symbol = find_option_security_id_fast(symbol, expiry_date, equity_ltp, option_type, strike_sel)
        
        lot_size = get_lot_size(symbol, expiry_date, sel_strike, option_type)
        total_quantity = lot_size * quantity
       
        best_bid, best_ask = get_best_bid_ask(opt_sec_id)
        logging.info(f"Best Bid: {best_bid}, Best Ask: {best_ask} for security {opt_sec_id}")

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
            order_resp = place_order(opt_sec_id, "BUY", total_quantity,)
            logging.info(f"Order response: {order_resp}")
            if not order_resp:
                return jsonify({"status": "error", "message": "Order placement failed"}), 200
            
            trade_data = {
                "timestamp": get_current_ist_time().strftime("%Y-%m-%d"),
                "symbol": symbol,
                "option_type": option_type,
                "strike": int(sel_strike),
                "quantity": int(quantity),
                "lot_size": int(lot_size),
                "trade_type": trade_type,  
                "order_status": "open",    
                "entry_time": get_current_ist_time().strftime("%H:%M:%S"),
                "entry_price": rounded_price,
                "exit_price": None,
                "exit_time": None,
                "reason": None,
                "pnl": None,
                "capital_used": rounded_price * lot_size * quantity,
                "option_security_id": int(opt_sec_id),
                "order_id": order_resp.get("orderId")
            }
            trade_record_id = save_or_update_trade_data(trade_data)
            if not trade_record_id:
                return jsonify({"error": "Failed to save live trade data"}), 500
            socketio.emit('new_position', {'status': 'New live trade initiated'})
        
            return jsonify({
                "status": f"Order placement confirmed and trade logged for {option_type} option",
                "symbol": symbol,
                "order_status": "open",
                "entry_price": rounded_price,
                "quantity": total_quantity,
                "trade_record_id": trade_record_id
            }), 200
        
        elif trade_type == "paper":
            
            trade_data = {
                "timestamp": get_current_ist_time().strftime("%Y-%m-%d"),
                "symbol": symbol,
                "option_type": option_type,
                "strike": int(sel_strike),
                "quantity": int(quantity),
                "lot_size": int(lot_size),
                "trade_type": trade_type,  
                "order_status": "open",    
                "entry_time": get_current_ist_time().strftime("%H:%M:%S"),
                "entry_price": rounded_price,
                "exit_price": None,
                "exit_time": None,
                "reason": None,
                "pnl": None,
                "capital_used": rounded_price * lot_size * quantity,
                "option_security_id": int(opt_sec_id),
                "order_id": None
            }
            trade_record_id = save_or_update_trade_data(trade_data)  
            elapsed_time = time.time() - start_time
            logging.info(f"Paper trade setup time: {elapsed_time:.3f} seconds")

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

    else:
        end_time = time.time()
        logging.info(f"Webhook total execution time: {end_time - start_time:.3f} seconds")
        return jsonify({"error": "Invalid signal"}), 400
        

        
@app.route('/positions/exit', methods=['POST'], endpoint='exit_position_api')
def exit_position_api():
    data = request.json
    symbol = data.get("symbol", "").strip().upper()
    if not symbol:
        return jsonify({"error": "Missing symbol"}), 400
    try:
        open_pos = find_open_position(symbol)  
        print("Open Position Data:", open_pos)
        response, status_code = exit_position_core_logic(symbol)
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
                    if entry_price and quantity and lot_size:
                        floating_pnl += (current_ltp - entry_price) * quantity * lot_size
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


def polling_loop():
    while True:
        poll_and_update_transit_orders()
        time.sleep(60)  

from flask_socketio import SocketIO
if __name__ == "__main__":
    with app.app_context():
        refresh_active_option_ids(app)
        load_master_data(force_reload=True)
        global_settings = load_global_settings_optimized()
        start_atm_cache_updater(60)
    
    t_expiry = threading.Thread(target=expiry_check_loop, daemon=True)
    t_expiry.start()
    
    socketio.init_app(app)
    import ws_client
    ws_client.set_socketio(socketio)
    t = threading.Thread(target=ws_client.start_ws_loop, args=(app,), daemon=True)
    t.start()
    
    polling_thread = threading.Thread(target=polling_loop, daemon=True)
    polling_thread.start()
    port = int(os.environ.get("PORT", 8080))
    socketio.run(app, host="0.0.0.0", port=port, debug=True)

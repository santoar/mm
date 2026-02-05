import os
import sys
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
)
from helper import (
    place_order,
    place_order_with_confirmation,
    get_best_bid_ask,
    find_open_position,
    find_open_position_with_broker_sync,
    get_index_ltp,
    save_or_update_trade_data,
    get_setting,
    refresh_settings_cache,
    check_broker_open_position,
    fetch_broker_positions_list,
    clear_position_cache,
    get_dhan_access_token,
    get_dhan_client_id,
    periodic_cache_refresh,
    global_settings_cache,
    update_setting,
    normalize_symbol,
    parse_executed_price,
    is_duplicate_trade,
    check_order_status,
    match_broker_position,           
    poll_and_update_transit_orders,
    round_to_0_05
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
processed_alerts = set()
batched_trades = []
atm_option_security_cache = {}

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

def get_specific_id_quantity(target_sec_id):
    try:
        from helper import fetch_broker_positions_list
        all_positions = fetch_broker_positions_list()
        
        for pos in all_positions:
            if str(pos.get('securityId')) == str(target_sec_id):
                return int(pos.get('netQty', 0))
        return 0
    except Exception as e:
        logging.error(f"Error checking broker qty: {e}")
        return 0

def exit_position_core_logic(symbol, option_type=None, strike=None, pre_checked_pos=None, exit_reason=""):
    from ws_client import get_last_ltp
    
    if pre_checked_pos:
        open_pos = pre_checked_pos
    else:
        open_pos = find_open_position(symbol, option_type=option_type, strike=strike)
    
    if open_pos is None or not isinstance(open_pos, dict):
        logging.warning(f"Exit Ignored: No trade in DB for {symbol}")
        return {"status": "No position in DB"}, 200
    
    row_id = open_pos.get("id")
    opt_sec_id = open_pos.get("option_security_id")
    trading_symbol = open_pos.get("trading_symbol")
    entry_price = float(open_pos.get('entry_price', 0) or 0)
    total_quantity = int(open_pos.get('lot_size', 1)) * int(open_pos.get('quantity', 1))

    broker_match = check_broker_open_position(
        trading_symbol=trading_symbol, 
        option_type=option_type, 
        strike=strike, 
        security_id=opt_sec_id,
        force_refresh=True
    )
    
    if broker_match is None or broker_match.get("state") == "closed":
        logging.warning(f"Manual Exit! Position {trading_symbol} NOT found on Broker.")
        
        supabase.table("trade_log").update({
            "order_status": "closed",
            "reason": "manual_exit_sync",
            "exit_time": get_current_ist_time().strftime("%H:%M:%S")
        }).eq("id", int(row_id)).execute()
        
        return {"status": "Paper Trade closed on broker. DB synced."}, 200

    is_rapid_exit = True if exit_reason == "rapid_exit_alert" else False
    
    
    last_ltp = get_last_ltp(str(opt_sec_id))
    exit_price = round_to_0_05(last_ltp) if last_ltp else entry_price
    
    paper_trade = str(get_setting("paper_trade")).lower() == 'true'
    order_id = ""

    if not paper_trade:
        exit_order_resp = place_order(opt_sec_id, "SELL", total_quantity)
        if exit_order_resp and exit_order_resp.get("orderId"):
            order_id = exit_order_resp.get("orderId")
            avg_p = exit_order_resp.get("averagePrice") or exit_order_resp.get("executed_price")
            if avg_p: exit_price = float(avg_p)
        else:
            logging.error("Broker Exit Order FAILED.")
            return {"error": "Broker Exit Failed"}, 500

    pnl = (exit_price - entry_price) * total_quantity
    supabase.table("trade_log").update({
        "order_status": "closed",
        "exit_price": float(exit_price),
        "exit_time": get_current_ist_time().strftime("%H:%M:%S"),
        "reason": exit_reason or "webhook_exit",
        "pnl": float(pnl),
        "order_id": order_id
    }).eq("id", int(row_id)).execute()

    return {"status": f"Successfully exited {trading_symbol}", "pnl": pnl}, 200

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
    try:
        resp = supabase.table("trade_log").select("*")\
            .in_("order_status", ["open", "pending"])\
            .in_("trade_type", ["live", "paper"])\
            .eq("timestamp", today)\
            .execute()
        all_trades = resp.data or []
    except Exception as e:
        print("Error fetching live trades:", e)
        all_trades = []

    live_positions = []
    paper_positions = []
    
    if market_data_cache:
        for trade in all_trades:
            try:
                sec_id = str(trade.get("option_security_id"))
                current_ltp = market_data_cache.get(sec_id, 0)
                entry_price = float(trade.get("entry_price") or 0)
                quantity = int(trade.get("quantity") or 0)
                lot_size = int(trade.get("lot_size") or 0)
                pnl = (float(current_ltp) - entry_price) * quantity * lot_size

                t = {
                    "id": trade.get("id"),
                    "timestamp": trade.get("timestamp"),
                    "symbol": trade.get("symbol"),
                    "trading_symbol": trade.get("trading_symbol", ""),
                    "option_type": trade.get("option_type"),
                    "strike": trade.get("strike"),
                    "quantity": quantity,
                    "lot_size": lot_size,
                    "trade_type": trade.get("trade_type"),
                    "order_status": trade.get("order_status"),
                    "entry_time": trade.get("entry_time"),
                    "entry_price": entry_price,
                    "ltp": current_ltp,
                    "exit_price": trade.get("exit_price"),
                    "exit_time": trade.get("exit_time"),
                    "reason": trade.get("reason"),
                    "pnl": round(pnl, 2),
                    "capital_used": trade.get("capital_used"),
                    "option_security_id": trade.get("option_security_id"),
                    "order_id": trade.get("order_id")
                }
                if trade.get("trade_type") == "live":
                    live_positions.append(t)
                elif trade.get("trade_type") == "paper":
                    paper_positions.append(t)

            except Exception as e:
                print(f"Error processing trade {trade.get('id')}: {e}")
                continue
    return jsonify({
        "live_positions": live_positions,
        "paper_positions": paper_positions
    })

@app.route("/positions/closed", methods=["GET"])
def get_closed_positions():
    today = datetime.now().date().isoformat()
    response = []
    
    resp = None
    for attempt in range(3):
        try:
            resp = supabase.table("trade_log").select("*")\
                .in_("order_status", ["closed", "failed", "rejected"])\
                .eq("timestamp", today).execute()
            break
        except Exception as e:
            logging.warning(f"DB Connection failed (Attempt {attempt+1}/3): {e}")
            time.sleep(1)
    
    try:
        if resp is None or not hasattr(resp, "data"):
            logging.error(f"Supabase query returned no data after retries")
            return jsonify({"error": "Unable to fetch closed trades"}), 500

        closed_positions = resp.data or []
        logging.info(f"Closed positions fetched from DB: {len(closed_positions)} rows")
    except Exception as e:
        logging.error(f"Error processing DB data: {e}")
        return jsonify([]), 200

    broker_positions = fetch_broker_positions_list() 
    for trade in closed_positions:
        entry_price = trade.get("entry_price") or 0.0
        exit_price  = trade.get("exit_price") or 0.0
        pnl         = float(trade.get("pnl") or 0.0)

        if (trade.get("trade_type") or "").lower() == "live" and broker_positions:
            local_trade = {
                "trading_symbol": trade.get("trading_symbol", ""),
                "option_type": trade.get("option_type"),
                "strike": trade.get("strike"),
                "option_security_id": trade.get("option_security_id"),
            }
            match = match_broker_position(local_trade, broker_positions)
            if match:
                pos = match["pos"]
                buy_avg  = float(pos.get("buyAvg")  or 0.0)
                sell_avg = float(pos.get("sellAvg") or 0.0)
                realized  = float(pos.get("realizedProfit") or 0.0)
                if buy_avg > 0:
                    entry_price = buy_avg
                if sell_avg > 0:
                    exit_price = sell_avg
                if realized != 0:
                    pnl = realized
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
            "entry_price": float(entry_price),   
            "exit_price": float(exit_price),     
            "exit_time": trade.get("exit_time"),
            "reason": trade.get("reason"),
            "pnl": float(pnl),             
            "capital_used": trade.get("capital_used"),
            "option_security_id": trade.get("option_security_id"),
            "order_id": trade.get("order_id")
        })        
        
    return jsonify(response)

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
    
    alert_key = f"{symbol}_{action}_{option_type}_{strike}"
    if alert_key in processed_alerts:
        logging.info(f"BLOCKING DUPLICATE ALERT: {alert_key}")
        return jsonify({"status": "Already processing"}), 200
    
    processed_alerts.add(alert_key)
    threading.Timer(20, lambda: processed_alerts.discard(alert_key)).start()
    
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
        
        # 3. Calculate Option Security ID
        option_info = atm_option_security_cache.get((symbol, option_type))
        if option_info:
            opt_sec_id = option_info['SECURITY_ID']
            sel_strike = option_info['STRIKE']
            full_opt_symbol = option_info['SYMBOL_NAME']
        else:
            opt_sec_id, sel_strike, full_opt_symbol = find_option_security_id_fast(symbol, expiry_date, index_ltp, option_type, strike_sel)
        
        from shared_objects import market_data_cache
        from ws_client import subscribe_symbols
        
        subscribe_symbols([str(opt_sec_id)])

        ltp = None
        
        for _ in range(10): 
            val = market_data_cache.get(str(opt_sec_id))
            if val and float(val) > 0:
                ltp = float(val)
                break
            time.sleep(0.05)

        if not ltp:
            logging.warning(f"WebSocket silent for {opt_sec_id}. Switching to API Fallback...")
            try:
                url = "https://api.dhan.co/v2/marketfeed/quote"
                
                headers = {
                    "access-token": get_setting("dhan_access_token"),
                    "client-id": get_setting("dhan_client_id"),
                    "Content-Type": "application/json"
                }
                
                payload = {
                    "NSE_FNO": [str(opt_sec_id)]
                }
                
                resp = requests.post(url, headers=headers, json=payload, timeout=2)
                
                if resp.status_code == 200:
                    data = resp.json()
                    if data.get("data") and "NSE_FNO" in data["data"]:
                        stock_data = data["data"]["NSE_FNO"].get(str(opt_sec_id))
                        if stock_data and "last_price" in stock_data:
                            ltp = float(stock_data["last_price"])
                            logging.info(f"API Fallback Success! Fetched LTP: {ltp}")
                else:
                    logging.error(f"API Fallback Failed: Status {resp.status_code} | {resp.text}")

            except Exception as e:
                logging.error(f"Critical API Fallback Error: {e}")

        
        if ltp and ltp > 0:
            rounded_price = round_to_0_05(ltp)
            logging.info(f"Final Execution Price: {rounded_price}")
        else:
            return jsonify({"error": "Market Data Unavailable (WebSocket & API both failed)."}), 400
        entry_price = rounded_price 
                
        lot_size = get_lot_size(symbol, expiry_date, sel_strike, option_type)
        total_quantity = lot_size * quantity
       
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
        try:
            if trade_type == "live":
                order_resp = place_order_with_confirmation(opt_sec_id, "BUY", total_quantity)
                
                if not order_resp or not order_resp.get("orderId"):
                    return jsonify({"error": "Order placement failed"}), 500
                
                try:
                    if 'socketio' in globals():
                        socketio.emit('new_position', {'status': 'Live trade started'})
                except Exception: pass
                
                return jsonify({"status": "Success", "order_id": order_resp.get("orderId")}), 200
        
            else: # Paper Trade
                trade_data = {
                    "timestamp": today_date,
                    "symbol": symbol,
                    "trading_symbol": full_opt_symbol,
                    "option_type": option_type,
                    "strike": int(sel_strike),
                    "quantity": int(quantity),
                    "lot_size": int(lot_size),
                    "trade_type": "paper",
                    "order_status": "open",
                    "entry_time": get_current_ist_time().strftime("%H:%M:%S"),
                    "entry_price": entry_price,
                    "capital_used": entry_price * total_quantity,
                    "option_security_id": int(opt_sec_id)
                }
                save_or_update_trade_data(trade_data)
                try:
                    if 'socketio' in globals():
                        socketio.emit('new_position', {'status': 'Paper trade started'})
                except Exception: pass
                return jsonify({"status": "Paper Trade Started"}), 200

        except Exception as e:
            logging.error(f"Order error: {e}")
            return jsonify({"error": "Internal execution error"}), 500

    return jsonify({"error": "Invalid flow"}), 400
        
@app.route('/positions/exit', methods=['POST'], endpoint='exit_position_api')
def exit_position_api():
    data = request.json
    raw_symbol = data.get("symbol", "")
    symbol = normalize_symbol(raw_symbol)
    if not symbol:
        return jsonify({"error": "Missing symbol"}), 400
    try:
        open_pos = find_open_position_with_broker_sync(symbol)  
        if not open_pos:
            return jsonify({"status": "Position already closed"}), 200

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

def polling_loop():
    while True:
        poll_and_update_transit_orders()
        time.sleep(60)  

from flask_socketio import SocketIO

socketio.init_app(app)
import ws_client  
ws_client.set_socketio(socketio)

print("--- INITIALIZING BOT FOR CLOUD DEPLOYMENT ---")
with app.app_context():
    try:
        load_master_data(force_reload=True)
        
        threading.Thread(target=ws_client.start_ws_loop, args=(app,), daemon=True).start()
        threading.Thread(target=polling_loop, daemon=True).start()
        threading.Thread(target=expiry_check_loop, daemon=True).start()
        threading.Thread(target=start_atm_cache_updater, args=(120,), daemon=True).start()
        logging.info("All Cloud Background Threads Started.")
    except Exception as e:
        logging.error(f"Error during initialization: {e}")


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    
    socketio.run(app, host="0.0.0.0", port=port, debug=True, use_reloader=False)
import re
import os
import sys
import uuid
import json
import time
import queue
import logging
import requests
import threading
import collections
from flask import Flask
from shared_objects import market_data_cache
from datetime import datetime, date
from supabase import create_client
from master_data import (
    index_symbol_to_id as index_symbol_to_id_map,
    get_symbol_from_security_id,
    get_lot_size_for_security,
    get_strike_for_security,
    get_option_type_for_security,
    get_expiry_date
)
from time_utils import get_current_ist_time, format_ist_time, convert_utc_to_ist


SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s: %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout) # Sirf console par bhejo, Cloud Run apne aap handle kar lega
    ],
    force=True
)

logging.getLogger('werkzeug').setLevel(logging.WARNING)
logging.getLogger('httpx').setLevel(logging.WARNING)
logging.getLogger('httpcore').setLevel(logging.WARNING)
logging.getLogger('urllib3').setLevel(logging.WARNING)
logging.getLogger('websockets').setLevel(logging.WARNING)
logging.getLogger('supabase').setLevel(logging.WARNING)

logging.info("Helper loaded - Clean logging + Supabase quiet")


global_settings_cache = {}

def load_settings_from_supabase():
    try:
        response = supabase.table("settings").select("*").execute()
        rows = response.data or []
        settings = {row['key']: row['value'] for row in rows}
        #logging.info(f"Loaded settings from Supabase: {settings}")
        return settings
    except Exception as e:
        logging.error(f"Failed to load settings from Supabase: {e}")
        return {}

def refresh_settings_cache():
    global global_settings_cache
    settings = load_settings_from_supabase()
    if settings:
        if not isinstance(global_settings_cache, dict):
            logging.warning(f"global_settings_cache not dict, reinitializing. Was: {type(global_settings_cache)}")
            global_settings_cache = {}
        if not isinstance(settings, dict):
            logging.error(f"settings from supabase is not dict: {type(settings)}")
            return
        global_settings_cache.clear()
        global_settings_cache.update(settings)
        logging.info(f"Settings cache refreshed with keys: {list(global_settings_cache.keys())}")
    else:
        logging.warning("Settings not refreshed due to empty data")

def get_setting(key, default=None):
    return global_settings_cache.get(key, default)

def periodic_cache_refresh(interval_sec=300):
    def refresh_loop():
        while True:
            try:
                refresh_settings_cache()
            except Exception as e:
                logging.error(f"Periodic cache refresh failed: {e}")
            time.sleep(interval_sec)
    thread = threading.Thread(target=refresh_loop, daemon=True)
    thread.start()


def get_dhan_access_token(): return get_setting("dhan_access_token")

def get_dhan_client_id(): return get_setting("dhan_client_id")


global_rate_limit_lock = threading.Lock()
global_min_interval = 0.05  

def load_global_settings_optimized():
    settings_keys = [
        "expiry_date",
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
    response = supabase.table("settings").select("*").execute()
    
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


def update_setting(key, value):
    existing = supabase.table("settings").select("key").eq("key", key).single().execute().data
    try:
        if existing:
            supabase.table("settings").update({"value": value}).eq("key", key).execute()
        else:
            supabase.table("settings").insert({"key": key, "value": value}).execute()
        refresh_settings_cache()
    except Exception as e:
        logging.error(f"Error updating setting {key}: {e}")

def round_to_0_05(x):
    return round(x * 20) / 20

def is_duplicate_trade(symbol, option_type, strike, date):
    try:
        resp = supabase.table("trade_log")\
            .select("id")\
            .eq("symbol", symbol)\
            .eq("option_type", option_type)\
            .eq("strike", strike)\
            .eq("timestamp", date)\
            .eq("order_status", "open")\
            .maybe_single().execute()
        return bool(resp and resp.data)
    except Exception:
        return False

def get_best_bid_ask(security_id):
    from shared_objects import market_data_cache
    
    ltp = market_data_cache.get(str(security_id))
    
    if ltp and float(ltp) > 0:
        val = float(ltp)
        return val, val  
    
    return None, None
       
def place_order(security_id, txn_type, qty=1):
    client_id = get_setting("dhan_client_id")
    access_token = get_setting("dhan_access_token")
    
    ltp, _ = get_best_bid_ask(security_id)
    
    if not ltp:
        logging.error(f"Order Cancelled: LTP missing for {security_id}")
        return None
    
    price = round_to_0_05(ltp)
    
    symbol_raw = get_symbol_from_security_id(security_id)
    expiry_date = get_expiry_date()
    expiry = expiry_date.strftime("%b").upper() if expiry_date else ""
    year = expiry_date.strftime("%Y") if expiry_date else ""
    strike = get_strike_for_security(security_id)
    option_type = get_option_type_for_security(security_id)
    trading_symbol = make_broker_style_symbol(symbol_raw, expiry, year, strike, option_type)

    
    url = "https://api.dhan.co/v2/orders"
    headers = {
        "Content-Type": "application/json",
        "access-token": get_setting("dhan_access_token"),
        "client-id": get_setting("dhan_client_id"),
    }
    order_type = "MARKET" if txn_type.upper() == "SELL" else "LIMIT"
    
    body = {
        "dhanClientId": client_id,
        "transactionType": txn_type.upper(),
        "exchangeSegment": "NSE_FNO",
        "productType": "INTRADAY",
        "orderType": order_type,
        "validity": "DAY",
        "securityId": str(security_id),
        "quantity": int(qty),
        "price": price if order_type == "LIMIT" else 0,
        "tradingSymbol": trading_symbol
    }
    try:
        logging.info(f"Firing {txn_type} Order: {trading_symbol} @ {price}")
        response = requests.post(url, headers=headers, json=body)
        resp_json = response.json()        
        
        if resp_json.get("orderId"):
            resp_json["executed_price"] = price 
            resp_json["tradingSymbol"] = trading_symbol
            return resp_json
        else:
            logging.error(f"Order Placement Failed: {resp_json}")
            return None
    except Exception as e:
        logging.error(f"Fatal Order Error: {e}")
        return None

def place_order_with_confirmation(security_id, txn_type, qty=1):
    order_resp = place_order(security_id, txn_type, qty)
    if order_resp and order_resp.get("orderId"):
        order_id = order_resp.get("orderId")
        logging.info(f"Order placed with order_id {order_id}, starting confirmation thread")

        trading_symbol = order_resp.get("tradingSymbol", "")
        symbol_raw = get_symbol_from_security_id(security_id)

        base_symbol_only = re.split(r'[-\s]', symbol_raw)[0].upper()

        provisional_trade_data = build_provisional_trade_data(order_id, security_id, txn_type, qty, provisional_status="pending")
        
        if provisional_trade_data is not None:
            provisional_trade_data["trading_symbol"] = trading_symbol
            provisional_trade_data["symbol"] = base_symbol_only

        provisional_id = save_or_update_trade_data(provisional_trade_data)
        if provisional_id:
            supabase.table("trade_log").update({
                "order_status": "pending"
            }).eq("id", provisional_id).execute()

            logging.info(f"Marked provisional trade {provisional_id} as pending.")
            socketio.emit('order_update', {
                'order_id': order_id,
                'order_status': 'pending'
            }, namespace='/io')
        check_order_status_with_interval_and_save(order_id, security_id, txn_type, qty)
        return order_resp
    else:
        logging.error("Order placement failed or missing orderId")
        return None

def check_order_status_with_interval_and_save(order_id, security_id, txn_type, qty, interval=3, max_retries=50):
    def task():
        trading_symbol = None
        
        for retry_count in range(max_retries):
            status, executed_price, trading_symbol = check_order_status(order_id)
            logging.info(f"Order {order_id} status check #{retry_count + 1}: {status}, exec_price={executed_price}")

            if status == "CONFIRM":
                if executed_price is None or executed_price == 0:
                    logging.warning(f"Executed price not found yet for order {order_id}. Retrying...")
                    time.sleep(interval)
                    continue
                
                if not trading_symbol:
                    try:
                        resp = supabase.table("trade_log").select("trading_symbol").eq("order_id", order_id).maybe_single().execute()
                        data = getattr(resp, "data", None)
                        if data:
                            trading_symbol = data.get("trading_symbol")
                    except Exception as e:
                        logging.error(f"Error fetching trading_symbol for order_id {order_id}: {e}")
                else:
                    trading_symbol = trading_symbol
                
                trade_data = build_trade_data_for_order(order_id, security_id, txn_type, qty, executed_price,)
      
                if trade_data:
                    save_or_update_trade_data(trade_data)
                    try:
                        with app.app_context():
                            socketio.emit('new_position', {
                                'status': 'New live trade updated with executed price',
                                'order_id': trade_data.get("order_id"),
                                'symbol': trade_data.get("symbol")
                            }, namespace='/io')
                    except Exception as exc:
                        logging.error(f"SocketIO emit error: {exc}")
                    logging.info(f"Order {order_id} updated with executed price and dashboard notified.")
                return  
            elif status in ["FAILED", "REJECTED", "CANCELLED", "EXPIRED"]:
                existing_trade = None
                try:
                    resp = supabase.table("trade_log").select("*").eq("order_id", order_id).maybe_single().execute()
                    existing_trade = getattr(resp, "data", None)
                except Exception as e:
                    logging.error(f"Error finding existing trade for {order_id}: {e}")

                close_data = {
                    "order_id": order_id,
                    "order_status": "failed",
                    "exit_time": get_current_ist_time().strftime("%H:%M:%S"),
                    "reason": f"order_{status.lower()}",
                    "exit_price": 0.0,
                    "pnl": 0.0,
                    "symbol": existing_trade.get("symbol") if existing_trade else "",
                    "option_type": existing_trade.get("option_type") if existing_trade else "",
                    "strike": existing_trade.get("strike") if existing_trade else 0,
                    "quantity": existing_trade.get("quantity") if existing_trade else 0,
                    "lot_size": existing_trade.get("lot_size") if existing_trade else 0,
                    "option_security_id": existing_trade.get("option_security_id") if existing_trade else 0,
                    "trade_type": existing_trade.get("trade_type") if existing_trade else "live",
                    "entry_time": existing_trade.get("entry_time") if existing_trade else get_current_ist_time().strftime("%H:%M:%S"),
                    "entry_price": existing_trade.get("entry_price") if existing_trade else 0.0,
                    "timestamp": existing_trade.get("timestamp") if existing_trade else get_current_ist_time().strftime("%Y-%m-%d"),
                    "capital_used": existing_trade.get("capital_used") if existing_trade else 0.0,
                }
                save_or_update_trade_data(close_data)
                try:
                    with app.app_context():
                        socketio.emit('order_update', {
                            'order_id': order_id,
                            'order_status': 'failed'
                        }, namespace='/io')
                except Exception as exc:
                    logging.error(f"SocketIO emit error: {exc}")
                logging.info(f"Order {order_id} updated with executed price and dashboard notified.")
                return

            elif status in ["PENDING", "TRANSIT"]:
                try:
                    resp = supabase.table("trade_log").update({
                        "order_status": "pending",
                    }).eq("order_id", order_id).execute()
                    with app.app_context():
                        socketio.emit('order_update', {
                            'order_id': order_id,
                            'order_status': "pending"
                        }, namespace='/io')
                except Exception as e:
                    logging.error(f"DB update/socket emit exception for pending order {order_id}: {e}")

            if retry_count < max_retries - 1:
                time.sleep(interval)    
        logging.warning(f"Order {order_id} not confirmed after max retries.")
        
    threading.Thread(target=task, daemon=True).start()

def check_order_status(order_id):
    token = get_dhan_access_token()
    client_id = get_dhan_client_id()
    
    if not token or not client_id:
        return "UNKNOWN", 0.0, None

    url = f"https://api.dhan.co/v2/orders/{order_id}"
    headers = {
        "Content-Type": "application/json",
        "access-token": token,
        "client-id": client_id,
    }

    try:
        response = requests.get(url, headers=headers)
        
        if response.status_code == 200:
            data = response.json()
            if isinstance(data, list): data = data[0]
            
            if not data or 'orderStatus' not in data:
                logging.warning(f"Dhan returned success but no order data for {order_id}")
                return "FAILED", 0.0, None

            status = str(data.get("orderStatus", "")).upper()
            price = data.get("averageTradedPrice") or data.get("tradedPrice") or data.get("price") or 0.0
            tsym = data.get("tradingSymbol", "")

            logging.info(f"API Check -> Order: {order_id} | Status: {status} | Price: {price}")

            if status in ["TRADED", "FULLY_EXECUTED", "CONFIRM"]:
                return "CONFIRM", float(price), tsym
            elif status in ["REJECTED", "FAILED", "CANCELLED"]:
                return "FAILED", 0.0, tsym
            else:
                return "TRANSIT", 0.0, tsym
        else:
            logging.error(f"Dhan API Error {response.status_code} for order {order_id}")
            return "FAILED", 0.0, None

    except Exception as e:
        logging.error(f"check_order_status fatal error: {e}")
        return "UNKNOWN", 0.0, None


def parse_executed_price(order_data, security_id=None, paper_trade=False):
    price = (
        order_data.get("averageTradedPrice")
        or order_data.get("tradedPrice")
        or order_data.get("price")
    )
    try:
        price = float(price)
        if price > 0:
            return price
    except Exception:
        pass

    if not paper_trade and security_id:
        from shared_objects import market_data_cache
        ltp = market_data_cache.get(str(security_id))
        if ltp:
            return float(ltp)
            
    return 0.0

def make_broker_style_symbol(sym, expiry, year, strike, opt_type):
    return f"{sym}-{expiry}{year}-{strike}-{opt_type}".replace("--", "-")


def build_provisional_trade_data(order_id, security_id, txn_type, qty, provisional_status="pending"):
    try:
        symbol_raw = get_symbol_from_security_id(security_id)
        expiry_date = get_expiry_date()
        expiry = expiry_date.strftime("%b").upper() if expiry_date else ""
        year = expiry_date.strftime("%Y") if expiry_date else ""
        strike = get_strike_for_security(security_id)
        option_type = get_option_type_for_security(security_id)
        
        broker_quantity = int(qty)
        lot_size = get_lot_size_for_security(security_id)
        num_lots = int(broker_quantity // lot_size) if lot_size else broker_quantity
        entry_price = 0.0

        capital_used = entry_price * lot_size * num_lots  

        base_symbol_only = re.split(r'[-\s]', symbol_raw)[0].upper()
        trading_symbol = make_broker_style_symbol(symbol_raw, expiry, year, strike, option_type)

        trade_data = {
            "order_id": order_id,
            "symbol": base_symbol_only, 
            "trading_symbol": trading_symbol,
            "option_security_id": security_id,
            "trade_type": "live",
            "quantity": num_lots,
            "order_status": provisional_status,   
            "entry_time": get_current_ist_time().strftime("%H:%M:%S"),
            "entry_price": entry_price,  
            "timestamp": get_current_ist_time().strftime("%Y-%m-%d"),
            "lot_size": lot_size,
            "capital_used": capital_used,
            "strike": strike,
            "option_type": option_type,
            "pnl": 0,
            "exit_time": "00:00:00",
            "exit_price": 0.0,
            "reason": ""
        }
        return trade_data
    except Exception as e:
        print(f"Error building provisional trade data for order {order_id}: {e}")
        return None

def build_trade_data_for_order(order_id, security_id, txn_type, qty, executed_price):
    try:
        expiry_date = get_expiry_date()  
        expiry = expiry_date.strftime("%b").upper() if expiry_date else ""
        year = expiry_date.strftime("%Y") if expiry_date else ""
        strike = get_strike_for_security(security_id)
        option_type = get_option_type_for_security(security_id)
        symbol_raw = get_symbol_from_security_id(security_id)
        base_symbol_only = re.split(r'[-\s]', symbol_raw)[0].upper()
        
        trading_symbol = make_broker_style_symbol(symbol_raw, expiry, year, strike, option_type)
        
        broker_quantity = int(qty)
        lot_size = get_lot_size_for_security(security_id) or 1
        num_lots = int(broker_quantity // lot_size) if lot_size else broker_quantity
        capital_used = float(executed_price) * lot_size * num_lots
        
        trade_data = {
            "order_id": order_id,
            "symbol": base_symbol_only,      
            "trading_symbol": trading_symbol,
            "option_security_id": security_id,
            "trade_type": "live",
            "quantity": num_lots,
            "order_status": "open",
            "entry_time": get_current_ist_time().strftime("%H:%M:%S"),
            "entry_price": executed_price,
            "timestamp": get_current_ist_time().strftime("%Y-%m-%d"),
            "lot_size": broker_quantity,  
            "capital_used": capital_used,
            "strike": strike,
            "option_type": option_type,
            "pnl": 0,
            "exit_time": "00:00:00",
            "exit_price": 0.0,
            "reason": "",
        }
        return trade_data

    except Exception as e:
        logging.error(f"Error building trade data for order {order_id}: {e}")
        return None

def save_trade_data(data):
    if not data.get("timestamp"):
        trade_date = get_current_ist_time().date()
    else:
        trade_date = datetime.strptime(data.get("timestamp"), "%Y-%m-%d").date()
    
    if not data.get("entry_time"):
        entry_time = get_current_ist_time().time()
    else:
        entry_time = datetime.strptime(data.get("entry_time"), "%H:%M:%S").time()
        
    exit_time = None
    if data.get("exit_time"):
        exit_time = datetime.strptime(data.get("exit_time"), "%H:%M:%S").time()

    trade_dict = {
        "timestamp": trade_date.strftime("%Y-%m-%d"),
        "symbol": data.get("symbol"),
        "trading_symbol": data.get("trading_symbol"),
        "option_type": data.get("option_type"),
        "strike": int(float(data.get("strike") or 0)),
        "quantity": data.get("quantity"),
        "lot_size": data.get("lot_size"),
        "trade_type": data.get("trade_type"),
        "order_status": data.get("order_status"),
        "entry_time": entry_time.strftime("%H:%M:%S"),
        "entry_price": float(data.get("entry_price") or 0),
        "exit_price": float(data.get("exit_price") or 0),
        "exit_time": exit_time.strftime("%H:%M:%S") if exit_time else "00:00:00",
        "reason": data.get("reason") or "",
        "pnl": float(data.get("pnl") or 0),
        "capital_used": float(data.get("capital_used") or 0),
        "option_security_id": int(data.get("option_security_id") or 0),
        "order_id": data.get("order_id")
    }
    resp = supabase.table("trade_log").insert(trade_dict).execute()
    logging.info(f"save_trade_data insert response: {getattr(resp,'status_code',None)} {getattr(resp,'data',None)}")
    return resp
    
def save_or_update_trade_data(data):
    trade_id = data.get("id")
    order_id = data.get("order_id")
    
    order_status_val = data.get("order_status") or "open"
    if order_status_val is None:
        order_status_val = "open"
    
    exit_time_val = data.get("exit_time")
    if order_status_val == "open":
        exit_time_val = "00:00:00"
    else:
        exit_time_val = exit_time_val or get_current_ist_time().strftime("%H:%M:%S")

    reason_val = data.get("reason") or ""
    trade_type_val = data.get("trade_type") or "paper"
    
    filtered_data = {k: v for k,v in data.items() if k != "id"}
    
    def safe_int(key, default=0):
        val = filtered_data.get(key, data.get(key, default))
        try:
            return int(float(val))
        except Exception:
            return default

    def safe_float(key, default=0.0):
        val = filtered_data.get(key, data.get(key, default))
        try:
            return float(val)
        except Exception:
            return default

    def safe_str(key, default=""):
        val = filtered_data.get(key, data.get(key, default))
        return val if val is not None else default
    
    opt_sec_id = safe_int("option_security_id", 0)
    lot_size_val = get_lot_size_for_security(opt_sec_id) or 1
    
    quantity_val = safe_int("quantity", 0)
    lot_size_value = safe_int("lot_size", lot_size_val)

    entry_price_val = safe_float("entry_price", 0.0)
    capital_used_val = 0.0
       
    if order_status_val == "open" and entry_price_val and quantity_val:
        capital_used_val = entry_price_val * lot_size_value * quantity_val
            
    symbol_raw = safe_str("symbol", "")   
    expiry_date = get_expiry_date()  
    expiry = expiry_date.strftime("%b").upper() if expiry_date else ""
    year = expiry_date.strftime("%Y") if expiry_date else ""

    strike = safe_int("strike", 0)
    option_type = safe_str("option_type", "")
    trading_symbol = data.get("trading_symbol") or make_broker_style_symbol(symbol_raw, expiry, year, strike, option_type)
    base_symbol_only = re.split(r'[-\s]', symbol_raw)[0].upper()
    
    base = {
        "timestamp": safe_str("timestamp", get_current_ist_time().strftime("%Y-%m-%d")),
        "symbol": base_symbol_only,       
        "trading_symbol": trading_symbol,
        "option_type": option_type,
        "strike": strike,
        "quantity": quantity_val,
        "lot_size": lot_size_val,
        "trade_type": trade_type_val,         
        "order_status": order_status_val,    
        "entry_time": safe_str("entry_time", get_current_ist_time().strftime("%H:%M:%S")),
        "entry_price": safe_float("entry_price", 0.0),
        "exit_price": safe_float("exit_price", 0.0),
        "exit_time": exit_time_val,
        "reason": reason_val,
        "pnl": safe_float("pnl", 0.0),
        "capital_used": capital_used_val,
        "option_security_id": safe_int("option_security_id", 0),
        "order_id": safe_str("order_id", None),
    }
    if trade_id is None and order_id:
        exists = supabase.table("trade_log").select("id").eq("order_id", order_id).maybe_single().execute()
        if exists and getattr(exists, "data", None):
            trade_id = exists.data["id"]

    if trade_id is not None:
        try:
            trade_id_int = int(trade_id)
        except Exception:
            logging.error(f"Invalid trade_id value: {trade_id}")
            return None
    
        old_lot_size = None
        old_row = supabase.table("trade_log").select("lot_size").eq("id", trade_id_int).maybe_single().execute()
        if old_row and getattr(old_row, "data", None):
            old_lot_size = old_row.data.get("lot_size", None)
            try:
                old_lot_size = int(old_lot_size)
            except Exception:
                old_lot_size = None

        new_lot_size = base.get("lot_size", 1)
        if old_lot_size is not None and old_lot_size > 1:
            base["lot_size"] = old_lot_size
        else:
            base["lot_size"] = new_lot_size  

        logging.info(f"save_or_update_trade_data update: id={trade_id_int}, old_lot_size={old_lot_size}, new_lot_size={new_lot_size}")

        response = supabase.table("trade_log").update(base).eq("id", trade_id_int).execute()
        logging.info(f"save_or_update_trade_data update response: {getattr(response,'status_code',None)} {getattr(response,'data',None)}")
        if getattr(response, "status_code", 200) >= 400:
            logging.error(f"Update failed: {response}")
            return None
        return trade_id_int

    else:
        response = supabase.table("trade_log").insert(base).execute()
        logging.info(f"save_or_update_trade_data insert response: {getattr(response,'status_code',None)} {getattr(response,'data',None)}")
        if getattr(response, "status_code", 200) >= 400:
            logging.error(f"Insert failed: {response}")
            return None
        inserted = response.data[0] if getattr(response, "data", None) else None
        return inserted.get("id") if inserted else None

def save_trade_data_async(data):
    def task():
        try:
            supabase.table("trade_log").insert(data).execute()
        except Exception as e:
            logging.error(f"Async save_trade_data failed: {e}")
    threading.Thread(target=task, daemon=True).start()


def find_open_position(symbol_or_trading_symbol, option_type=None, strike=None, today=None):
    if today is None:
        today = date.today().strftime("%Y-%m-%d")
    
    sym = (str(symbol_or_trading_symbol) or "").strip().upper()
    logging.info(f"Looking for trade: {sym} | Opt: {option_type}")
    
    
    try:
        query = supabase.table("trade_log").select("*") \
            .eq("order_status", "open") \
            .eq("timestamp", today) \
            .in_("trade_type", ["live", "paper"])

        if option_type:
            query = query.eq("option_type", str(option_type).upper())
        
        if strike:
            try:
                query = query.eq("strike", float(strike))
            except: pass

        
        resp = query.or_(f"symbol.eq.{sym},trading_symbol.eq.{sym}").execute()
        data = getattr(resp, 'data', [])

        if data and len(data) > 0:
            return data[0]
            
        return None

    except Exception as e:
        logging.error(f"Search failed for {sym}: {e}")
        return None


def find_open_position_with_broker_sync(trading_symbol, option_type=None, today=None):
    trading_symbol = (trading_symbol or "").strip().upper()

    open_pos = find_open_position(trading_symbol, option_type, today)
    logging.info(f"find_open_position_with_broker_sync open_pos: {open_pos}")
    paper_trade = str(get_setting("paper_trade", "false")).lower() == "true"

    if open_pos and not paper_trade:
        broker_positions = fetch_broker_positions_list()
        match = match_broker_position(open_pos, broker_positions)
        if match:
            return open_pos
        else:
            logging.warning(f"No matching broker position for {open_pos}")
            return None
    return open_pos

def check_broker_open_position(trading_symbol, option_type=None, strike=None, security_id=None, force_refresh=False):
    now = time.time()
    cache_key = (trading_symbol, option_type, strike, security_id)
    
    if not force_refresh:
        cache_entry = _position_cache.get(cache_key)
        if cache_entry and (now - cache_entry['time']) < _cache_expiry_seconds:
            #logging.info(f"Using cached broker position for {trading_symbol}")
            return cache_entry['position']
    
    url = "https://api.dhan.co/v2/positions"
    headers = {
        "access-token": get_setting("dhan_access_token"),
        "client-id": get_setting("dhan_client_id"),
        "Content-Type": "application/json"
    }
    try:
        resp = requests.get(url, headers=headers, timeout=5)
        if resp.status_code != 200:
            logging.error(f"Broker positions API error: {resp.status_code}")
            return None

        data = resp.json()
        positions = data.get("positions", []) if isinstance(data, dict) else data

        if not positions:
            time.sleep(0.5)
            resp = requests.get(url, headers=headers, timeout=5)
            if resp.status_code == 200:
                data2 = resp.json()
                positions = data2.get("positions", []) if isinstance(data2, dict) else data2
        local_trade = {
            "trading_symbol": trading_symbol,
            "option_type": option_type,
            "strike": strike,
            "option_security_id": security_id
        }
        match = match_broker_position(local_trade, positions)
        
        if match and match.get("state") == "open":
            _position_cache[cache_key] = {'time': now, 'position': match}
            logging.info(f"LIVE OPEN: {trading_symbol} found on broker.")
            return match
        
        if cache_key in _position_cache:
            del _position_cache[cache_key]
            
        logging.warning(f"NO LIVE POSITION: {trading_symbol} is closed or not found on broker.")
        return None

    except Exception as e:
        logging.error(f"Error in check_broker_open_position: {e}")
        return None

def normalize_symbol(s):
    if not s: return ""
    return str(s).replace("-", "").replace("_", "").replace(".", "").upper().strip()

def map_opt(t):
    t = (t or "").upper().strip()
    return {"CALL": "CE", "PUT": "PE", "CE": "CE", "PE": "PE"}.get(t, t)

def match_broker_position(local_trade, broker_positions):
    
    raw_local_trading_symbol = local_trade.get("trading_symbol") or local_trade.get("symbol")
    target_symbol = normalize_symbol(local_trade.get("trading_symbol") or local_trade.get("symbol"))
    target_option_type = map_opt(local_trade.get("option_type"))
    target_strike = local_trade.get("strike")
    target_sec_id = str(local_trade.get("option_security_id") or "").strip()

    target_strike_val = None
    if target_strike is not None and target_strike != "":
        try:
            target_strike_val = float(target_strike)
        except Exception:
            target_strike_val = str(target_strike).strip()
    
    logging.info(
        f"LOCAL TRADE RAW => trading_symbol={raw_local_trading_symbol}, "
        f"opt_type={local_trade.get('option_type')}, "
        f"strike={target_strike}, sec_id={target_sec_id}"
    )
    logging.info(
        f"LOCAL TARGET => symbol={target_symbol}, "
        f"opt_type={target_option_type}, strike_val={target_strike_val}"
    )
    
    for pos in broker_positions:
        pos_symbol = normalize_symbol(pos.get("tradingSymbol") or pos.get("symbol") or "")
        pos_type = (pos.get("positionType") or "").upper().strip()

        pos_option_type_raw = ""
        if "drvOptionType" in pos:
            pos_option_type_raw = str(pos["drvOptionType"])
        elif "option_type" in pos:
            pos_option_type_raw = str(pos["option_type"])
        pos_option_type = map_opt(pos_option_type_raw)  

        pos_strike = pos.get("strike") or pos.get("strikePrice")
        pos_sec_id = str(pos.get("securityId") or pos.get("option_security_id") or "").strip()
        net_quantity = int(pos.get("netQty") or pos.get("tradedQuantity") or 0)
        
        logging.info(f"Checking Local Trade ({target_symbol}, {target_option_type}, {target_strike_val})")
        logging.info(f"Against Broker Pos ({pos_symbol}, {pos_option_type_raw}->{pos_option_type}, {pos_strike}) Net Qty: {net_quantity}, Type: {pos_type}")
        
        is_strike_match = False
        if target_strike_val is None or pos_strike is None:
            is_strike_match = True
        else:
            try:
                is_strike_match = abs(float(pos_strike) - float(target_strike_val)) < 0.01
            except Exception:
                is_strike_match = False
        
        symbol_ok = (pos_symbol == target_symbol)
        opt_ok = (not target_option_type or pos_option_type == target_option_type)
        strike_ok = is_strike_match
        secid_ok = (not target_sec_id or pos_sec_id == target_sec_id)

        logging.info(
            f"MATCH CHECK => symbol_ok={symbol_ok}, opt_ok={opt_ok}, "
            f"strike_ok={strike_ok}, secid_ok={secid_ok}"
        )

        if symbol_ok and opt_ok and strike_ok and secid_ok:
            buy_avg = float(pos.get("buyAvg", 0) or pos.get("costPrice", 0) or 0)
            
            if pos_type == "CLOSED" or buy_avg == 0:
                logging.info("Matched CLOSED/flat broker position.")
                return {"pos": pos, "state": "closed"}
            
            elif pos_type == "LONG" and buy_avg > 0:
                logging.info(f"Matched LONG broker position (buyAvg={buy_avg}>0).")
                return {"pos": pos, "state": "open"}
            else:
                logging.info(f"Matched unknown pos_type '{pos_type}' with buyAvg={buy_avg}, treating as closed.")
                return {"pos": pos, "state": "closed"}

    logging.info("FAILURE: No matching broker position found.")
    return None

MAX_RETRIES = 3
RETRY_INTERVAL = 3 
    
def poll_and_update_transit_orders():
    logging.info("Polling started")
    changes = []
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
                
        def map_status(status):
                if status in ["REJECTED", "FAILED", "CANCELLED"]:
                    return "failed"
                if status == "CONFIRM":
                    return "pending"
                if status == "TRANSIT":
                    return "transit"
                if status in ["TRADED", "PART_TRADED"]:
                    return "open"
                return "unknown"
        
        for i in range(MAX_RETRIES):
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
            logging.info(f"Status for order {order_id}: {status}, Executed price: {executed_price}")
            
            mapped_status = map_status(status)
            
            if mapped_status == "failed":
                try:
                    supabase.table("trade_log").update(
                        {"order_status": "failed"}
                    ).eq("id", trade["id"]).execute()
                    logging.info(f"Updated trade {trade['id']} to status failed")
                    changes.append({"trade_id": trade["id"], "order_status": "failed"})
                except Exception as e:
                    logging.error(f"DB update exception for failed order {order_id}: {e}")
                break
            
            elif mapped_status == "transit":
                try:
                    supabase.table("trade_log").update(
                        {"order_status": "transit"}
                    ).eq("id", trade["id"]).execute()
                    logging.info(f"Updated trade {trade['id']} to status transit")
                    changes.append({"trade_id": trade["id"], "order_status": "transit"})
                except Exception as e:
                    logging.error(f"DB update exception for transit order {order_id}: {e}")
                break
            
            elif mapped_status == "pending":
                try:
                    supabase.table("trade_log").update(
                        {"order_status": "pending"}
                    ).eq("id", trade["id"]).execute()
                    logging.info(f"Updated trade {trade['id']} to status pending")
                    changes.append({"trade_id": trade["id"], "order_status": "pending"})
                except Exception as e:
                    logging.error(f"DB update exception for pending order {order_id}: {e}")
                break
            
            elif mapped_status == "open" and executed_price and float(executed_price) > 0:
                try:
                    supabase.table("trade_log").update({
                        "order_status": "open",
                        "entry_price": float(executed_price),
                    }).eq("id", trade["id"]).execute()
                    logging.info(f"Updated trade {trade['id']} to status open with entry_price {executed_price}")
                    changes.append({"trade_id": trade["id"], "order_status": "open"})
                except Exception as e:
                    logging.error(f"DB update exception for open order {order_id}: {e}")
                break
            else:
                logging.warning(f"Retry {i+1}/{MAX_RETRIES} for order {order_id}. Status: {mapped_status}")
                if i < MAX_RETRIES - 1:
                    time.sleep(RETRY_INTERVAL)
                else:
                    try:
                        supabase.table("trade_log").update(
                            {"order_status": "transit"}
                        ).eq("id", trade["id"]).execute()
                        logging.info(f"Final retry failed for {order_id}. Set to transit.")
                        changes.append({"trade_id": trade["id"], "order_status": "transit"})
                    except Exception as e:
                        logging.error(f"Failed to set transit for {order_id}: {e}")
    try:
        broker_positions = fetch_broker_positions_list()
        open_trades = supabase.table("trade_log").select("*")\
            .in_("order_status", ["open", "pending", "transit"])\
            .eq("trade_type", "live").execute().data or []
        logging.info(f"Checking {len(open_trades)} live trades against {len(broker_positions)} broker positions.")

        for trade in open_trades:
            trade_id = trade.get("id")
            order_id = str(trade.get("order_id", "")).strip()
            current_status = trade.get('order_status', '').lower()
                                    
            if current_status in ["pending", "transit"]:
                logging.info(f"Trade id {trade_id} is {current_status}; skipping positional sync check.")
                continue
            
            order_status, _, _ = check_order_status(order_id)
            logging.info(f"Fresh order status for trade {trade_id}: {order_status}")
                                    
            if order_status == "FAILED":
                logging.info(f"Trade id {trade_id} order FAILED; marking failed.")
                exit_time_str = get_current_ist_time().strftime("%H:%M:%S")
                supabase.table("trade_log").update({
                    "order_status": "failed",
                    "exit_time": exit_time_str,
                    "exit_price": trade.get("entry_price", 0),
                    "reason": "order_failed"
                }).eq("id", trade_id).execute()
                clear_position_cache(trade.get("symbol"), trade.get("option_type"), trade.get("strike"), order_id)
                changes.append({"trade_id": trade_id, "order_status": "closed"})
                continue
            
            match = match_broker_position(trade, broker_positions)
            if match:
                state = match["state"]
                if state == "open":
                    logging.info(f"Trade id {trade_id} matched with OPEN broker position.")
                    supabase.table("trade_log").update({
                        "order_status": "open",
                        "exit_time": "00:00:00",
                        "reason": ""
                    }).eq("id", trade_id).execute()
                    changes.append({"trade_id": trade_id, "order_status": "open"})
                else:  # closed
                    logging.info(f"Trade id {trade_id} matched with CLOSED broker position. Marking closed.")
                    exit_time_str = get_current_ist_time().strftime("%H:%M:%S")
                    supabase.table("trade_log").update({
                        "order_status": "closed",
                        "exit_time": exit_time_str,
                        "exit_price": trade.get("entry_price", 0),
                        "reason": "broker_position_closed"
                    }).eq("id", trade_id).execute()
                    clear_position_cache(trade.get("symbol"), trade.get("option_type"), trade.get("strike"), order_id)
                    changes.append({"trade_id": trade_id, "order_status": "closed"})
            else:
                if order_status == "CONFIRM":
                    logging.warning(f"Trade id {trade_id}: CONFIRM order but no broker position yet. Keeping status as is.")
                else:
                    logging.info(f"Trade id {trade_id}: No match and order {order_status}. Keeping {current_status}.")

        logging.info("Polling completed")
        return changes
    except Exception as e:
        logging.error(f"Error fetching broker positions or processing trades: {e}")
        return changes    
               
def get_index_ltp(symbol): 
    symbol = symbol.upper()
    security_id = index_symbol_to_id_map.get(symbol)
    if security_id is None:
        logging.warning(f"No security ID found for {symbol}")
        return None
    
    ltp = market_data_cache.get(str(security_id))
    if ltp is None:
        logging.warning(f"LTP for {symbol} (security_id: {security_id}) not found in WebSocket cache")
    return ltp

def place_order_async(security_id, txn_type, qty=1):
    def task():
        try:
            place_order(security_id, txn_type, qty)
        except Exception as e:
            logging.error(f"Async place_order failed: {e}")
    threading.Thread(target=task, daemon=True).start()

def fetch_broker_positions_list():
    url = url = "https://api.dhan.co/v2/positions"
    headers = {
        "access-token": get_setting("dhan_access_token"),
        "client-id": get_setting("dhan_client_id"),
        "Content-Type": "application/json"
    }
    positions = []
    logging.info(f"Fetching broker positions from URL: {url}")
    try:
        resp = requests.get(url, headers=headers)
        logging.info(f"Broker Positions Response (Status {resp.status_code}): {resp.text[:200]}...")
        if resp.status_code == 200:
            data = resp.json()
            logging.debug(f"PARSED BROKER POSITIONS LIST DATA: {json.dumps(data, indent=2)}")
            if isinstance(data, list):
                logging.info("Broker response type: list.")
                logging.debug(f"Broker positions snippet: {data[:2]}")
                positions = data
            elif isinstance(data, dict):
                positions = data.get("positions", [])
                logging.info(f"Broker response type: dict (positions key). Total items: {len(positions)}") # <-- UPDATED LOG
                logging.debug(f"Broker positions snippet: {positions[:2]}")
            
            if positions:
                logging.info(f"--- Broker Positions (Total: {len(positions)}) ---")
                for idx, pos in enumerate(positions):
                    symbol = pos.get('tradingSymbol') or pos.get('symbol', 'N/A')
                    p_type = pos.get('positionType', 'N/A')
                    net_qty = pos.get('netQty', 0)
                    
                    logging.info(f"POS {idx + 1}: Symbol={symbol}, Type={p_type}, Net Qty={net_qty}")
                logging.info("------------------------------------")
            
        else:
            logging.error(f"Broker positions API failed with status code: {resp.status_code}")
    except requests.exceptions.Timeout:
        logging.error("Request timed out while fetching broker positions.")
    except Exception as e:
        logging.error(f"Could not fetch broker positions: {e}")
    return positions
   
def clear_position_cache(trading_symbol, option_type=None, strike=None, order_id=None):
    cache_key = (trading_symbol, option_type, strike, order_id)
    if cache_key in _position_cache:
        del _position_cache[cache_key]

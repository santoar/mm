import re
import os
import uuid
import json
import time
import logging
import requests
import threading
import collections
import master_data
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


logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(message)s',
                    handlers=[logging.StreamHandler()])

logging.info("Debug message")

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

def periodic_cache_refresh(interval_sec=120):
    def refresh_loop():
        while True:
            try:
                refresh_settings_cache()
            except Exception as e:
                logging.error(f"Periodic cache refresh failed: {e}")
            time.sleep(interval_sec)
    thread = threading.Thread(target=refresh_loop, daemon=True)
    thread.start()


def get_dhan_access_token():
    return get_setting("dhan_access_token")


def get_dhan_client_id():
    return get_setting("dhan_client_id")



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
    except Exception as e:
        logging.error(f"is_duplicate_trade query failed: {e}")
        return False


global_rate_limit_lock = threading.Lock()
last_global_api_call_time = 0
global_min_interval = 0.05  

def global_rate_limit_wait():
    global last_global_api_call_time
    with global_rate_limit_lock:
        now = time.time()
        elapsed = now - last_global_api_call_time
        wait_time = global_min_interval - elapsed
        if wait_time > 0:
            time.sleep(wait_time)
        last_global_api_call_time = time.time()

api_call_timestamps = collections.deque()

def log_api_call():
    now = time.time()
    api_call_timestamps.append(now)
    
    while api_call_timestamps and (now - api_call_timestamps[0]) > 1:
        api_call_timestamps.popleft()
    logging.info(f"API calls in last 1 second: {len(api_call_timestamps)}")

class DhanApiManager:
    def __init__(self, api_type='order'):
        self.last_api_call_time = 0
        self.lock = threading.Lock()
        if api_type == 'order':
            self.rate_limit_interval = 0.04
        elif api_type == 'data':
            self.rate_limit_interval = 0.2 
        elif api_type == 'quote':
            self.rate_limit_interval = 1  
        elif api_type == 'non_trading':
            self.rate_limit_interval = 0.05
        else:
            self.rate_limit_interval = 0.05 

    def _wait_for_rate_limit(self):
        current_time = time.time()
        time_since_last_call = current_time - self.last_api_call_time
        if time_since_last_call < self.rate_limit_interval:
            wait_time = self.rate_limit_interval - time_since_last_call
            logging.debug(f"API Rate limit per type, waiting for {wait_time:.2f}s...")
            time.sleep(wait_time)
        self.last_api_call_time = time.time()
    
    def make_request(self, method, url, headers, json_data):
        print(f"DEBUG: API call to {url}")
        logging.info(f"make_request called with URL: {url}")
        self._wait_for_rate_limit()
        global_rate_limit_wait()
        log_api_call()
        if method.lower() == 'post':
            return requests.post(url, headers=headers, json=json_data)
        elif method.lower() == 'get':
            return requests.get(url, headers=headers)
        return None

order_api_manager = DhanApiManager(api_type='order')
data_api_manager = DhanApiManager(api_type='data')
quote_api_manager = DhanApiManager(api_type='quote')
non_trading_api_manager = DhanApiManager(api_type='non_trading')


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

api_call_cache = {}
best_bid_ask_cache = {}
COOLDOWN_SECONDS = 1.5
CACHE_LIFETIME = 7

def get_best_bid_ask(security_id):
    now = time.time()
    if security_id in api_call_cache:
        elapsed = now - api_call_cache[security_id]
        if elapsed < COOLDOWN_SECONDS:
            logging.info(f"Skipping bid/ask call for {security_id}, cooldown {elapsed:.2f}s")
            if security_id in best_bid_ask_cache:
                cached_time, bid_ask = best_bid_ask_cache[security_id]
                if now - cached_time < CACHE_LIFETIME:
                    return bid_ask
            return None, None
    if security_id in best_bid_ask_cache:
        cached_time, bid_ask = best_bid_ask_cache[security_id]
        if now - cached_time < CACHE_LIFETIME:
            return bid_ask
    
    try:
        url = "https://api.dhan.co/v2/marketfeed/quote"
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "access-token": get_setting("dhan_access_token"),
            "client-id": get_setting("dhan_client_id"),   
        }
        body = {"NSE_FNO": [security_id]}
                      
        response = quote_api_manager.make_request('post', url, headers, body)
        resp = response.json()
        
        
        if resp.get("status") != "success" or not resp.get("data"):
            logging.warning(f"Market depth API failed for security {security_id}: {resp}")
            return None, None

        security_data = resp["data"]["NSE_FNO"].get(str(security_id), {})
        depth = security_data.get("depth", {})

        buy_depth = depth.get("buy", [])
        sell_depth = depth.get("sell", [])

        best_bid = buy_depth[0].get("price", None) if buy_depth else None
        best_ask = sell_depth[0].get("price", None) if sell_depth else None
        if best_bid is None or best_ask is None:
            logging.warning(f"Best bid or ask price missing for {security_id}")
            return None, None

        best_bid_ask_cache[security_id] = (now, (best_bid, best_ask))
        api_call_cache[security_id] = now
        return best_bid, best_ask
    except Exception as e:
        logging.error(f"Error fetching best bid/ask for {security_id}: {e}")
        return None, None
        
def throttled_get_best_bid_ask(security_id):
    global last_called_security, last_call_time
    now = time.time()
    if security_id == last_called_security and (now - last_call_time) < min_interval:
        logging.info(f"Skipping bid/ask call for security {security_id} to respect rate limit")
        return None, None
    last_called_security = security_id
    last_call_time = now
    return get_best_bid_ask(security_id)


       
def place_order(security_id, txn_type, qty=1):
    client_id = get_setting("dhan_client_id")
    access_token = get_setting("dhan_access_token")
    best_bid, best_ask = get_best_bid_ask(security_id)
    if not best_bid or not best_ask or best_bid <= 0 or best_ask <= 0:
        logging.error("Invalid bid or ask price, skipping order placement")
        return None
    avg_price = (best_bid + best_ask) / 2
    avg_price_rounded = round_to_0_05(avg_price)
    
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
    if txn_type.upper() == "SELL":
        order_type = "MARKET"
        price = None  
    else:
        order_type = "LIMIT"
        price = avg_price_rounded
    
    body = {
        "dhanClientId": get_setting("dhan_client_id"),
        "correlationId": f"order_{security_id}_{int(time.time())}",
        "transactionType": txn_type.upper(),
        "exchangeSegment": "NSE_FNO",
        "productType": "INTRADAY",
        "orderType": "LIMIT",
        "validity": "DAY",
        "securityId": str(security_id),
        "quantity": qty,
        "disclosedQuantity": 0,
        "price": avg_price_rounded,
        "tradingSymbol": trading_symbol
    }
        
    try:
        logging.info("Sending order to Dhan API")
        response = order_api_manager.make_request('post', url, headers, body)
        resp_json = response.json()
        resp_json["tradingSymbol"] = trading_symbol or resp_json.get("tradingSymbol", "")
        order_id = resp_json.get("orderId")
        order_status = resp_json.get("orderStatus", "").upper()

        if not order_id:
            logging.error(f"Order ID missing in response: {resp_json}")
            return None
        if order_status in ["FAILED", "REJECTED"]:
            logging.error(f"Live order failed: {resp_json}")
            return None
        elif order_status in ["PENDING", "TRANSIT"]:
            logging.info(f"Live order in transit: {resp_json}")
            return resp_json
        elif order_status == "CONFIRM":
            logging.info(f"Live order placed successfully: {resp_json}")
            return resp_json
        else:
            logging.warning(f"Live order returned unhandled status: {resp_json}")
            return None
    except Exception as e:
        logging.error(f"Error placing live order: {e}")
        return False

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
            logging.info(f"Provisional trade record inserted with ID {provisional_id}")

        check_order_status_with_interval_and_save(order_id, security_id, txn_type, qty)

        return order_resp
    else:
        logging.error("Order placement failed or missing orderId")
        return None


def check_order_status_with_interval_and_save(order_id, security_id, txn_type, qty, interval=3, max_retries=50):
    from app import app
    def task():
        from shared_objects import socketio
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
                return    

            if retry_count < max_retries - 1:
                time.sleep(interval)

        logging.warning(f"Order {order_id} not confirmed after max retries.")

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
            "reason": "max_retries_exceeded",
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

    threading.Thread(target=task, daemon=True).start()


def check_order_status(order_id):
    token = get_dhan_access_token()
    client_id = get_dhan_client_id()
    if not token:
        logging.error("Dhan access token missing")
        return "UNKNOWN", None
    url = f"https://api.dhan.co/v2/orders/{order_id}"
    headers = {
        "Content-Type": "application/json",
        "access-token": get_setting("dhan_access_token"),
        "client-id": get_setting("dhan_client_id"),
    }
    try:
        response = order_api_manager.make_request('get', url, headers, None)
        if response.status_code == 200:
            data = response.json()
            if isinstance(data, list):
                data = data[0] if len(data) > 0 else {}
            
            status = data.get("orderStatus", "").upper()
            executed_price = (
                data.get("averageTradedPrice")
                or data.get("tradedPrice")
                or data.get("price")      
            )
            try:
                executed_price = float(executed_price) if executed_price is not None else None
            except Exception:
                executed_price = None
            
            trading_symbol = data.get("tradingSymbol", "")
            
            if status in ["TRADED", "PART_TRADED", "CONFIRM"]:
                return "CONFIRM", executed_price, trading_symbol
            elif status in ["REJECTED", "FAILED", "CANCELLED"]:
                return "FAILED", executed_price, trading_symbol
            elif status in ["PENDING", "TRANSIT"]:
                return "TRANSIT", executed_price, trading_symbol
            else:
                return "UNKNOWN", executed_price, trading_symbol
        else:
            logging.error(f"API error {response.status_code} for order {order_id}")
            return "UNKNOWN", None
    except Exception as e:
        logging.error(f"Exception checking order status: {e}")
        return "UNKNOWN", None


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
        price = None

    if not paper_trade and security_id:
        from ws_client import get_last_ltp
        ltp = get_last_ltp(str(security_id))
        if ltp and float(ltp) > 0:
            return float(ltp)
        best_bid, best_ask = get_best_bid_ask(security_id)
        if best_bid and best_ask:
            return (best_bid + best_ask) / 2
    return 0

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
    
    symbol_raw = data.get("symbol")
    expiry_date = get_expiry_date()  
    expiry = expiry_date.strftime("%b").upper() if expiry_date else ""
    year = expiry_date.strftime("%Y") if expiry_date else ""

    strike = data.get("strike")
    option_type = data.get("option_type")
    trading_symbol = make_broker_style_symbol(symbol_raw, expiry, year, strike, option_type)
    
    quantity = int(data.get("quantity") or 0)
    lot_size = int(data.get("lot_size") or 1)
    
    base_symbol_only = re.split(r'[-\s]', symbol_raw)[0].upper()

    trade_dict = {
        "timestamp": trade_date.strftime("%Y-%m-%d"),
        "symbol": base_symbol_only,    
        "trading_symbol": trading_symbol,
        "option_type": data.get("option_type"),
        "strike": int(float(data.get("strike") or 0)),
        "quantity": quantity, 
        "lot_size": lot_size,  
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
    
    exit_time_val = data.get("exit_time") or "00:00:00"
    reason_val = data.get("reason") or ""
    trade_type_val = data.get("trade_type") or "paper"
    order_status_val = data.get("order_status") or "open"
    
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
       
    if order_status_val == "open" and safe_float("entry_price", 0.0) and quantity_val:
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
        "exit_time": get_current_ist_time().strftime("%H:%M:%S"),
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
        if old_lot_size and old_lot_size > 1:
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

    cache_key = (symbol_or_trading_symbol, option_type, strike, today)
    now = time.time()
    cache_entry = _position_cache.get(cache_key)
    
    if cache_entry and now - cache_entry['time'] < _cache_expiry_seconds:
        return cache_entry['data']

    logging.info(f"Query params => symbol_or_trading_symbol: {symbol_or_trading_symbol}, option_type: {option_type}, strike: {strike}, date: {today}")

    try:
        query = supabase.table("trade_log").select(
            "id,symbol,trading_symbol,option_type,strike,option_security_id,entry_price,order_status,quantity,lot_size"
        ).eq("order_status", "open").in_("trade_type", ["live", "paper"]).eq("timestamp", today)
        if option_type is not None:
            query = query.eq("option_type", option_type)
        if strike is not None:
            query = query.eq("strike", float(strike))
        resp = query.eq("symbol", symbol_or_trading_symbol).execute()
        data = getattr(resp, 'data', None)
        if data and len(data):
            trade = data[0]
        else:
            query2 = supabase.table("trade_log").select(
                "id,symbol,trading_symbol,option_type,strike,option_security_id,entry_price,order_status,quantity,lot_size"
            ).eq("order_status", "open").in_("trade_type", ["live", "paper"]).eq("timestamp", today)
            if option_type is not None:
                query2 = query2.eq("option_type", option_type)
            if strike is not None:
                try:
                    strike_val = int(strike)
                except (TypeError, ValueError):
                    strike_val = strike
                query2 = query2.eq("strike", strike_val)
            resp2 = query2.eq("symbol", symbol_or_trading_symbol).execute()
            data2 = getattr(resp2, 'data', None)
            if data2 and len(data2):
                trade = data2[0]
            else:
                trade = None

        if trade:
            result = {
                'id': trade.get('id'),
                'symbol': trade.get('symbol'),
                'trading_symbol': trade.get('trading_symbol'),
                'option_type': trade.get('option_type'),
                'strike': trade.get('strike'),
                'option_security_id': trade.get('option_security_id'),
                'entry_price': trade.get('entry_price'),
                'order_status': trade.get('order_status'),
                'quantity': trade.get('quantity'),
                'lot_size': trade.get('lot_size')
            }
            _position_cache[cache_key] = {'time': now, 'data': result}
            return result
        else:
            logging.info("No open position found.")
            return None

    except Exception as e:
        logging.error(f"Supabase query exception: {e}")
        return None

_position_cache = {}
_cache_expiry_seconds = 10

def find_open_position_with_broker_sync(trading_symbol, option_type=None, today=None):
    open_pos = find_open_position(trading_symbol, option_type, today)
    logging.info(f"find_open_position_with_broker_sync open_pos: {open_pos}")
    paper_trade = str(get_setting("paper_trade", "false")).lower() == "true"

    if open_pos:
        if not paper_trade:
            broker_pos = check_broker_open_position(trading_symbol, option_type)
            logging.info(f"find_open_position_with_broker_sync broker_pos: {broker_pos}")

            if broker_pos:
                return open_pos
            else:
                return None
        else:
            return open_pos
    return None

def check_broker_open_position(trading_symbol, option_type=None, strike=None, order_id=None):
    now = time.time()
    cache_key = (trading_symbol, option_type, strike, order_id)
    cache_entry = _position_cache.get(cache_key)
    if cache_entry and (now - cache_entry['time']) < _cache_expiry_seconds:
        return cache_entry['position']
    
    url = "https://api.dhan.co/v2/positions"
    headers = {
        "access-token": get_setting("dhan_access_token"),
        "client-id": get_setting("dhan_client_id"),
        "Content-Type": "application/json"
    }
    try:
        resp = requests.get(url, headers=headers)
        if resp.status_code == 200:
            data = resp.json()
            positions = data.get("positions", []) if isinstance(data, dict) else []
            logging.info(f"RAW BROKER POSITIONS: {json.dumps(positions, indent=2)}")
            debug_errors = []
            def normalize_symbol(s):
                return str(s).replace("-", "").replace("_", "").replace(".", "").lower().strip()

            for pos in positions:
                pos_trading_symbol = normalize_symbol(pos.get("tradingSymbol", ""))
                input_symbol = normalize_symbol(trading_symbol)
                pos_option_type = str(pos.get("drvOptionType", "")).lower().strip()
                input_option_type = str(option_type).lower().strip() if option_type else None
                pos_position_type = str(pos.get("positionType", "")).upper().strip()
                pos_net_qty = int(float(pos.get("netQty", 0)))
                
                match = (pos_trading_symbol == input_symbol)
                if input_option_type is not None:
                    match = match and (pos_option_type == input_option_type)
                match = match and (pos_position_type in ["OPEN", "LONG"]) and (pos_net_qty > 0)

                logging.info(f"TRY-MATCH: {pos_trading_symbol}|{pos_option_type}|{pos_strike_val}|{pos_position_type}|{pos_net_qty}  VS  {input_symbol}|{input_option_type}|{input_strike_val}")

                if match:
                    _position_cache[cache_key] = {'time': now, 'position': pos}
                    logging.info(f"MATCHED BROKER POSITION: {pos}")
                    return pos
            logging.warning(f"NO BROKER POSITION MATCHED for trading_symbol={trading_symbol}")

        else:
            logging.error(f"Broker positions API HTTP error: {resp.status_code}")
        return None
    except Exception as e:
        logging.error(f"Error fetching broker positions: {e}")
        return None
                
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


def exit_position_core_logic(symbol, option_type, strike):
    from ws_client import get_last_ltp
    exit_order_resp = None
    
    paper_trade = get_setting("paper_trade")
    if isinstance(paper_trade, str):
        paper_trade = paper_trade.lower() == 'true'
    
    if paper_trade:
        open_pos = find_open_position(symbol, option_type, strike)
        if not open_pos:
            return {"status": "No open position to exit in paper trade"}, 200
        
        row_id = open_pos.get("id")
        if not row_id:
            return {"error": "DB row id for open position not found"}, 400
        
        opt_sec_id = open_pos.get("option_security_id")
        lot_size_val = open_pos.get('lot_size')
        try:
            lot_size = int(lot_size_val)
            if lot_size in (None, 0, 1):
                raise ValueError(f"Lot size invalid: {lot_size}")
        except Exception:
            lot_size = int(get_lot_size_for_security(opt_sec_id) or 100)
        actual_exit_price = get_last_ltp(str(opt_sec_id)) or 0
        try:
            actual_exit_price = round_to_0_05(float(actual_exit_price))
        except Exception:
            actual_exit_price = 0

        pnl_points = actual_exit_price - float(open_pos.get('entry_price', 0))
        quantity = int(open_pos.get('quantity', 0))  
        actual_pnl = pnl_points * lot_size * quantity                 
        order_id = open_pos.get("order_id") or str(uuid.uuid4())
        
        update_dict = {
            "id": row_id,
            "symbol": open_pos.get("symbol", "EMPTY"),
            "trading_symbol": open_pos.get("trading_symbol", "EMPTY"),
            "option_type": open_pos.get("option_type", "EMPTY"),
            "strike": open_pos.get("strike", 0),
            "entry_price": open_pos.get("entry_price", 0.0),
            "order_status": "closed",
            "exit_price": float(actual_exit_price),
            "exit_time": get_current_ist_time().strftime("%H:%M:%S"),
            "reason": "exited",
            "pnl": actual_pnl,
            "order_id": order_id,
            "exit_order_id": order_id,
            "quantity": quantity,
            "lot_size": lot_size,
        }
        save_or_update_trade_data(update_dict)
        return {"status": "Paper trade position closed locally."}, 200
    
    else:
        max_retries = 3
        retry_delay = 2  
        broker_pos = None
        order_id = None
        
        for attempt in range(max_retries):
            broker_pos = check_broker_open_position(symbol, option_type, strike, order_id)
            if broker_pos:
                logging.info(f"Broker position found on attempt {attempt+1}")
                break
            else:
                logging.info(f"Broker position missing, retrying {attempt+1}/{max_retries}...")
                time.sleep(retry_delay)
        
        if broker_pos is None:
            return {"error": "Broker open position not found; exit order refused"}, 400
        
        
        opt_sec_id = broker_pos.get("securityId")
        total_quantity = int(float(broker_pos.get("netQty", 0)))
        entry_price = float(broker_pos.get("costPrice", 0))
        
        open_pos = find_open_position(symbol, option_type, strike)
        if not open_pos or not open_pos.get('id'):
            return {"error": "No matching open trade to close"}, 400

        lot_size_val = open_pos.get('lot_size')
        try:
            lot_size = int(lot_size_val)
            if lot_size in (None, 0, 1):
                raise ValueError(f"Lot size invalid: {lot_size}")
        except Exception:
            lot_size = int(get_lot_size_for_security(opt_sec_id) or 100)

        lots = int(total_quantity // lot_size) if lot_size else total_quantity

        try:
            exit_order_resp = place_order(opt_sec_id, "SELL", total_quantity, immediate_exit=True)
            logging.info(f"place_order response: {exit_order_resp}")
        
            if (not exit_order_resp) or ("orderId" not in exit_order_resp):
                logging.error("Sell order failed or missing orderId")
                return {"error": "Broker sell order failed or missing orderId"}, 500
        
            order_id = exit_order_resp.get("orderId")
            actual_exit_price = exit_order_resp.get("executed_price") or get_last_ltp(str(opt_sec_id))
            actual_exit_price = round_to_0_05(float(actual_exit_price))
        except Exception as e:
            return {"error": f"Exception during place_order: {e}"}, 500
        
               
        pnl_points = actual_exit_price - entry_price
        actual_pnl = pnl_points * total_quantity
        
        row_id = open_pos['id']
        trading_symbol = open_pos.get("trading_symbol", "") or ""
        order_id = exit_order_resp.get("orderId")
        update_dict = {
            "symbol": open_pos.get("symbol", "EMPTY"),
            "trading_symbol": open_pos.get("trading_symbol", "EMPTY"),
            "option_type": open_pos.get("option_type", "EMPTY"),
            "strike": open_pos.get("strike", 0),
            "entry_price": open_pos.get("entry_price", 0.0),
            "order_status": "closed",
            "exit_price": float(actual_exit_price),
            "exit_time": get_current_ist_time().strftime("%H:%M:%S"),
            "reason": "broker_closed",
            "pnl": float(actual_pnl),
            "order_id": order_id,   
            "exit_order_id": order_id,
            "quantity": total_quantity,
            "lot_size": lot_size     
        }
        try:
            resp = supabase.table("trade_log").update(update_dict).eq("id", row_id).execute()
        except Exception as e:
            return {"error": f"Exception during DB update: {e}"}, 500
        
        return {"status": "Live exit successful, broker sell order placed and DB updated"}, 200

def fetch_broker_positions_list():
    url = "https://api.dhan.co/v2/positions"
    headers = {
        "access-token": get_setting("dhan_access_token"),
        "client-id": get_setting("dhan_client_id"),
        "Content-Type": "application/json"
    }
    try:
        resp = requests.get(url, headers=headers)
        if resp.status_code == 200:
            data = resp.json()
            if isinstance(data, list):
                return data
            elif isinstance(data, dict):
                return data.get("positions", [])
    except Exception as e:
        logging.error(f"Could not fetch broker positions: {e}")
    return []

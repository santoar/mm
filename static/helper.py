import os
import time
import pytz
import logging
import requests
import master_data
from flask import Flask
from datetime import date
from shared_objects import market_data_cache
from datetime import datetime, timedelta
from supabase import create_client
from master_data import equity_symbol_to_id as equity_symbol_to_id_map
import threading
from supabase import create_client

# --- Time Utilities ---
from time_utils import get_current_ist_time, format_ist_time, convert_utc_to_ist


SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
print(f"Supabase URL: {SUPABASE_URL}")
print(f"Supabase KEY: {SUPABASE_KEY}")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[logging.StreamHandler()]
)

_access_token_cache = None
_client_id_cache = None
_cache_lock = threading.Lock()


def refresh_token_cache():
    global _access_token_cache, _client_id_cache
    with _cache_lock:
        logging.info("Starting cache refresh")
        try:
            logging.info("Starting cache refresh")
            resp_token = supabase.table("settings").select("value").eq("key", "dhan_access_token").execute()
            logging.info(f"dhan_access_token response: {resp_token.data}")
            if resp_token.data and len(resp_token.data) > 0:
                _access_token_cache = resp_token.data[0]["value"]
                logging.info(f"Access token updated in cache: {_access_token_cache[:10]}...")
            else:
                logging.warning("Could not refresh dhan_access_token from supabase")

            resp_client = supabase.table("settings").select("value").eq("key", "dhan_client_id").execute()
            logging.info(f"dhan_client_id response: {resp_client.data}")
            if resp_client.data and len(resp_client.data) > 0:
                _client_id_cache = resp_client.data[0]["value"]
                logging.info(f"Client ID updated in cache: {_client_id_cache}")
            else:
                logging.warning("No client ID data found")
            
        except Exception as e:
            logging.error(f"Error refreshing cache: {e}")
            

def get_access_token_cached():
    global _access_token_cache
    with _cache_lock:
        return _access_token_cache

def get_client_id_cached():
    global _client_id_cache
    with _cache_lock:
        return _client_id_cache

def schedule_daily_cache_refresh():
    refresh_token_cache()
    logging.info(f"Initial token cache refreshed at {datetime.now()} IST")

    def refresh_loop():
        while True:
            try:
                now = datetime.now(pytz.timezone('Asia/Kolkata'))
                next_times = [
                    now.replace(hour=9, minute=10, second=0, microsecond=0),
                    now.replace(hour=21, minute=10, second=0, microsecond=0)
                ]
                # shift to next day if time already passed
                next_events = [t if t > now else t + timedelta(days=1) for t in next_times]
                next_refresh_time = min(next_events)
                wait_seconds = (next_refresh_time - now).total_seconds()
                logging.info(f"Waiting {wait_seconds/60:.2f} minutes for next token cache refresh at {next_refresh_time}")
                time.sleep(wait_seconds)

                refresh_token_cache()
                logging.info(f"Token cache refreshed at {datetime.now()} IST")
            except Exception as e:
                logging.error(f"Error in scheduled cache refresh loop: {e}")

    thread = threading.Thread(target=refresh_loop, daemon=True)
    thread.start()


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

class DhanApiManager:
    def __init__(self, rate_limit_interval=.04):
        self.last_api_call_time = 0
        self.rate_limit_interval = rate_limit_interval  
        
    def _wait_for_rate_limit(self):
        current_time = time.time()
        time_since_last_call = current_time - self.last_api_call_time
        if time_since_last_call < self.rate_limit_interval:
            wait_time = self.rate_limit_interval - time_since_last_call
            logging.info(f"API Rate limit, waiting for {wait_time:.2f}s...")
            time.sleep(wait_time)
        self.last_api_call_time = time.time()

    def make_request(self, method, url, headers, json_data):
        self._wait_for_rate_limit()
        if method.lower() == 'post':
            return requests.post(url, headers=headers, json=json_data)
        elif method.lower() == 'get':
            return requests.get(url, headers=headers)
        return None

api_manager = DhanApiManager(rate_limit_interval=.04) 



def get_setting(key):
    result = supabase.table("settings").select("value").eq("key", key).single().execute()
    return result.data["value"] if result.data else None


def update_setting(key, value):
    existing = supabase.table("settings").select("key").eq("key", key).single().execute().data
    if existing:
        supabase.table("settings").update({"value": value}).eq("key", key).execute()
    else:
        supabase.table("settings").insert({"key": key, "value": value}).execute()

def get_all_settings():
    rows = supabase.table("settings").select("*").execute().data
    for s in rows:
        logging.debug(f"Key: '{s['key']}', Value: '{s['value']}'")    


def round_to_0_05(x):
    return round(x * 20) / 20

def get_best_bid_ask(security_id):
    try:
        url = "https://api.dhan.co/v2/marketfeed/quote"
        client_id = get_client_id_cached()
        access_token = get_access_token_cached()
        logging.info(f"Getting best bid/ask with client_id={client_id}")
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "access-token": access_token,
            "client-id": client_id,   
        }
        body = {"NSE_FNO": [security_id]}
                      
        response = api_manager.make_request('post', url, headers, body)
        resp = response.json()
        
        
        if resp.get("status") != "success" or not resp.get("data"):
            logging.warning(f"Market depth API failed for security {security_id}: {resp}")
            return None, None

        security_data = resp["data"]["NSE_FNO"].get(str(security_id), {})
        depth = security_data.get("depth", {})

        buy_depth = depth.get("buy", [])
        sell_depth = depth.get("sell", [])

        best_bid = None
        best_ask = None

        if buy_depth:
            best_bid = buy_depth[0].get("price", None)

        if sell_depth:
            best_ask = sell_depth[0].get("price", None)

        if best_bid is None or best_ask is None:
            logging.warning(f"Best bid or ask price missing in market depth for {security_id}")
            return None, None

        return best_bid, best_ask

    except Exception as e:
        logging.error(f"Error fetching best bid/ask market depth for {security_id}: {e}")
        return None, None

def place_order(security_id, txn_type, qty=1):
    client_id = get_client_id_cached()
    access_token = get_access_token_cached()
    logging.info(f"Placing order with client_id={client_id} and token={access_token}")

    if not client_id or not access_token:
        logging.error("Client ID or access token missing from cache")
        return None
    
    logging.info(f"Placing {txn_type} order for {security_id} qty {qty}")
    
    best_bid, best_ask = get_best_bid_ask(security_id)
    if not best_bid or not best_ask or best_bid <= 0 or best_ask <= 0:
        logging.error("Invalid bid or ask price, skipping order placement")
        return None
    avg_price = (best_bid + best_ask) / 2
    avg_price_rounded = round_to_0_05(avg_price)
    
    url = "https://api.dhan.co/v2/orders"
    headers = {
        "Content-Type": "application/json",
        "access-token": access_token,
        "client-id": client_id,
    }
    body = {
        "dhanClientId": client_id,
        "correlationId": f"order_{security_id}_{int(time.time())}",
        "transactionType": txn_type.upper(),
        "exchangeSegment": "NSE_FNO",
        "productType": "INTRADAY",
        "orderType": "LIMIT",
        "validity": "DAY",
        "securityId": str(security_id),
        "quantity": qty,
        "disclosedQuantity": 0,
        "price": avg_price_rounded
    }
    try:
        logging.info("Sending order to Dhan API")
        response = api_manager.make_request('post', url, headers, body)
        resp_json = response.json()

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

def webhook_handler():
    token = get_access_token_cached()
    client_id = get_client_id_cached()
    if not token or not client_id:
        logging.error("Webhook missing credentials")
        return {"error": "Invalid credentials"}, 400


def cancel_order(order_id):
    client_id = get_client_id_cached()
    access_token = get_access_token_cached()
    logging.info(f"Cancelling order {order_id} with client_id={client_id}")

    if not client_id or not access_token:
        logging.error("Client ID or access token missing from cache")
        return False
    
    url = f"https://api.dhan.co/v2/orders/{order_id}/cancel"
    headers = {
        "Content-Type": "application/json",
        "access-token": access_token,
        "client-id": client_id_cached,
    }
    try:
        response = api_manager.make_request('post', url, headers, None)
        if response.status_code == 202:
            logging.info(f"Order {order_id} cancelled successfully")
            return True
        else:
            logging.error(f"Failed to cancel order {order_id}, status code: {response.status_code}")
            return False
    except Exception as e:
        logging.error(f"Exception during cancelling order {order_id}: {e}")
        return False

def check_order_status(order_id):
    client_id = get_client_id_cached()
    access_token = get_access_token_cached()
    logging.info(f"Checking status for order {order_id} with client_id={client_id}")

    if not client_id or not access_token:
        logging.error("Client ID or access token missing from cache")
        return "UNKNOWN"
    url = f"https://api.dhan.co/v2/orders/{order_id}"
    headers = {
        "Content-Type": "application/json",
        "access-token": access_token,
        "client-id": client_id,
    }
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            data = response.json()
            status = data.get("orderStatus", "").upper()
            if status in ["TRADED", "PART_TRADED", "CONFIRM"]:
                return "CONFIRM"
            elif status in ["REJECTED", "FAILED", "CANCELLED"]:
                return "FAILED"
            elif status in ["PENDING", "TRANSIT"]:
                return "TRANSIT"
            else:
                return "UNKNOWN"
        else:
            logging.error(f"API error {response.status_code} for order {order_id}")
            return "UNKNOWN"
    except Exception as e:
        logging.error(f"Exception checking order status: {e}")
        return "UNKNOWN"

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
        "option_type": data.get("option_type"),
        "strike": int(float(data.get("strike") or 0)),
        "quantity": int(data.get("quantity") or 0),
        "lot_size": int(data.get("lot_size") or 0),
        "trade_type": data.get("trade_type"),
        "order_status": data.get("order_status"),
        "entry_time": entry_time.strftime("%H:%M:%S"),
        "entry_price": float(data.get("entry_price") or 0),
        "exit_price": float(data.get("exit_price") or 0),
        "exit_time": exit_time.strftime("%H:%M:%S") if exit_time else None,
        "reason": data.get("reason"),
        "pnl": float(data.get("pnl") or 0),
        "capital_used": float(data.get("capital_used") or 0),
        "option_security_id": int(data.get("option_security_id") or 0),
        "order_id": data.get("order_id")
    }

    supabase.table("trade_log").insert(trade_dict).execute()

def save_or_update_trade_data(data):
    trade_id = data.get("id")
    if trade_id is None:
        insert_data = {
            "timestamp": data.get("timestamp"),
            "symbol": data.get("symbol"),
            "option_type": data.get("option_type"),
            "strike": int(float(data.get("strike") or 0)),
            "quantity": int(data.get("quantity") or 0),
            "lot_size": int(data.get("lot_size") or 0),
            "trade_type": data.get("trade_type"),
            "order_status": data.get("order_status"),
            "entry_time": data.get("entry_time"),
            "entry_price": float(data.get("entry_price") or 0),
            "exit_price": float(data.get("exit_price") or 0),
            "exit_time": data.get("exit_time"),
            "reason": data.get("reason"),
            "pnl": float(data.get("pnl") or 0),
            "capital_used": float(data.get("capital_used") or 0),
            "option_security_id": int(data.get("option_security_id") or 0),
            "order_id": data.get("order_id"),
        }
        response = supabase.table("trade_log").insert(insert_data).execute()
        logging.debug(f"Insert response: {response}")
        logging.debug(f"Insert response data: {getattr(response, 'data', None)}")
        
        if getattr(response, 'status_code', 200) >= 400:
            logging.error(f"Insert failed with status_code: {response.status_code}")
            return None
        
        if not getattr(response, 'data', None):
            logging.error("No data returned after insert")
            return None
        
        inserted_record = response.data[0]
        return inserted_record.get("id") if inserted_record else None
    else:
        update_data = {
            "timestamp": data.get("timestamp"),
            "symbol": data.get("symbol"),
            "option_type": data.get("option_type"),
            "strike": int(float(data.get("strike") or 0)),
            "quantity": int(data.get("quantity") or 0),
            "lot_size": int(data.get("lot_size") or 0),
            "trade_type": data.get("trade_type"),
            "order_status": data.get("order_status"),
            "entry_time": data.get("entry_time"),
            "entry_price": float(data.get("entry_price") or 0),
            "exit_price": float(data.get("exit_price") or 0),
            "exit_time": data.get("exit_time"),
            "reason": data.get("reason"),
            "pnl": float(data.get("pnl") or 0),
            "capital_used": float(data.get("capital_used") or 0),
            "option_security_id": int(data.get("option_security_id") or 0),
            "order_id": data.get("order_id"),
        }
        response = supabase.table("trade_log").update(update_data).eq("id", trade_id).execute()
        logging.debug(f"Update response: {response}")
        
        if getattr(response, 'status_code', 200) >= 400:
            logging.error(f"Update failed with status_code: {response.status_code}")
            return None
        return trade_id


def save_trade_data_async(data):
    def task():
        try:
            supabase.table("trade_log").insert(data).execute()
        except Exception as e:
            logging.error(f"Async save_trade_data failed: {e}")
    threading.Thread(target=task, daemon=True).start()


def find_open_position(symbol, today=None):
    if today is None:
        today = date.today().strftime("%Y-%m-%d")
    
    try:
        resp = supabase.table("trade_log").select(
            "id,symbol,option_security_id,entry_price,order_status,quantity,lot_size"
        ).eq("symbol", symbol.upper())\
         .eq("order_status", "open")\
         .in_("trade_type", ["live", "paper"])\
         .eq("timestamp", today)\
         .maybe_single().execute()
    except Exception as e:
        print(f"Supabase query failed in find_open_position: {e}")
        return None
    
    if resp is None:
        print("Supabase response is None")
        return None
    
    # Check .data attribute safely
    trade = getattr(resp, 'data', None)
    if not trade:
        return None
    
    return {
        'id': trade.get('id'),
        'symbol': trade.get('symbol'),
        'option_security_id': trade.get('option_security_id'),
        'entry_price': trade.get('entry_price'),
        'order_status': trade.get('order_status'),
        'quantity': trade.get('quantity'),
        'lot_size': trade.get('lot_size')
    }

def get_equity_ltp(symbol):
    symbol = symbol.upper()
    security_id = equity_symbol_to_id_map.get(symbol)
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



























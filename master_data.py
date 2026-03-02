import os
import time
import logging
import requests
import pandas as pd
from datetime import datetime
from functools import lru_cache
from time_utils import get_current_ist_time, format_ist_time, convert_utc_to_ist


MASTER_CSV_FILE = "/tmp/api-scrip-master-detailed.csv"
df_master = None
index_symbol_to_id = {}
option_symbol_to_id = {}
option_cache = {}
cache_find_option_security = {}
preprocessed_option_map = {}
atm_option_security_cache = {}
loaded_expiry_date = None


# Define your trading stocks list here (39 stocks)
trading_symbols = set([
    "NIFTY", "SENSEX", "FINNIFTY",
])

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')


def fetch_live_master_data_direct(url='https://images.dhan.co/api-data/api-scrip-master-detailed.csv'):
    # Check if file exists and was updated today (using IST to be safe)
    if os.path.exists(MASTER_CSV_FILE):
        last_modified = os.path.getmtime(MASTER_CSV_FILE)
        # UTC to IST time conversion for file modification check
        last_date = datetime.fromtimestamp(last_modified).date()
        ist_today = get_current_ist_time().date()
        
        if last_date == ist_today:
            logging.debug("Master data already updated today in /tmp.")
            return

    logger.info("Downloading master data to /tmp...")
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36',
        'Accept': 'text/csv,application/csv'
    }
    
    
    try:
        r = requests.get(url, headers=headers, timeout=15) # Added timeout
        if r.status_code == 200:
            with open(MASTER_CSV_FILE, "wb") as f:
                f.write(r.content)
            logger.info("Master data successfully downloaded and saved to /tmp.")
        else:
            logger.error(f"Failed to download master data: {r.status_code} - {r.text[:100]}")
    except Exception as e:
         logger.error(f"Exception during master data download: {e}")

def get_expiry_date(symbol="NIFTY"):
    from helper import global_settings_cache
    from datetime import datetime
    import logging
    
    # 1. Symbol ko clean karo (sirf upper case aur extra spaces hatao)
    symbol_name = str(symbol).strip().upper()
    
    # 2. Key banao (Underscore rehne do kyunki DB mein hai)
    expiry_key = f"{symbol_name}_expiry"
    
    # 3. Cache se value uthao
    expiry_str = global_settings_cache.get(expiry_key)
    
    # 4. Agar SENSEX_expiry nahi mili, toh NIFTY_expiry ya generic try karo
    if not expiry_str:
        expiry_str = global_settings_cache.get("NIFTY_expiry") or global_settings_cache.get("expiry_date")
    
    if expiry_str:
        try:
            # String (2026-02-09) se date object banayein
            return datetime.strptime(str(expiry_str).strip(), "%Y-%m-%d").date()
        except Exception as e:
            logging.error(f"Expiry parsing error for {symbol_name}: {e}")
            return None
    
    logging.warning(f"No expiry found in cache for key: {expiry_key}")
    return None
def load_master_data(force_reload=False):
    global df_master, index_symbol_to_id, preprocessed_option_map
    
    try:
        logger.info("Starting to fetch latest master data CSV...")
        fetch_live_master_data_direct()
        df_master = pd.read_csv(MASTER_CSV_FILE, low_memory=False)
        
        # --- IDs Setup ---
        index_symbol_to_id.clear()
        index_symbol_to_id["NIFTY"] = "13"
        index_symbol_to_id["FINNIFTY"] = "27"  # Correct ID
        index_symbol_to_id["SENSEX"] = "51" 
        
        # Date column format fix
        expiry_col = 'SM_EXPIRY_DATE' if 'SM_EXPIRY_DATE' in df_master.columns else 'EXPIRY_DATE'
        df_master[expiry_col] = pd.to_datetime(df_master[expiry_col], errors='coerce').dt.strftime('%Y-%m-%d')
        
        # Aaj ki date (Purane contracts filter karne ke liye)
        today_str = datetime.now().strftime('%Y-%m-%d')

                
        valid_search_ids = ["13", "27", "51"] # Nifty, FinNifty, Sensex IDs
        valid_search_names = ["NIFTY", "FINNIFTY", "SENSEX", "BSESEX", "BSXOPT", "NIFTY FIN SERVICE"]

        option_df = df_master[
            (df_master['EXCH_ID'].isin(['NSE', 'BSE'])) & 
            (df_master['SEGMENT'] == 'D') & 
            (df_master['INSTRUMENT'] == 'OPTIDX') & 
            # Ya toh ID match kare, ya Naam match kare
            (
                df_master['SECURITY_ID'].astype(str).isin(valid_search_ids) | 
                df_master['UNDERLYING_SYMBOL'].str.strip().str.upper().isin(valid_search_names)
            ) &
            # Sirf purani expiry hatayenge, aane wali saari rakhenge
            (df_master[expiry_col] >= today_str) 
        ].copy()

        found_symbols = option_df['UNDERLYING_SYMBOL'].unique()
        logger.info(f"Unique symbols found in CSV (All Future Expiries): {found_symbols}")

        preprocess_option_map(option_df)
        
        logger.info(f"Master Data loaded successfully. Total Contracts: {len(option_df)}")
        
    except Exception as e:
        logger.error(f"Critical error loading CSV: {e}")
def preprocess_option_map(option_df):
    global preprocessed_option_map
    preprocessed_option_map.clear()
    
    expiry_col = 'SM_EXPIRY_DATE' if 'SM_EXPIRY_DATE' in option_df.columns else 'EXPIRY_DATE'
    option_df['FORMATTED_EXPIRY'] = pd.to_datetime(option_df[expiry_col], errors='coerce').dt.strftime('%Y-%m-%d')

    for _, row in option_df.iterrows():
        if pd.isna(row['FORMATTED_EXPIRY']):
            continue

        raw_symbol_name = str(row.get('SYMBOL_NAME', '')).strip().upper()
        raw_underlying = str(row.get('UNDERLYING_SYMBOL', '')).strip().upper()
        
        # --- SMART DETECTION LOGIC ---
        underlying = None
        
        # Sensex Check
        if "BSXOPT" in raw_symbol_name or raw_underlying in ["SENSEX", "BSE SENSEX", "1", "51", "BSESEX"]:
            underlying = "SENSEX"
        
        # Nifty Check (Ensure BANKNIFTY doesn't get mixed)
        elif ("NIFTY" in raw_symbol_name or raw_underlying in ["NIFTY", "NIFTY 50", "13"]) and "FIN" not in raw_symbol_name and "BANK" not in raw_symbol_name:
            underlying = "NIFTY"
            
        # FINNIFTY Check (Expanded names)
        elif "FINNIFTY" in raw_symbol_name or "FIN" in raw_symbol_name or raw_underlying in ["FINNIFTY", "NIFTY FIN SERVICE", "27"]:
            underlying = "FINNIFTY"
            
        if not underlying:
            continue # Agar kaam ka symbol nahi hai toh skip karo

        key = (
            underlying,
            row['FORMATTED_EXPIRY'],
            str(row['OPTION_TYPE']).strip().upper()
        )
        
        entry = {
            'strike': float(row['STRIKE_PRICE']),
            'SECURITY_ID': int(row['SECURITY_ID']),
            'SYMBOL_NAME': raw_symbol_name,
            'LOT_SIZE': int(row.get('LOT_SIZE', 20 if underlying == "SENSEX" else 0)) 
        }
        preprocessed_option_map.setdefault(key, []).append(entry)

    # Sort strikes
    for key in preprocessed_option_map:
        preprocessed_option_map[key].sort(key=lambda x: x['strike'])

@lru_cache(maxsize=1024)
def cached_find_option_security(symbol, expiry_str, ltp, option_type, strike_selection):
    pass

def find_option_security_id_fast(symbol, expiry, ltp, option_type, strike_selection):
    key_cache = (symbol.upper(), expiry.strftime("%Y-%m-%d"), ltp, option_type.upper(), strike_selection)
    if key_cache in cache_find_option_security:
        return cache_find_option_security[key_cache]

    expiry_str = expiry.strftime("%Y-%m-%d")
    key = (symbol.upper(), expiry_str, option_type.upper())
    options_list = preprocessed_option_map.get(key)
    if not options_list:
        logger.warning(f"No option contracts found for {symbol} with expiry {expiry_str} type {option_type}")
        return None, None, None

    strikes = [opt['strike'] for opt in options_list]
    diffs = [abs(s - ltp) for s in strikes]

    min_idx = diffs.index(min(diffs))
    target_idx = min_idx + strike_selection
    if target_idx < 0 or target_idx >= len(strikes):
        selected_idx = min_idx
    else:
        selected_idx = target_idx

    selected_opt = options_list[selected_idx]
    logger.info(f"Found Security ID: {selected_opt['SECURITY_ID']} for option symbol: {selected_opt['SYMBOL_NAME']}")

    result = (selected_opt['SECURITY_ID'], selected_opt['strike'], selected_opt['SYMBOL_NAME'])
    cache_find_option_security[key_cache] = result 
    return result

def check_and_reload_expiry():
    global loaded_expiry_date
    current_expiry = get_expiry_date()
    if current_expiry != loaded_expiry_date:
        logging.info(f"Expiry date changed from {loaded_expiry_date} to {current_expiry}. Reloading master data.")
        load_master_data(force_reload=True)
        loaded_expiry_date = current_expiry
    else:
        logging.info(f"Expiry date unchanged: {current_expiry}. No reload needed.")
   
    
def another_function():
    from helper import get_all_settings 
    settings = get_all_settings()
   
def get_atm_strike(ltp, step=50):
    return int(round(ltp / step) * step)
    atm_option_security_cache = {}

def get_symbol_from_security_id(security_id):
    global option_cache, index_symbol_to_id, df_master
    
    try:
        sec_id_int = int(security_id)
        sec_id_str = str(security_id).strip()
    except:
        return None

    for key, val in option_cache.items():
        if val.get('SECURITY_ID') == sec_id_int:
            return key[0]  
    
    for symbol, sid in index_symbol_to_id.items():
        if str(sid) == sec_id_str:
            return symbol
    
    if df_master is not None and not df_master.empty:
        try:
            row = df_master[df_master['SECURITY_ID'] == sec_id_int]
            
            if not row.empty:
                found_symbol = str(row.iloc[0].get('UNDERLYING_SYMBOL', '')).strip().upper()
                
                if found_symbol == "NIFTY 50" or found_symbol == "NIFTY": return "NIFTY"
                if found_symbol == "NIFTY BANK" or found_symbol == "BANKNIFTY": return "BANKNIFTY"
                if found_symbol == "BSE SENSEX" or found_symbol == "SENSEX": return "SENSEX"
                if found_symbol == "FINNIFTY" or found_symbol == "NIFTY FIN SERVICE": return "FINNIFTY"
                                
                return found_symbol
                
        except Exception as e:
            logging.error(f"Error searching symbol in CSV for {security_id}: {e}")

    logging.error(f"CRITICAL: Symbol NOT found anywhere for ID: {security_id}")
    return None

def get_lot_size(symbol, expiry, strike, option_type):
    
    global preprocessed_option_map
    import time
    
    symbol_upper = str(symbol).strip().upper()
    
    if "SENSEX" in symbol_upper:
        return 20  
        
    elif "NIFTY" in symbol_upper and "BANK" not in symbol_upper:
        return 65
    elif "FINNIFTY" in symbol_upper and "BANK" not in symbol_upper:
        return 60
        
    try:
        expiry_str = str(expiry)
        option_type_upper = str(option_type).strip().upper()
        
        
        key = (symbol_upper, expiry_str, option_type_upper)
        
        contracts = preprocessed_option_map.get(key)
        
        if contracts and len(contracts) > 0:
            found_lot = contracts[0].get('LOT_SIZE', 0)
            if found_lot and int(found_lot) > 0:
                return int(found_lot)

    except Exception as e:
        logger.error(f"Error fetching lot size: {e}")
        
    return 1 


def get_lot_size_for_security(security_id):
    global preprocessed_option_map
    for key, contracts in preprocessed_option_map.items():
        for contract in contracts:
            if contract['SECURITY_ID'] == security_id:
                symbol, expiry_str, option_type = key
                from datetime import datetime
                expiry = datetime.strptime(expiry_str, "%Y-%m-%d").date()
                
                return get_lot_size(symbol, expiry, contract['strike'], option_type)
    
    
    return 1

def get_strike_for_security(security_id):
    for key in option_cache.keys():
        if option_cache[key]['SECURITY_ID'] == security_id:
            return key[3]  
    return None

def get_option_type_for_security(security_id):
    for key in option_cache.keys():
        if option_cache[key]['SECURITY_ID'] == security_id:
            return key[2]  
    return None
    
def get_instrument_id(symbol):
    clean_symbol = symbol.strip().upper()

    if clean_symbol == "NIFTY 50":
        clean_symbol = "NIFTY"
    elif clean_symbol == "BSE SENSEX":
        clean_symbol = "SENSEX"
    elif clean_symbol == "NIFTY BANK":
        clean_symbol = "BANKNIFTY"
    
    sec_id = index_symbol_to_id.get(clean_symbol)
    
    if sec_id:
        return int(sec_id)
    else:
        logging.error(f"Instrument ID not found for symbol: {symbol}")
        return None
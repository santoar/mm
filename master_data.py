import os
import time
import logging
import requests
import pandas as pd
from datetime import datetime
from functools import lru_cache
from time_utils import get_current_ist_time, format_ist_time, convert_utc_to_ist


MASTER_CSV_FILE = "api-scrip-master-detailed.csv"
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
    "NIFTY",
])

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')


def fetch_live_master_data_direct(url='https://images.dhan.co/api-data/api-scrip-master-detailed.csv'):
    if os.path.exists(MASTER_CSV_FILE):
        last_modified = os.path.getmtime(MASTER_CSV_FILE)
        last_date = datetime.fromtimestamp(last_modified).date()
        if last_date == datetime.today().date():
            logging.debug("Master data updated today.")
            return
    logger.info("Downloading master data...")
    r = requests.get(url)
    if r.status_code == 200:
        with open(MASTER_CSV_FILE, "wb") as f:
            f.write(r.content)
        logger.info("Master data downloaded.")
    else:
        logger.warning(f"Failed to download master data: {r.status_code}")

def get_expiry_date():
    from helper import global_settings_cache
    expiry_str = global_settings_cache.get("expiry_date", "").strip()
    print(f"expiry_str value in get_expiry_date: {expiry_str}")
    if expiry_str:
        from datetime import datetime
        try:
            return datetime.strptime(expiry_str, "%Y-%m-%d").date()
        except ValueError:
            try:
                return datetime.strptime(expiry_str, "%m/%d/%Y").date()
            except Exception as e:
                logger.error(f"Expiry date parsing error: {e}")
                return None
    else:
        logging.warning("Expiry date not set in settings.")
        return None

def load_master_data(force_reload=False):
    global df_master, index_symbol_to_id, option_symbol_to_id, option_cache, loaded_expiry_date
    
    expiry_date = get_expiry_date()

    if loaded_expiry_date == expiry_date and not force_reload:
        logger.info(f"Expiry unchanged ({expiry_date}), skipping reload.")
        return

    loaded_expiry_date = expiry_date
    
    index_symbol_to_id.clear()
    option_symbol_to_id.clear()
    option_cache.clear()
    
    logger.info("Starting to fetch latest master data CSV...")
    fetch_live_master_data_direct()
    logger.info(f"Reading master data from file: {MASTER_CSV_FILE}")
    df_master = pd.read_csv(MASTER_CSV_FILE, low_memory=False)
    logger.info(f"Master CSV loaded: {df_master.shape[0]} rows, {df_master.shape[1]} columns")
    
    
    index_symbols = df_master[
        (df_master['EXCH_ID'] == 'NSE') & 
        (df_master['SEGMENT'] == 'I') & 
        (df_master['INSTRUMENT_TYPE'] == 'INDEX') & 
        (df_master['UNDERLYING_SYMBOL'].str.strip().str.upper().isin(trading_symbols))
    ]
    index_symbol_to_id.update(index_symbols.set_index('UNDERLYING_SYMBOL')['SECURITY_ID'].to_dict())
    logger.info(f"Loaded {len(index_symbol_to_id)} index symbols")
    
    option_df = df_master[
        (df_master['EXCH_ID'] == 'NSE') & 
        (df_master['SEGMENT'] == 'D') & 
        (df_master['INSTRUMENT'] == 'OPTIDX') & 
        (df_master['INSTRUMENT_TYPE'] == 'OP') & 
        (df_master['UNDERLYING_SYMBOL'].str.strip().str.upper().isin(trading_symbols))
    ].copy()
    logger.debug(f"option_df shape: {option_df.shape}")
    
    if expiry_date:
        option_df['SM_EXPIRY_DATE'] = pd.to_datetime(option_df['SM_EXPIRY_DATE'], errors='coerce').dt.date
        option_df = option_df[option_df['SM_EXPIRY_DATE'] == expiry_date]
        logger.info(f"Filtered option contracts by expiry {expiry_date}, count: {option_df.shape[0]}")
    else:
        logger.warning("Expiry date not set, loading all option contracts.")
    
    option_symbol_to_id.update(
        {str(k).strip().upper(): int(v) for k, v in option_df.set_index('SYMBOL_NAME')['SECURITY_ID'].to_dict().items()}
    )
    
    for _, row in option_df.iterrows():
        expiry_str = pd.to_datetime(row['SM_EXPIRY_DATE'], errors='coerce').strftime('%Y-%m-%d')
        key = (
            row['UNDERLYING_SYMBOL'].strip().upper(),
            expiry_str,
            row['OPTION_TYPE'].strip().upper(),
            float(row['STRIKE_PRICE'])
        )
        option_cache[key] = {
            'SECURITY_ID': int(row['SECURITY_ID']),
            'SYMBOL_NAME': row['SYMBOL_NAME'].strip().upper(),
            'LOT_SIZE': int(row.get('LOT_SIZE', 1))
        }
    preprocess_option_map(option_df)
    
    logging.info(f"Loaded {len(index_symbol_to_id)} index symbols")
    logger.info(f"Loaded {len(option_symbol_to_id)} option contracts")

def preprocess_option_map(option_df):
    
    global preprocessed_option_map
    preprocessed_option_map.clear()
    option_df['SM_EXPIRY_DATE_FORMATTED'] = pd.to_datetime(option_df['SM_EXPIRY_DATE'], errors='coerce').dt.strftime('%Y-%m-%d')

    for _, row in option_df.iterrows():
        key = (
            row['UNDERLYING_SYMBOL'].strip().upper(),
            row['SM_EXPIRY_DATE_FORMATTED'],
            row['OPTION_TYPE'].strip().upper()
        )
        entry = {
            'strike': float(row['STRIKE_PRICE']),
            'SECURITY_ID': int(row['SECURITY_ID']),
            'SYMBOL_NAME': row['SYMBOL_NAME'].strip().upper(),
            'LOT_SIZE': int(row.get('LOT_SIZE', 1))
        }
        preprocessed_option_map.setdefault(key, []).append(entry)

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
    for key, val in option_cache.items():
        if val['SECURITY_ID'] == security_id:
            return key[0]  
    for symbol, sec_id in index_symbol_to_id.items():
        if sec_id == security_id:
            return symbol
    return None

def get_lot_size(symbol, expiry, strike, option_type):
    start = time.time()
    try:
        expiry_str = expiry.strftime("%Y-%m-%d")
        key = (symbol.upper(), expiry_str, option_type.upper(), float(strike))
        contract = option_cache.get(key)

        if contract:
            lot_size = contract.get('LOT_SIZE', 1)
        else:
            logger.warning(f"Lot size not found in option cache for {key}, defaulting to 1")
            lot_size = 1
    except Exception as e:
        logging.error(f"Error fetching lot size for {symbol}: {e}, defaulting to 1.")
        lot_size = 1
    end = time.time()
    logger.info(f"get_lot_size executed in {end - start:.4f}s for {symbol}")
    return lot_size


def get_lot_size_for_security(security_id):
    for key, val in option_cache.items():
        if val['SECURITY_ID'] == security_id:
            symbol, expiry_str, option_type, strike = key
            from datetime import datetime
            expiry = datetime.strptime(expiry_str, "%Y-%m-%d").date()
            return get_lot_size(symbol, expiry, strike, option_type)
    return 1

def get_strike_for_security(security_id):
    for key in option_cache.keys():
        if option_cache[key]['SECURITY_ID'] == security_id:
            return key[3]  # strike is fourth element of key tuple
    return None

def get_option_type_for_security(security_id):
    for key in option_cache.keys():
        if option_cache[key]['SECURITY_ID'] == security_id:
            return key[2]  # option type is third element of key tuple
    return None
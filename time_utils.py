from datetime import datetime
import pytz

IST_TIMEZONE = pytz.timezone('Asia/Kolkata')

def get_current_ist_time():
    return datetime.now(IST_TIMEZONE)

def format_ist_time(dt=None, fmt='%Y-%m-%d %H:%M:%S'):
    if dt is None:
        dt = get_current_ist_time()
    return dt.strftime(fmt)

def convert_utc_to_ist(utc_dt):
    utc = pytz.utc
    utc_dt = utc.localize(utc_dt)
    ist_dt = utc_dt.astimezone(IST_TIMEZONE)
    return ist_dt

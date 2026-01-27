from supabase import create_client
import os

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# सभी closed trades जिनका capital_used 0 है निकालो
trades_to_fix = supabase.table("trade_log").select("*").eq("order_status", "closed").eq("capital_used", 0).execute().data

for trade in trades_to_fix:
    trade_id = trade['id']
    # entry के वक्त का capital_used निकालो (supabase या आपके पास local CSV/log से भी)
    # यहां मान लेते हैं आपको उसमें entry_price, lot_size, quantity मिल सकता है
    if trade.get('entry_price') and trade.get('lot_size') and trade.get('quantity'):
        fixed_capital = float(trade['entry_price']) * int(trade['lot_size']) * int(trade['quantity'])
        supabase.table("trade_log").update({"capital_used": fixed_capital}).eq("id", trade_id).execute()
        print(f"Fixed trade {trade_id}: set capital_used to {fixed_capital}")

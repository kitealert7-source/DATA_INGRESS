
import os
import time
import requests
import pandas as pd
from datetime import datetime
import json

# Setup Paths matching raw_update_sop17
import sys
sys.path.append(os.getcwd())
try:
    from scripts.utils.path_config import GET_DATA_ROOT
except ImportError:
    # Handle direct execution from scripts/utils
    sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
    from scripts.utils.path_config import GET_DATA_ROOT

BASE_DIR = os.path.join(GET_DATA_ROOT(), "MASTER_DATA")
BTC_DIR = os.path.join(BASE_DIR, "BTC_DELTA_MASTER", "RAW")
ETH_DIR = os.path.join(BASE_DIR, "ETH_DELTA_MASTER", "RAW")

ASSETS = [
    {"asset": "BTC", "symbol": "BTCUSD", "dir": BTC_DIR},
    {"asset": "ETH", "symbol": "ETHUSD", "dir": ETH_DIR}
]

def get_last_timestamp(filepath):
    if not os.path.exists(filepath):
        return None
    try:
        with open(filepath, 'rb') as f:
            try:
                f.seek(-1024, os.SEEK_END)
            except OSError:
                f.seek(0)
            last_lines = f.readlines()
        if not last_lines: return None
        last_line = last_lines[-1].decode('utf-8').strip()
        if not last_line: return None
        ts = last_line.split(',')[0]
        if "time" in ts: return None
        return pd.to_datetime(ts)
    except:
        return None

def diag_delta():
    print("--- DELTA PIPELINE DIAGNOSTIC (DRY RUN) ---")
    
    for item in ASSETS:
        asset = item["asset"]
        symbol = item["symbol"]
        target_dir = item["dir"]
        
        print(f"\n[ASSET: {asset} ({symbol})]")
        
        for tf in ["3m", "5m"]:
            print(f"  [Timeframe: {tf}]")
            
            # 1. Last Timestamp
            current_year = datetime.now().year
            filename = f"{asset}_{tf}_{current_year}_DELTA_RAW.csv"
            filepath = os.path.join(target_dir, filename)
            
            last_ts = get_last_timestamp(filepath)
            print(f"    Raw File: {filename}")
            
            if last_ts:
                print(f"    Last TS:  {last_ts}")
                start_time_epoch = int(last_ts.timestamp())
            else:
                print(f"    Last TS:  None (Defaults to 1 year ago)")
                start_time_epoch = int(time.time()) - (365 * 24 * 3600)

            # 2. API Params
            now_epoch = int(time.time())
            params = {
                "symbol": symbol,
                "resolution": tf,
                "start": start_time_epoch,
                "end": now_epoch
            }
            url = "https://api.delta.exchange/v2/history/candles"
            
            print(f"    API URL: {url}")
            print(f"    Params:  {params}")
            
            # 3. Dry Run Fetch
            try:
                print("    --> EXEC DRY RUN FETCH...")
                resp = requests.get(url, params=params)
                
                if resp.status_code != 200:
                    print(f"    [ERROR] Status {resp.status_code}")
                    continue
                    
                result = resp.json().get('result', [])
                if not result:
                    print(f"    [DRY RUN] Returned: 0 rows (Empty result)")
                    continue
                
                # Parse for analysis
                data = []
                for c in result:
                    t = c.get('time') or c.get('t')
                    data.append({"time": t})
                
                df = pd.DataFrame(data)
                df['time'] = pd.to_datetime(df['time'], unit='s')
                df = df.sort_values('time')
                
                # Simulate Pipeline Filter (> start_time)
                # Pipeline: new_items = [x for x in batch_data if x['time'] > curr_start]
                # Note: 'curr_start' in pipeline loop matches request start.
                
                # Convert comparison strictness
                # start_time_epoch is integer seconds.
                # df['time'] is datetime.
                
                start_dt = pd.to_datetime(start_time_epoch, unit='s')
                df_filtered = df[df['time'] > start_dt]
                
                count_total = len(df)
                count_new = len(df_filtered)
                
                print(f"    [DRY RUN] Total Returned: {count_total}")
                print(f"    [DRY RUN] New (Strict > Last): {count_new}")
                
                if count_new > 0:
                    first = df_filtered['time'].min()
                    last = df_filtered['time'].max()
                    print(f"    [DRY RUN] Batch First: {first}")
                    print(f"    [DRY RUN] Batch Last:  {last}")
                    
                    # Logic Check
                    if first <= start_dt:
                        print("    [WARNING] Filter logic failure? Batch starts before Last TS!")
                    else:
                        print("    [PASS] Batch starts strictly after Last TS.")
                else:
                    if count_total > 0:
                        print("    [INFO] Data returned but all overlaps (<= Last TS). Pipeline will discard correctly.")
                    else:
                        print("    [INFO] No data.")

            except Exception as e:
                print(f"    [EXCEPTION] {e}")

if __name__ == "__main__":
    diag_delta()

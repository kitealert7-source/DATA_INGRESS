
import os
import pandas as pd
import MetaTrader5 as mt5
from datetime import datetime
import process_octafx

# Use the config from the imported module
ASSETS = process_octafx.ASSETS
BASE_DIR = process_octafx.BASE_DIR

def run_dry_run():
    print("--- OCTAFX PIPELINE DIAGNOSTIC (DRY RUN) ---")
    if not mt5.initialize():
        print(f"MT5 Init Failed: {mt5.last_error()}")
        return

    targets = ["BTCUSD", "ETHUSD", "XAUUSD"]
    
    for symbol in targets:
        print(f"\n[SYMBOL: {symbol}]")
        
        # 1. Included?
        if symbol not in ASSETS:
            print(f"  Included in pipeline? NO")
            continue
        print(f"  Included in pipeline? YES")
        
        cfg = ASSETS[symbol]
        
        for tf_name, tf_val in cfg["tfs"].items():
            print(f"  [Timeframe: {tf_name}]")
            
            # 2. Last Timestamp
            # Find year file logic similar to pipeline
            current_year = datetime.now().year
            # In diag we guess the file exists
            # We must verify if the user has split by year correctly
            # Using process_octafx logic:
            raw_filename = f"{symbol}_{tf_name}_{current_year}_MT5_RAW.csv"
            raw_path = os.path.join(cfg["master_dir"], "RAW", raw_filename)
            
            last_ts = process_octafx.get_last_timestamp(raw_path)
            print(f"    Raw File: {raw_filename}")
            if not last_ts:
                print(f"    Last TS:  None (File not found or empty)")
                from_date = datetime(2024,1,1) # Default
            else:
                print(f"    Last TS:  {last_ts}")
                from_date = last_ts

            # 3. Planned Fetch
            now_buffer = datetime.now() + pd.Timedelta(days=1)
            print(f"    Fetch Fn: copy_rates_range")
            print(f"    Date From: {from_date}")
            print(f"    Date To:   {now_buffer}")
            
            # 4. Dry Run (XAUUSD ONLY)
            if symbol == "XAUUSD":
                print("    --> EXEC DRY RUN FETCH...")
                rates = mt5.copy_rates_range(symbol, tf_val, from_date, now_buffer)
                
                if rates is None or len(rates) == 0:
                    print("    [DRY RUN] Returned: 0 rows")
                else:
                    df = pd.DataFrame(rates)
                    df['time'] = pd.to_datetime(df['time'], unit='s')
                    
                    # Filter strict > from_date (like pipeline)
                    # Pipeline logic: no explicit > filter if copy_rates_range handles start?
                    # valid copy_rates_range is inclusive of 'from'?
                    # usually it includes 'from' bar. Pipeline usually de-dupes later or we should filter.
                    # process_octafx currently: 
                    # rates = mt5.copy_rates_range(...)
                    # df_temp = df_temp[df_temp['time'] > from_date] matches strict append rule.
                    
                    df_filtered = df[df['time'] > from_date]
                    
                    if df_filtered.empty:
                         print("    [DRY RUN] Returned rows (post-filter): 0")
                    else:
                        first_ts = df_filtered['time'].min()
                        last_ts_batch = df_filtered['time'].max()
                        print(f"    [DRY RUN] Returned rows (post-filter): {len(df_filtered)}")
                        print(f"    [DRY RUN] Batch First: {first_ts}")
                        print(f"    [DRY RUN] Batch Last:  {last_ts_batch}")
                        
                        # Dedup check
                        if len(df_filtered) != len(df_filtered.drop_duplicates(subset='time')):
                            print("    [DRY RUN] WARNING: Duplicates detected within batch!")
                        else:
                            print("    [DRY RUN] Clean batch (no internal dupes).")

    mt5.shutdown()

if __name__ == "__main__":
    run_dry_run()

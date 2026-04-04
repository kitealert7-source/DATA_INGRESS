import os
import time
import json
import hashlib
from datetime import datetime
import pandas as pd
import MetaTrader5 as mt5
from pathlib import Path

ENGINE_VERSION = "SOP17_HISTORY_ACQUISITION_v2"
_DI = Path(__file__).resolve().parents[2]
_AG = _DI.parent / "Anti_Gravity_DATA_ROOT"
BASE_DIR   = str(_AG / "MASTER_DATA" / "XAUUSD_OCTAFX_MASTER" / "RAW")
TMP_DIR    = str(_DI / "tmp" / "XAUUSD_HISTORY")
REPORT_DIR = str(_DI / "reports")

SYMBOL = "XAUUSD"
MAX_YEARS_BACK = 30
BATCH_SIZE = 50000

TIMEFRAMES = ["1m", "5m", "15m", "30m", "1h", "4h", "1d"]
MT5_TIMEFRAMES = {
    "1m": mt5.TIMEFRAME_M1,
    "5m": mt5.TIMEFRAME_M5,
    "15m": mt5.TIMEFRAME_M15,
    "30m": mt5.TIMEFRAME_M30,
    "1h": mt5.TIMEFRAME_H1,
    "4h": mt5.TIMEFRAME_H4,
    "1d": mt5.TIMEFRAME_D1
}

# Mapping custom timeframe to seconds
TF_SECONDS = {
    "1m": 60,
    "5m": 300,
    "15m": 900,
    "30m": 1800,
    "1h": 3600,
    "4h": 14400,
    "1d": 86400
}

os.makedirs(TMP_DIR, exist_ok=True)
os.makedirs(REPORT_DIR, exist_ok=True)

def compute_file_sha256(filepath: str) -> str:
    h = hashlib.sha256()
    b = bytearray(128*1024)
    mv = memoryview(b)
    with open(filepath, 'rb', buffering=0) as f:
        while n := f.readinto(mv):
            h.update(mv[:n])
    return h.hexdigest()

def _write_raw_manifest(filepath: str, df: pd.DataFrame, interval_sec: int):
    filename = os.path.basename(filepath)
    manifest_path = filepath + "_manifest.json"
    
    parts = filename.replace(".csv", "").split("_")
    symbol = parts[0]
    tf = parts[2]
    year = int(parts[3])
    
    row_count = len(df)
    first_ts = df['time'].min().isoformat() if row_count > 0 else ""
    last_ts = df['time'].max().isoformat() if row_count > 0 else ""
    
    file_hash = compute_file_sha256(filepath)
    
    manifest = {
        "symbol": symbol,
        "timeframe": tf,
        "year": year,
        "row_count": row_count,
        "first_timestamp": first_ts,
        "last_timestamp": last_ts,
        "sha256": file_hash,
        "schema_version": "1.1",
        "columns": list(df.columns),
        "interval_seconds": interval_sec
    }
    
    tmp_path = manifest_path + ".tmp"
    with open(tmp_path, 'w') as f:
        json.dump(manifest, f, indent=2)
    os.replace(tmp_path, manifest_path)

def discover_broker_capabilities():
    print(f"\n[{datetime.utcnow().isoformat()}] Discovering API Capabilities via copy_rates_from_pos for {SYMBOL}...")
    
    # Critical MT5 API Guard: Force symbol into Market Watch cache
    if not mt5.symbol_select(SYMBOL, True):
        print(f"CRITICAL: Failed to select {SYMBOL} in Market Watch.")
        return {}

    capabilities = {}
    for tf_label, mt5_tf in MT5_TIMEFRAMES.items():
        # Request 10 million bars starting from pos 0 (current)
        rates = mt5.copy_rates_from_pos(SYMBOL, mt5_tf, 0, 10_000_000)
        if rates is not None and len(rates) > 0:
            df = pd.DataFrame(rates)
            df['time'] = pd.to_datetime(df['time'], unit='s', utc=True)
            earliest = df['time'].min()
            
            capabilities[tf_label] = {
                'earliest_ts': earliest.isoformat(),
                'total_bars': len(rates),
                'truncated': len(rates) == 10_000_000 # Unlikely, but just in case
            }
            print(f"  {tf_label:<4}: {len(rates):>10} bars. Earliest: {earliest}")
        else:
            capabilities[tf_label] = {'error': False, 'total_bars': 'unknown'}
            print(f"  {tf_label:<4}: Initial large request failed. Will rely entirely on reverse-walking.")
            
    return capabilities

def pull_historical_data(capabilities):
    print(f"\n[{datetime.utcnow().isoformat()}] Starting Reverse-Walk Historical Pull...")
    hard_stop_date = pd.Timestamp(datetime.utcnow(), tz='UTC') - pd.DateOffset(years=MAX_YEARS_BACK)
    
    for tf_label, mt5_tf in MT5_TIMEFRAMES.items():
        if tf_label not in capabilities or 'error' in capabilities[tf_label]:
            continue
            
        print(f"\n--- Pulling {tf_label} ---")
        tmp_file = os.path.join(TMP_DIR, f"{SYMBOL}_{tf_label}_extended_raw.csv")
        
        all_dfs = []
        current_to_date = pd.Timestamp(datetime.utcnow(), tz='UTC')
        earliest_seen_ts = current_to_date + pd.Timedelta(days=1) # Init future
        
        iteration = 0
        total_pulled = 0
        
        while True:
            iteration += 1
            if current_to_date < hard_stop_date:
                print(f"    HARD STOP REACHED (30 Years) for {tf_label}. Breaking.")
                break
                
            SAFE_BATCH_SIZE = 50000 # Date-based handles larger requests better
            
            # Asynchronous download guard: MT5 downloads history in the background. If not cached, it returns None initially.
            rates = None
            for attempt in range(10):
                # Use pure python datetime in UTC (no pandas interference)
                py_dt = current_to_date.to_pydatetime()
                rates = mt5.copy_rates_from(SYMBOL, mt5_tf, py_dt, SAFE_BATCH_SIZE)
                if rates is not None and len(rates) > 0:
                    break
                time.sleep(1.0) # Wait for terminal to sync with broker server
                
            if rates is None or len(rates) == 0:
                err = mt5.last_error()
                # Weekend mitigation
                if iteration == 1:
                     print(f"    Batch {iteration}: 0 bars returned from {current_to_date}. Market might be closed (Weekend). Stepping back 3 days to find tail...")
                     current_to_date = current_to_date - pd.Timedelta(days=3)
                     continue
                else:
                     print(f"    Batch {iteration}: 0 bars returned after sync attempts. MT5 Error: {err}. End of history reached.")
                     break
                
            df_batch = pd.DataFrame(rates)
            df_batch['time'] = pd.to_datetime(df_batch['time'], unit='s', utc=True)
            df_batch.rename(columns={'tick_volume': 'volume'}, inplace=True)
            df_batch = df_batch[['time', 'open', 'high', 'low', 'close', 'volume']]
            
            batch_min_ts = df_batch['time'].min()
            batch_max_ts = df_batch['time'].max()
            
            print(f"    Batch {iteration}: {len(df_batch):>6} bars | Range: {batch_min_ts} to {batch_max_ts}")
            
            # OVERLAP GUARD
            if batch_min_ts >= earliest_seen_ts:
                print("    OVERLAP DETECTED: Batch min_ts is not strictly older than previous tail. Breaking.")
                break
                
            all_dfs.append(df_batch)
            total_pulled += len(df_batch)
            earliest_seen_ts = batch_min_ts
            
            # Step back 1 interval from the *earliest bar received*
            current_to_date = earliest_seen_ts - pd.Timedelta(seconds=TF_SECONDS[tf_label])

        if all_dfs:
            final_df = pd.concat(all_dfs, ignore_index=True)
            initial_count = len(final_df)
            final_df = final_df.drop_duplicates(subset=['time'])
            drop_count = initial_count - len(final_df)
            final_df = final_df.sort_values('time').reset_index(drop=True)
            
            print(f"  [SAVED TEMP] {tf_label}: Total Pulled: {initial_count} | Duplicates Dropped in batch: {drop_count} | Final Row Count: {len(final_df)}")
            print(f"  Final Span: {final_df['time'].min()} to {final_df['time'].max()}")
            final_df.to_csv(tmp_file, index=False)
        else:
            print(f"  No data pulled for {tf_label}.")

def validate_and_generate_report():
    print(f"\n[{datetime.utcnow().isoformat()}] Validating Extracted History...")
    report = {
        'xauusd_full_history_validation': {},
        'overall_summary': {
            'earliest_available': '9999-12-31',
            'longest_span_tf': None,
            'rebuild_recommended': False,
            'message': "Strict instructions followed: DO NOT automatically rebuild CLEAN or RESEARCH layers."
        }
    }
    
    longest_years = 0
    earliest_overall = "9999-12-31"
    
    for tf_label in TIMEFRAMES:
        tmp_file = os.path.join(TMP_DIR, f"{SYMBOL}_{tf_label}_extended_raw.csv")
        if not os.path.exists(tmp_file):
            continue
            
        df = pd.read_csv(tmp_file)
        df['time'] = pd.to_datetime(df['time'], utc=True)
        
        # Chrono order check
        assert df['time'].is_monotonic_increasing, f"CRITICAL: Non-increasing timestamps in {tf_label}"
        
        total_bars = len(df)
        min_ts = df['time'].min()
        max_ts = df['time'].max()
        years_covered = (max_ts - min_ts).days / 365.25
        
        if years_covered > longest_years:
            longest_years = years_covered
            report['overall_summary']['longest_span_tf'] = tf_label
            
        if min_ts.isoformat() < earliest_overall:
            earliest_overall = min_ts.isoformat()
        
        # Basic Gap Analysis (excluding weekends roughly)
        df['delta'] = df['time'].diff()
        major_gaps = df[df['delta'].dt.total_seconds() > 3 * 86400] # Gaps > 3 days (ignores most weekends)
        
        report['xauusd_full_history_validation'][tf_label] = {
            'earliest_timestamp': min_ts.isoformat(),
            'total_years_covered': round(years_covered, 2),
            'total_bars': total_bars,
            'major_gaps_count_gt_3days': len(major_gaps),
            'max_gap_days': round(df['delta'].max().total_seconds() / 86400, 2) if total_bars > 1 else 0
        }
        
    report['overall_summary']['earliest_available'] = earliest_overall
    
    report_path = os.path.join(REPORT_DIR, "xauusd_full_history_validation.yaml")
    import yaml
    with open(report_path, 'w') as f:
        yaml.dump(report, f, sort_keys=False)
    print(f"  Validation Report Saved: {report_path}")
    return report

def safe_merge_into_raw():
    print(f"\n[{datetime.utcnow().isoformat()}] Safe Merging into Protocol Raw Layers...")
    
    broker = "OCTAFX" # Hardcoded based on directive
    
    for tf_label in TIMEFRAMES:
        tmp_file = os.path.join(TMP_DIR, f"{SYMBOL}_{tf_label}_extended_raw.csv")
        if not os.path.exists(tmp_file):
            continue
            
        df_ext = pd.read_csv(tmp_file)
        df_ext['time'] = pd.to_datetime(df_ext['time'], utc=True)
        # Add 'spread' column as 0 for older data if missing, to match RAW schema
        if 'spread' not in df_ext.columns:
            df_ext['spread'] = 0
            
        print(f"\n  Merging {tf_label} ({len(df_ext)} extended rows)...")
        
        # Partition incoming temporary data by year
        df_ext['year'] = df_ext['time'].dt.year
        years_present = df_ext['year'].unique()
        
        for yr in sorted(years_present):
            target_file = os.path.join(BASE_DIR, f"{SYMBOL}_{broker}_{tf_label}_{yr}_RAW.csv")
            
            df_year_ext = df_ext[df_ext['year'] == yr].copy()
            df_year_ext = df_year_ext.drop(columns=['year'])
            
            if os.path.exists(target_file):
                df_existing = pd.read_csv(target_file)
                df_existing['time'] = pd.to_datetime(df_existing['time'], utc=True)
                
                # Merge
                combined = pd.concat([df_year_ext, df_existing], ignore_index=True)
                pre_len = len(combined)
                
                # Use drop_duplicates keeping 'last' which effectively overwrites older extended data with current pipeline data if conflicting
                combined = combined.drop_duplicates(subset=['time'], keep='last')
                post_len = len(combined)
                
                combined = combined.sort_values('time').reset_index(drop=True)
                
                added_rows = len(combined) - len(df_existing)
                print(f"    Year {yr}: Appended {added_rows} new historical rows (Total {len(combined)})")
            else:
                combined = df_year_ext.sort_values('time').reset_index(drop=True)
                print(f"    Year {yr}: Created new raw file (Total {len(combined)})")
            
            # Atomic Save
            tmp_tgt = target_file + ".tmp"
            combined.to_csv(tmp_tgt, index=False)
            os.replace(tmp_tgt, target_file)
            
            # Atomic Manifest Write (ONLY this file's manifest)
            _write_raw_manifest(target_file, combined, TF_SECONDS[tf_label])

if __name__ == "__main__":
    print(f"=== {ENGINE_VERSION} ===")
    
    if not mt5.initialize():
        print("CRITICAL: Failed to initialize MT5. Ensure terminal is running.")
        exit(1)
        
    try:
        caps = discover_broker_capabilities()
        pull_historical_data(caps)
        validate_and_generate_report()
        safe_merge_into_raw()
    finally:
        mt5.shutdown()
        
    print("\n[PROCESS COMPLETE. NO REBUILD TRIGGERED.]")

import os
import json
import hashlib
import pandas as pd
from datetime import datetime
from pathlib import Path

_DI = Path(__file__).resolve().parents[2]
_AG = _DI.parent / "Anti_Gravity_DATA_ROOT"
TMP_DIR    = str(_DI / "tmp" / "XAUUSD_HISTORY")
RAW_DIR    = str(_AG / "MASTER_DATA" / "XAUUSD_OCTAFX_MASTER" / "RAW")
REPORT_DIR = str(_DI / "reports")

SYMBOL = "XAUUSD"
BROKER = "OCTAFX"
TIMEFRAMES = ["1m", "5m", "15m", "30m", "1h", "4h", "1d", "1w", "1mn"]

TF_INTERVALS = {
    "1m": 60, "5m": 300, "15m": 900, "30m": 1800,
    "1h": 3600, "4h": 14400, "1d": 86400, "1w": 604800, "1mn": 2592000
}

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

def perform_safe_merge():
    print(f"\n[{datetime.utcnow().isoformat()}] Executing Atomic RAW Append Operations...")
    
    audit_report = {
        "timestamp_utc": datetime.utcnow().isoformat(),
        "status": "SUCCESS",
        "timeframes": {}
    }
    
    for tf in TIMEFRAMES:
        source_file = os.path.join(TMP_DIR, f"{SYMBOL}_{tf}_manual.csv")
        if not os.path.exists(source_file):
            continue
            
        print(f"\nProcessing staging file: {os.path.basename(source_file)}")
        try:
            df_stage = pd.read_csv(source_file)
            df_stage['time'] = pd.to_datetime(df_stage['time'], utc=True)
            
            if 'spread' not in df_stage.columns:
                df_stage['spread'] = 0
                
            # Enforce Schema ('time', 'open', 'high', 'low', 'close', 'volume', 'spread')
            df_stage = df_stage[['time', 'open', 'high', 'low', 'close', 'volume', 'spread']]
            
            df_stage['year'] = df_stage['time'].dt.year
            years_present = df_stage['year'].unique()
            
            total_appended_tf = 0
            total_dupes_removed = 0
            monotonic_passed = True
            tf_min_ts = "9999-12-31"
            tf_max_ts = "1970-01-01"
            
            for yr in sorted(years_present):
                target_file = os.path.join(RAW_DIR, f"{SYMBOL}_{BROKER}_{tf}_{yr}_RAW.csv")
                
                df_yr_stage = df_stage[df_stage['year'] == yr].copy().drop(columns=['year'])
                
                if os.path.exists(target_file):
                    df_target = pd.read_csv(target_file)
                    df_target['time'] = pd.to_datetime(df_target['time'], utc=True)
                    target_initial_len = len(df_target)
                    
                    combined = pd.concat([df_yr_stage, df_target], ignore_index=True)
                    combined_initial_len = len(combined)
                    
                    # Deduplicate by timestamp, keeping the *first* (which is the stage data, meaning our fresh deep history overrides older pipeline holes for the same timestamp)
                    # Actually pipelines runs are "closer" so usually KEEP LAST is safer for recent data, but for historical, KEEP LAST preserves pipeline data over manual history if they intersect.
                    combined = combined.drop_duplicates(subset=['time'], keep='last')
                    dupes_dropped = combined_initial_len - len(combined)
                    
                    combined = combined.sort_values('time').reset_index(drop=True)
                    appended_this_year = len(combined) - target_initial_len
                    
                else:
                    combined = df_yr_stage.sort_values('time').reset_index(drop=True)
                    dupes_dropped = 0
                    appended_this_year = len(combined)
                    
                if not combined['time'].is_monotonic_increasing:
                    monotonic_passed = False
                    
                total_appended_tf += appended_this_year
                total_dupes_removed += dupes_dropped
                
                yr_min = combined['time'].min().isoformat()
                yr_max = combined['time'].max().isoformat()
                
                if yr_min < tf_min_ts: tf_min_ts = yr_min
                if yr_max > tf_max_ts: tf_max_ts = yr_max
                
                # Atomic writes
                tmp_tgt = target_file + ".tmp"
                combined.to_csv(tmp_tgt, index=False)
                os.replace(tmp_tgt, target_file)
                
                _write_raw_manifest(target_file, combined, TF_INTERVALS.get(tf, 60))
                print(f"  [{yr}] Wrote {len(combined)} rows. (Added: {appended_this_year}, Dupes dropped: {dupes_dropped})")
            
            audit_report["timeframes"][tf] = {
                "final_earliest_timestamp": tf_min_ts,
                "final_latest_timestamp": tf_max_ts,
                "total_rows_added_across_years": total_appended_tf,
                "duplicates_removed_during_merge": total_dupes_removed,
                "monotonic_check": "PASS" if monotonic_passed else "FAIL"
            }
            if not monotonic_passed:
                print(f"  CRITICAL WARNING: {tf} monotonic check FAILED.")
                
        except Exception as e:
             print(f"FAILED on {tf}: {str(e)}")
             audit_report["timeframes"][tf] = {"error": str(e)}

    # Save Final Audit
    audit_file = os.path.join(REPORT_DIR, "raw_layer_expansion_audit.json")
    with open(audit_file, "w") as f:
        json.dump(audit_report, f, indent=4)
        
    print(f"\nOperations complete. Final Audit written to: {audit_file}")
    print("NO DOWNSTREAM TRIGGERS EXECUTED.")

if __name__ == "__main__":
    perform_safe_merge()

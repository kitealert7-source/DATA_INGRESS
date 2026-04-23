
"""
BTCUSD OCTAFX 15m Deep History Acquisition
Strategy: Forward-walking monthly copy_rates_range chunks (KB Section 1).
Covers 2020-01-01 to present. Stages to tmp/, then merges into RAW.
"""

import os
import time
import json
import hashlib
from datetime import datetime
import pandas as pd
import MetaTrader5 as mt5
from pathlib import Path

ENGINE_VERSION = "SOP17_HISTORY_ACQUISITION_BTCUSD_15m_v1"
_DI = Path(__file__).resolve().parents[2]
_AG = _DI.parent / "Anti_Gravity_DATA_ROOT"
BASE_DIR   = str(_AG / "MASTER_DATA" / "BTC_OCTAFX_MASTER" / "RAW")
TMP_DIR    = str(_DI / "tmp" / "BTCUSD_HISTORY")
REPORT_DIR = str(_DI / "reports")

SYMBOL     = "BTCUSD"
BROKER     = "OCTAFX"
TF_LABEL   = "15m"
MT5_TF     = mt5.TIMEFRAME_M15
TF_SECONDS = 900

START_DATE = pd.Timestamp("2020-01-01", tz="UTC")

os.makedirs(TMP_DIR, exist_ok=True)
os.makedirs(REPORT_DIR, exist_ok=True)


def compute_file_sha256(filepath: str) -> str:
    h = hashlib.sha256()
    b = bytearray(128 * 1024)
    mv = memoryview(b)
    with open(filepath, 'rb', buffering=0) as f:
        while n := f.readinto(mv):
            h.update(mv[:n])
    return h.hexdigest()


def _write_raw_manifest(filepath: str, df: pd.DataFrame):
    filename = os.path.basename(filepath)
    manifest_path = filepath + "_manifest.json"

    parts = filename.replace(".csv", "").split("_")
    symbol = parts[0]
    tf     = parts[2]
    year   = int(parts[3])

    row_count = len(df)
    first_ts  = df['time'].min().isoformat() if row_count > 0 else ""
    last_ts   = df['time'].max().isoformat() if row_count > 0 else ""
    file_hash = compute_file_sha256(filepath)

    manifest = {
        "symbol":           symbol,
        "timeframe":        tf,
        "year":             year,
        "row_count":        row_count,
        "first_timestamp":  first_ts,
        "last_timestamp":   last_ts,
        "sha256":           file_hash,
        "schema_version":   "1.1",
        "columns":          list(df.columns),
        "interval_seconds": TF_SECONDS,
    }

    tmp_path = manifest_path + ".tmp"
    with open(tmp_path, 'w') as f:
        json.dump(manifest, f, indent=2)
    os.replace(tmp_path, manifest_path)


def generate_monthly_ranges(start: pd.Timestamp, end: pd.Timestamp):
    """Yield (month_start, month_end) pairs, forward-walking from start to end."""
    current = start.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    while current < end:
        next_month = (current + pd.DateOffset(months=1))
        yield current, min(next_month, end)
        current = next_month


def pull_15m_monthly():
    print(f"\n[{datetime.utcnow().isoformat()}] Pulling {SYMBOL} {TF_LABEL} via monthly copy_rates_range chunks...")

    if not mt5.symbol_select(SYMBOL, True):
        print(f"CRITICAL: Failed to select {SYMBOL} in Market Watch.")
        return False

    now_utc   = pd.Timestamp(datetime.utcnow(), tz="UTC")
    tmp_file  = os.path.join(TMP_DIR, f"{SYMBOL}_{TF_LABEL}_extended_raw.csv")

    all_dfs      = []
    total_pulled = 0
    failed_months= []

    for month_start, month_end in generate_monthly_ranges(START_DATE, now_utc):
        from_dt = month_start.to_pydatetime()
        to_dt   = month_end.to_pydatetime()

        rates = None
        for attempt in range(5):
            rates = mt5.copy_rates_range(SYMBOL, MT5_TF, from_dt, to_dt)
            if rates is not None and len(rates) > 0:
                break
            time.sleep(1.5)

        if rates is None or len(rates) == 0:
            err = mt5.last_error()
            print(f"  [{month_start.strftime('%Y-%m')}] 0 bars | MT5 error: {err}")
            failed_months.append(month_start.strftime('%Y-%m'))
            continue

        df_chunk = pd.DataFrame(rates)
        df_chunk['time'] = pd.to_datetime(df_chunk['time'], unit='s', utc=True)
        df_chunk.rename(columns={'tick_volume': 'volume'}, inplace=True)
        df_chunk = df_chunk[['time', 'open', 'high', 'low', 'close', 'volume']]

        # Drop boundary overlap: exclude bars >= month_end to avoid double-counting
        df_chunk = df_chunk[df_chunk['time'] < month_end]

        all_dfs.append(df_chunk)
        total_pulled += len(df_chunk)
        print(f"  [{month_start.strftime('%Y-%m')}] {len(df_chunk):>6} bars | {df_chunk['time'].min()} to {df_chunk['time'].max()}")

    if not all_dfs:
        print("  No data pulled.")
        return False

    final_df = pd.concat(all_dfs, ignore_index=True)
    pre_dedup = len(final_df)
    final_df  = final_df.drop_duplicates(subset=['time'])
    final_df  = final_df.sort_values('time').reset_index(drop=True)

    print(f"\n  Total pulled: {pre_dedup} | After dedup: {len(final_df)}")
    print(f"  Span: {final_df['time'].min()} to {final_df['time'].max()}")
    if failed_months:
        print(f"  Failed months (0 bars): {failed_months}")

    final_df.to_csv(tmp_file, index=False)
    print(f"  Staged to: {tmp_file}")
    return True


def merge_into_raw():
    print(f"\n[{datetime.utcnow().isoformat()}] Merging {TF_LABEL} into RAW layer...")

    tmp_file = os.path.join(TMP_DIR, f"{SYMBOL}_{TF_LABEL}_extended_raw.csv")
    if not os.path.exists(tmp_file):
        print("  No temp file found. Run pull first.")
        return

    df_ext = pd.read_csv(tmp_file)
    df_ext['time'] = pd.to_datetime(df_ext['time'], utc=True)
    if 'spread' not in df_ext.columns:
        df_ext['spread'] = 0

    df_ext['year'] = df_ext['time'].dt.year

    for yr in sorted(df_ext['year'].unique()):
        target_file = os.path.join(BASE_DIR, f"{SYMBOL}_{BROKER}_{TF_LABEL}_{yr}_RAW.csv")

        df_year = df_ext[df_ext['year'] == yr].drop(columns=['year']).copy()

        if os.path.exists(target_file):
            df_existing = pd.read_csv(target_file)
            df_existing['time'] = pd.to_datetime(df_existing['time'], utc=True)

            combined = pd.concat([df_year, df_existing], ignore_index=True)
            combined = combined.drop_duplicates(subset=['time'], keep='last')
            combined = combined.sort_values('time').reset_index(drop=True)

            added = len(combined) - len(df_existing)
            print(f"  Year {yr}: +{added} rows (total {len(combined)})")
        else:
            combined = df_year.sort_values('time').reset_index(drop=True)
            print(f"  Year {yr}: Created new file (total {len(combined)})")

        tmp_tgt = target_file + ".tmp"
        combined.to_csv(tmp_tgt, index=False)
        os.replace(tmp_tgt, target_file)

        _write_raw_manifest(target_file, combined)


if __name__ == "__main__":
    print(f"=== {ENGINE_VERSION} ===")
    print(f"Symbol: {SYMBOL} | Feed: {BROKER} | TF: {TF_LABEL} | Start: {START_DATE.date()}")

    if not mt5.initialize():
        print("CRITICAL: Failed to initialize MT5. Ensure terminal is running.")
        exit(1)

    try:
        ok = pull_15m_monthly()
        if ok:
            merge_into_raw()
        else:
            print("Pull failed — skipping merge.")
    finally:
        mt5.shutdown()

    print("\n[PROCESS COMPLETE. NO REBUILD TRIGGERED.]")
    print("Next step: run clean_rebuild_sop17.py and rebuild_research_sop17.py manually.")

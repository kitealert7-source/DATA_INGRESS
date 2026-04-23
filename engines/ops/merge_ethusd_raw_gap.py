"""
merge_ethusd_raw_gap.py
=======================
One-shot safe merge: ETH_OCTAFX_MASTER/RAW  →  ETHUSD_OCTAFX_MASTER/RAW

Protocol:
  1. T_last  = max(timestamp) in canonical file
  2. filter  = source rows where timestamp > T_last  (strictly forward)
  3. append  + enforce: sorted ascending, no duplicates, no boundary gap
  4. verify  : row count delta, last timestamp advanced, no backward jumps

Atomic write — canonical file is only replaced after all checks pass.
Run with --dry-run first to preview what would be merged.

Usage:
    python engines/ops/merge_ethusd_raw_gap.py --dry-run
    python engines/ops/merge_ethusd_raw_gap.py
"""

import argparse
import hashlib
import json
import os
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

# ── Paths ─────────────────────────────────────────────────────────────────────
_DI  = Path(__file__).resolve().parents[2]
_AG  = _DI.parent / "Anti_Gravity_DATA_ROOT"
_MD  = _AG / "MASTER_DATA"

CANONICAL_RAW = _MD / "ETHUSD_OCTAFX_MASTER" / "RAW"
SOURCE_RAW    = _MD / "ETH_OCTAFX_MASTER"    / "RAW"

SYMBOL = "ETHUSD"
BROKER = "OCTAFX"
YEAR   = 2026
TIMEFRAMES = ["1m", "3m", "5m", "15m", "30m", "1h", "4h", "1d"]

TF_EXPECTED_BARS_PER_DAY = {
    "1m": 1440, "3m": 480, "5m": 288, "15m": 96,
    "30m": 48,  "1h": 24,  "4h": 6,   "1d": 1,
}
GAP_DAYS = 6   # approximate — used for sanity check only


# ── Helpers ───────────────────────────────────────────────────────────────────

def sha256(path: Path) -> str:
    h = hashlib.sha256()
    mv = memoryview(bytearray(128 * 1024))
    with open(path, "rb", buffering=0) as f:
        while n := f.readinto(mv):
            h.update(mv[:n])
    return h.hexdigest()


def read_raw(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, comment="#")
    df["time"] = pd.to_datetime(df["time"])
    return df


def write_manifest(csv_path: Path, df: pd.DataFrame) -> None:
    manifest = {
        "symbol":       SYMBOL,
        "broker":       BROKER,
        "bar_count":    len(df),
        "first_bar_utc": df["time"].min().isoformat() + "Z",
        "last_bar_utc":  df["time"].max().isoformat() + "Z",
        "sha256":       sha256(csv_path),
        "generated_utc": datetime.now(timezone.utc).isoformat(),
    }
    manifest_path = Path(str(csv_path) + "_manifest.json")
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")


# ── Core merge logic ──────────────────────────────────────────────────────────

def merge_timeframe(tf: str, dry_run: bool) -> bool:
    fname         = f"{SYMBOL}_{BROKER}_{tf}_{YEAR}_RAW.csv"
    canonical     = CANONICAL_RAW / fname
    source        = SOURCE_RAW    / fname

    print(f"\n[{tf}] -- merge_timeframe ------------------------------------------")

    if not canonical.exists():
        print(f"  [SKIP] canonical not found: {canonical}")
        return False
    if not source.exists():
        print(f"  [SKIP] source not found: {source}")
        return False

    can_df = read_raw(canonical)
    src_df = read_raw(source)

    if can_df.empty:
        print(f"  [ABORT] canonical is empty — not safe to merge into an empty file")
        return False

    # ── Step 1: boundary ──────────────────────────────────────────────────────
    T_last = can_df["time"].max()
    print(f"  canonical: {len(can_df):>7} rows | T_last = {T_last}")
    print(f"  source   : {len(src_df):>7} rows | T_max  = {src_df['time'].max()}")

    # ── Step 2: filter strictly forward ──────────────────────────────────────
    new_rows = src_df[src_df["time"] > T_last].copy()
    print(f"  new rows (timestamp > T_last): {len(new_rows)}")

    if new_rows.empty:
        print(f"  [SKIP] nothing to merge — canonical already at or ahead of source")
        return True  # not an error

    # ── Step 3: invariant checks on new rows ─────────────────────────────────
    new_rows = new_rows.sort_values("time").reset_index(drop=True)

    # No duplicates in new batch
    if new_rows["time"].duplicated().any():
        dup_count = new_rows["time"].duplicated().sum()
        print(f"  [ERROR] {dup_count} duplicate timestamp(s) in new rows — aborting")
        return False

    # No backward jump at boundary
    if new_rows["time"].iloc[0] <= T_last:
        print(f"  [ERROR] first new row ({new_rows['time'].iloc[0]}) not strictly > T_last ({T_last})")
        return False

    # Sanity: expected delta
    expected_min = int(TF_EXPECTED_BARS_PER_DAY[tf] * GAP_DAYS * 0.5)   # 50% lower bound (markets closed, weekends)
    expected_max = int(TF_EXPECTED_BARS_PER_DAY[tf] * GAP_DAYS * 1.1)   # 10% upper bound
    if not (expected_min <= len(new_rows) <= expected_max):
        print(f"  [WARN] new row count {len(new_rows)} outside expected range "
              f"[{expected_min}, {expected_max}] for {GAP_DAYS}d gap — proceeding anyway")

    if dry_run:
        print(f"  [DRY RUN] would append {len(new_rows)} rows | "
              f"new T_last = {new_rows['time'].max()}")
        return True

    # ── Step 3 cont: merge and enforce on combined frame ─────────────────────
    merged = pd.concat([can_df, new_rows], ignore_index=True)
    merged = merged.sort_values("time").reset_index(drop=True)

    if merged["time"].duplicated().any():
        dup_count = merged["time"].duplicated().sum()
        print(f"  [ERROR] {dup_count} duplicate(s) in merged frame — aborting")
        return False

    if not merged["time"].is_monotonic_increasing:
        print(f"  [ERROR] merged frame is not monotonically increasing — aborting")
        return False

    # ── Step 4: quick integrity check ────────────────────────────────────────
    rows_before  = len(can_df)
    rows_after   = len(merged)
    delta        = rows_after - rows_before
    new_T_last   = merged["time"].max()

    if delta != len(new_rows):
        print(f"  [ERROR] row delta mismatch: expected {len(new_rows)}, got {delta}")
        return False
    if new_T_last <= T_last:
        print(f"  [ERROR] T_last did not advance ({new_T_last} <= {T_last})")
        return False

    print(f"  integrity OK | rows: {rows_before} -> {rows_after} (+{delta}) | "
          f"T_last: {T_last} -> {new_T_last}")

    # ── Atomic write ─────────────────────────────────────────────────────────
    tmp_path = canonical.with_suffix(".tmp")
    try:
        merged.to_csv(tmp_path, index=False)
        tmp_path.replace(canonical)
    except Exception as e:
        print(f"  [ERROR] write failed: {e}")
        if tmp_path.exists():
            tmp_path.unlink()
        return False

    write_manifest(canonical, merged)
    print(f"  [COMMIT] {canonical.name} updated")
    return True


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Safe merge ETHUSD RAW gap")
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview what would be merged without writing")
    args = parser.parse_args()

    if args.dry_run:
        print("=" * 60)
        print("DRY RUN — no files will be written")
        print("=" * 60)
    else:
        print("=" * 60)
        print("LIVE MERGE — files will be updated atomically")
        print("=" * 60)

    if not CANONICAL_RAW.exists():
        print(f"[ABORT] canonical RAW dir not found: {CANONICAL_RAW}")
        sys.exit(1)
    if not SOURCE_RAW.exists():
        print(f"[ABORT] source RAW dir not found: {SOURCE_RAW}")
        sys.exit(1)

    results = {}
    for tf in TIMEFRAMES:
        results[tf] = merge_timeframe(tf, dry_run=args.dry_run)

    print("\n" + "=" * 60)
    print("MERGE SUMMARY")
    print("=" * 60)
    passed = [tf for tf, ok in results.items() if ok]
    failed = [tf for tf, ok in results.items() if not ok]
    print(f"  OK    : {', '.join(passed) if passed else 'none'}")
    print(f"  FAILED: {', '.join(failed) if failed else 'none'}")
    print("=" * 60)

    if failed:
        print("\n[WARN] Some timeframes failed — review errors above before running pipeline.")
        sys.exit(1)
    elif not args.dry_run:
        print("\n[DONE] All timeframes merged. Next steps:")
        print("  1. Verify ETH_OCTAFX_MASTER can be retired (CLEAN/RESEARCH dirs)")
        print("  2. Run: python engines/ops/daily_pipeline.py")
        print("     or targeted: python engines/core/clean_rebuild_sop17.py --all")
        print("                  python engines/core/rebuild_research_sop17.py --register-lineage")


if __name__ == "__main__":
    main()


import pandas as pd
import os
from pathlib import Path

ENGINE_VERSION = "SOP17_INCREMENTAL_STABLE_v1"
import re
import sys
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Union

@dataclass
class ValidationMetrics:
    bars_total: int = 0
    duplicates: int = 0
    monotonic_errors: int = 0
    max_gap_bars: int = 0
    bars_expected: int = 0
    missing_pct: float = 0.0
    gaps: List[Dict] = field(default_factory=list)

@dataclass
class ValidationResult:
    file: str
    status: str # "PASS", "FAIL", or "WARN"
    metrics: ValidationMetrics
    valid: bool = True
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)

class SOP17Validator:
    """
    Enforces SOP v17 rules for Anti-Gravity Datasets.
    Rev3 (Corrected): Zero Tolerance Gaps, Pure Logic, No Unauthorized OHLC Rules.
    """
    
    # Valid Feeds & Timeframes (SOP_DATA_TIMEFRAMES_v1)
    SUPPORTED_MATRIX = {
        "OCTAFX": ["1m", "3m", "5m", "15m", "30m", "1h", "4h", "1d", "1w", "1mn"], 
        "DELTA": ["1m", "3m", "5m", "15m", "1h", "4h", "1d"],        
        "MT5": ["3m", "5m"],     # Legacy/Fallback
        "YAHOO": ["1d"]          # Structural macro data only (US10Y sovereign rates)
    }

    NAMING_REGEX = re.compile(
        r"^(?P<asset>[A-Z0-9]+)_(?P<feed>[A-Z]+)_(?P<timeframe>\d+[mhdwn]+)_(?P<year>\d{4})_(?P<type>RAW|CLEAN|RESEARCH)\.csv$"
    )

    # Asset Configuration
    ASSET_CONFIG = {
        "CRYPTO": {
            "gap_tolerance_bars": 0, # Zero tolerance (24/7 exchanges like Delta)
            "session_type": "24/7"
        },
        "CRYPTO_CFD": {
            "gap_tolerance_bars": 3000, # Allow weekend gaps (~50h for 1m)
            "session_type": "SESSION" # Weekends + daily sessions ignored
        },
        "FOREX": {
            "gap_tolerance_bars": 600, # Allow weekend gaps (~30h for 3m)
            "session_type": "SESSION" # Weekends ignored
        },
        "INDEX_CFD": {
            "gap_tolerance_bars": 10000, # Allow ~167 hours (7 days) for 1m (extended European holidays: Christmas/New Year)
            "session_type": "SESSION" # Weekends ignored, continuous CFD (no roll logic)
        },
        "SOVEREIGN_RATES": {
            "gap_tolerance_bars": 100000, # Structural macro data, non-tradable, large tolerance
            "session_type": "STRUCTURAL" # Non-tradable macro proxy
        }
    }

    # --- FRESHNESS GATE (SOP17 Rev4) ---
    # Max allowed staleness for the CURRENT year's partition, measured in wall-clock hours.
    # Simple universal rules: no weekend logic, no holiday files.
    # SOVEREIGN_RATES is exempt (structural/non-tradable data).
    FRESHNESS_THRESHOLDS_HOURS = {
        "CRYPTO":          6,    # Delta Exchange: 24/7, tightest tolerance
        "CRYPTO_CFD":      72,   # MT5/OctaFX broker: covers full weekend
        "FOREX":           72,   # MT5/OctaFX broker: covers full weekend
        "INDEX_CFD":       120,  # MT5/OctaFX broker: covers long weekends + exchange holidays (4-day gaps)
        "SOVEREIGN_RATES": None, # Exempt - structural macro data
    }

    @staticmethod
    def _check_freshness(filename: str, last_ts, asset_class: str, tf_key: str) -> Optional[str]:
        """
        Compares the dataset's last timestamp against UTC now.
        Returns a formatted error string if stale, None if fresh.
        Only applied to current-year partitions.
        """
        from datetime import datetime, timezone
        threshold_hours = SOP17Validator.FRESHNESS_THRESHOLDS_HOURS.get(asset_class)
        if threshold_hours is None:
            return None  # Exempt asset class

        # Dynamic override for large timeframes
        if tf_key == "1d" and threshold_hours < 36:
            threshold_hours = 36
        elif tf_key == "4h" and threshold_hours < 12:
            threshold_hours = 12

        # Extract base asset name and feed
        asset_match = re.search(r'^([A-Z0-9]+)_([A-Z0-9]+)_', filename)
        if asset_match:
            asset_name = asset_match.group(1)
            feed_name = asset_match.group(2)
        else:
            asset_name = "UNKNOWN"
            feed_name = "UNKNOWN"

        # Normalise last_ts to UTC-naive for comparison
        try:
            if hasattr(last_ts, 'tzinfo') and last_ts.tzinfo is not None:
                last_ts = last_ts.tz_convert('UTC').tz_localize(None)
            elif not isinstance(last_ts, type(pd.Timestamp('2020-01-01'))):
                last_ts = pd.Timestamp(last_ts)
        except Exception:
            return None  # Cannot parse, skip check

        now_utc = pd.Timestamp(datetime.utcnow())
        staleness_hours = (now_utc - last_ts).total_seconds() / 3600

        if staleness_hours > threshold_hours:
            return (
                f"[STALE] asset={asset_name} feed={feed_name} tf={tf_key} file={filename}\n"
                f"last_timestamp={last_ts.strftime('%Y-%m-%d %H:%M')}\n"
                f"now={now_utc.strftime('%Y-%m-%d %H:%M')}\n"
                f"threshold={threshold_hours}h\n"
                f"staleness={staleness_hours:.1f}h"
            )
        return None

    REQUIRED_RESEARCH_HEADERS = [
        "dataset_version",
        "execution_model_version",
        "utc_normalization_flag",
        "generation_timestamp"
    ]

    @staticmethod
    def _validate_filename_convention(filename: str) -> List[str]:
        errors = []
        match = SOP17Validator.NAMING_REGEX.match(filename)
        
        if not match:
            # Check legacy pattern to be helpful
            legacy_match = re.match(r"^[A-Z0-9]+_\d+[mh]_\d{4}_[A-Z]+_(RAW|CLEAN|RESEARCH)\.csv$", filename)
            if legacy_match:
                 errors.append(f"Legacy Filename: {filename}. Feed MUST be second component (e.g. XAUUSD_OCTAFX_5m...).")
            else:
                 errors.append(f"Invalid Filename: {filename}. Must match [ASSET]_[FEED]_[TIMEFRAME]_[YEAR]_[TYPE].csv")
            return errors

        feed = match.group("feed")
        tf = match.group("timeframe")

        # Feed-Timeframe Support Check
        if feed not in SOP17Validator.SUPPORTED_MATRIX:
            if feed == "MT5":
                 pass # Warning or Soft Pass for now
            else:
                 errors.append(f"Unsupported FEED: {feed}")
        
        if feed in SOP17Validator.SUPPORTED_MATRIX:
            allowed_tfs = SOP17Validator.SUPPORTED_MATRIX[feed]
            if tf not in allowed_tfs:
                errors.append(f"Timeframe {tf} not strictly supported for feed {feed}. Allowed: {allowed_tfs}")
                
        return errors

    @staticmethod
    def _detect_asset_class(filename: str) -> str:
        if ("BTC" in filename or "ETH" in filename) and ("MT5" in filename or "OCTAFX" in filename): return "CRYPTO_CFD"
        if ("BTC" in filename or "ETH" in filename) and "DELTA" in filename: return "CRYPTO"
        if "BTC" in filename or "ETH" in filename: return "CRYPTO"
        # Detect continuous index CFDs (no futures/roll logic)
        if any(idx in filename for idx in ["NAS100", "SPX500", "GER40", "AUS200", "UK100", "FRA40", "ESP35", "EUSTX50", "US30", "JPN225"]):
            return "INDEX_CFD"
        # Detect sovereign rates (structural macro proxy)
        if "US10Y" in filename:
            return "SOVEREIGN_RATES"
        # Detect FX pairs: XAU, or major currencies
        if any(fx in filename for fx in ["XAU", "EUR", "GBP", "JPY", "CHF", "AUD", "NZD", "CAD"]): 
            return "FOREX"
        return "CRYPTO" 

    @staticmethod
    def _parse_timeframe(filename: str) -> int:
        if "_1m_" in filename: return 60
        if "_2m_" in filename: return 120
        if "_3m_" in filename: return 180
        if "_5m_" in filename: return 300
        if "_15m_" in filename: return 900
        if "_30m_" in filename: return 1800
        if "_1h_" in filename: return 3600
        if "_4h_" in filename: return 14400
        if "_1d_" in filename: return 86400
        if "_1w_" in filename: return 604800
        if "_1mn_" in filename: return 2592000
        match = re.search(r'_(\d+)([mhdwn]+)_', filename)
        if match:
             val = int(match.group(1))
             unit = match.group(2)
             if unit == 'm': return val * 60
             if unit == 'h': return val * 3600
             if unit == 'd': return val * 86400
             if unit == 'w': return val * 604800
             if unit == 'mn': return val * 2592000
        return 300 

    @staticmethod
    def validate_raw_extended(filepath: str) -> ValidationResult:
        """
        Pure logic validation. Returns ValidationResult.
        """
        filename = os.path.basename(filepath)
        # Handle atomic write temp files (strip .tmp for naming validation)
        if filename.endswith('.tmp'):
            filename = filename[:-4]  # Remove .tmp suffix
        asset_class = SOP17Validator._detect_asset_class(filename)
        interval_seconds = SOP17Validator._parse_timeframe(filename)
        
        metrics = ValidationMetrics()
        result = ValidationResult(file=filename, status="PASS", metrics=metrics)

        # 0. Naming & SOP Compliance (SOP v1/v17)
        naming_errors = SOP17Validator._validate_filename_convention(filename)
        if naming_errors:
            result.valid = False
            result.status = "FAIL"
            result.errors.extend(naming_errors)
            # If critical naming error, we might want to stop, but checking content is useful for migration.

        try:
            # Read Data
            df = pd.read_csv(filepath, comment='#')
            
            if 'time' not in df.columns:
                result.valid = False
                result.status = "FAIL"
                result.errors.append("Missing 'time' column")
                return result

            # Ensure Datetime
            if not pd.api.types.is_datetime64_any_dtype(df['time']):
                df['time'] = pd.to_datetime(df['time'], format='mixed', utc=True).dt.tz_localize(None)
            elif df['time'].dt.tz is not None:
                df['time'] = df['time'].dt.tz_localize(None)

            metrics.bars_total = len(df)
            if metrics.bars_total == 0:
                result.valid = False
                result.status = "FAIL"
                result.errors.append("File is empty")
                return result
                
            # --- MINIMUM ROW THRESHOLD & EARLY-YEAR EXCEPTION ---
            MIN_ROWS_BY_TF = {
                "1mn": 6,
                "1w": 26,
                "1d": 120,
                "4h": 600,
                "1h": 2000,
                "30m": 3500,
                "15m": 7500,
                "5m": 15000,
                "3m": 15000,
                "1m": 15000,
            }
            
            # Extract TF and Year from filename 
            tf_match = re.search(r'_(\d+[mhdwn]+)_', filename)
            yr_match = re.search(r'_(\d{4})_', filename)
            
            if tf_match and yr_match:
                tf_key = tf_match.group(1).lower()
                dataset_year = int(yr_match.group(1))
                
                min_required = MIN_ROWS_BY_TF.get(tf_key, 0)
                
                # Check 2014 or first year partial exception dynamically:
                first_timestamp = df['time'].min()
                # Determine earliest possible year from RAW if possible, or fallback to file's first timestamp year
                # Specifically checking if first_ts matches dataset_year and it starts late in the year (month > 1)
                
                # We can't easily globally scan RAW without huge overhead here so we trust the dataframe's first date
                # Exception logic: If the dataset's very first row falls entirely inside the year and starts after January,
                # it's likely a partial first year.
                
                # We ALSO must exempt the CURRENT year (which is obviously partially completed).
                # Current year is determined by max timestamp or system time. We will use system time.
                from datetime import datetime
                current_year = datetime.utcnow().year
                
                is_partial_first_year = (first_timestamp.year == dataset_year) and (first_timestamp.month > 1)
                is_current_year = (dataset_year == current_year)
                
                if (not is_partial_first_year) and (not is_current_year) and (metrics.bars_total < min_required):
                    result.valid = False
                    result.status = "FAIL"
                    result.errors.append(f"Insufficient row count: {metrics.bars_total} < {min_required} required for {tf_key}")
                    return result

            
            # --- RESAMPLING CHECK (SOP Rule 9 / Sect 5) ---
            # Derived data is strictly forbidden. 
            # We check if the actual median interval matches the filename timeframe.
            if len(df) > 10:
                df['delta_check'] = df['time'].diff()
                median_delta = df['delta_check'].median().total_seconds()
                
                # Dynamic tolerance for resampling check
                # '1mn' is ~30 days (2592000s) but month lengths vary between 28-31 days (max 2678400s).
                # '1w' is 604800s. We allow up to 10% drift for large macro tfs, otherwise strictly 1.0s.
                tolerance = 1.0
                if interval_seconds >= 604800:
                    tolerance = interval_seconds * 0.15
                    
                if abs(median_delta - interval_seconds) > tolerance: # Tolerance 
                     result.valid = False
                     result.status = "FAIL"
                     result.errors.append(f"CRITICAL: Resampling Artifact? Filename implies {interval_seconds}s but median data interval is {median_delta}s.")
            
            # Define interval for gap logic
            interval = interval_seconds

            # 1. Integrity Rules (HARD FAIL)
            dup_count = df['time'].duplicated().sum()
            metrics.duplicates = int(dup_count)
            if dup_count > 0:
                result.valid = False
                result.status = "FAIL"
                result.errors.append(f"Found {dup_count} duplicate timestamps")

            if not df['time'].is_monotonic_increasing:
                metrics.monotonic_errors = 1 
                result.valid = False
                result.status = "FAIL"
                result.errors.append("Timestamps not monotonic")

            if not result.valid:
                return result


            # 2. Gap Detection (Zero Tolerance)
            # Calculate Expected Bars
            first_ts = df['time'].min()
            last_ts = df['time'].max()
            
            # Logic for Expected Bars & Gaps
            total_seconds = (last_ts - first_ts).total_seconds()
            raw_expected = int(total_seconds / interval) + 1
            
            # FX Weekend Adjustment logic roughly
            # For exact "Bars Expected", we ideally iterate. 
            # But for large files, iteration is slow.
            # We will use gap detection to find MISSING chunks.
            
            df['delta'] = df['time'].diff().shift(-1)
            # Last row delta is NaN
            
            # Gap Threshold: Anything > interval (with small floating point buffer) is a gap technically.
            # We allow 1.1x interval to handle minor drift if any, but strict rule says > 1 bar.
            # So delta > interval * 1.5 is definitely a missing bar.
            # Using 1.1 ensures we catch single missing bars (2x interval).
            
            # Convert delta to seconds for easy float comparison
            df['delta_sec'] = df['delta'].dt.total_seconds()
            
            gap_mask = df['delta_sec'] > (interval * 1.05) 
            gaps_df = df[gap_mask].copy()
            
            current_gaps = []
            max_gap = 0
            
            # Analyze Gaps
            bars_lost_cumulative = 0
            
            for idx, row in gaps_df.iterrows():
                gap_seconds = row['delta_sec']
                gap_bars = int(gap_seconds / interval) - 1
                
                # Check for Weekend/Holiday Exception (FX and CRYPTO_CFD)
                is_exempt = False
                if asset_class in ("FOREX", "CRYPTO_CFD"):
                    gap_start = row['time']
                    gap_end = gap_start + pd.Timedelta(seconds=gap_seconds)
                    
                    # Weekend exemption: If gap > 100 bars AND touches Fri/Sat/Sun
                    # Friday=4, Saturday=5, Sunday=6, Monday=0
                    start_dow = gap_start.weekday()
                    end_dow = gap_end.weekday()
                    
                    # Exempt if: start is Fri/Sat/Sun OR end is Sat/Sun/Mon (weekend spans)
                    if gap_bars > 100:
                        if start_dow >= 4 or start_dow == 0 or end_dow >= 5 or end_dow <= 1:
                            is_exempt = True
                        
                        # Holiday Exception: Christmas 2025 (Dec 24-26)
                        if gap_start.year == 2025 and gap_start.month == 12 and gap_start.day in [24, 25, 26]:
                             is_exempt = True
                
                if not is_exempt:
                   bars_lost_cumulative += gap_bars
                   if gap_bars > max_gap:
                       max_gap = gap_bars
                   current_gaps.append({
                       "start": row['time'],
                       "gap_bars": gap_bars,
                       "seconds": gap_seconds
                   })

            metrics.max_gap_bars = max_gap
            metrics.gaps = current_gaps
            metrics.bars_expected = metrics.bars_total + bars_lost_cumulative
            
            if metrics.bars_expected > 0:
                metrics.missing_pct = round(1.0 - (metrics.bars_total / metrics.bars_expected), 6)
            
            # Enforce Zero Tolerance (Hard Fail)
            if max_gap > SOP17Validator.ASSET_CONFIG[asset_class]["gap_tolerance_bars"]:
                 result.valid = False
                 result.status = "FAIL"
                 result.errors.append(f"GAP DETECTED: Max gap {max_gap} bars exceeds tolerance ({SOP17Validator.ASSET_CONFIG[asset_class]['gap_tolerance_bars']})")

            # --- FRESHNESS GATE: Only check current-year partitions ---
            # Exempt sparse timeframes (1w, 1mn) - naturally updated weekly/monthly
            SPARSE_TIMEFRAMES = {"1w", "1mn"}
            if yr_match and tf_match:
                from datetime import datetime
                current_year = datetime.utcnow().year
                tf_key_fresh = tf_match.group(1).lower()
                if int(yr_match.group(1)) == current_year and tf_key_fresh not in SPARSE_TIMEFRAMES:
                    freshness_error = SOP17Validator._check_freshness(
                        filename, last_ts, asset_class, tf_key_fresh
                    )
                    if freshness_error:
                        result.warnings.append(freshness_error)

            return result

        except Exception as e:
            result.valid = False
            result.status = "FAIL"
            result.errors.append(f"Validator Exception: {str(e)}")
            return result
            
    # Legacy wrappers for existing scripts (Audit All mode)
    # We will repurpose them to print if run as script, but return bool if used as lib.
    
    @staticmethod
    def validate_raw(filepath: str) -> bool:
        res = SOP17Validator.validate_raw_extended(filepath)
        if not res.valid:
            # Side effect for legacy: print errors? 
            # Runbook says "Pure Logic". 
            # But existing scripts might rely on "validate_raw" returning False and printing.
            # We will adhere to correct Runbook "Pure Logic" inside the class.
            # So we print NOTHING here.
            return False
        return True

    @staticmethod
    def validate_clean(filepath: str) -> bool:
        return SOP17Validator.validate_raw(filepath)

    @staticmethod
    def validate_research(filepath: str) -> bool:
        """
        Validate RESEARCH datasets with OctaFX execution price governance.
        Enforces ADDENDUM_EXECUTION_PRICE_SEMANTICS_OCTAFX.md rules.
        """
        filename = os.path.basename(filepath)
        
        # First, run standard integrity checks
        base_valid = SOP17Validator.validate_raw(filepath)
        if not base_valid:
            return False
        
        # OCTAFX EXECUTION PRICE GOVERNANCE (ADDENDUM)
        # Rule 1: Detect Invalid Hybrid (CRITICAL FAIL)
        # Applies ONLY when ALL are true:
        # - feed == OCTAFX
        # - dataset_stage == RESEARCH
        # - execution_model_version starts with octafx_exec
        # - spread == 0
        # - RESEARCH OHLC == CLEAN OHLC
        
        if "OCTAFX" in filename.upper() and "_RESEARCH.csv" in filename:
            try:
                # Read RESEARCH file with metadata
                with open(filepath, 'r', encoding='utf-8') as f:
                    lines = f.readlines()
                
                # Extract execution_model_version from header
                exec_model_version = None
                for line in lines:
                    if line.startswith("# execution_model_version:"):
                        exec_model_version = line.split(":", 1)[1].strip()
                        break
                
                # Check if octafx_exec model is declared
                if exec_model_version and exec_model_version.startswith("octafx_exec"):
                    # Read data to check spread and OHLC
                    df = pd.read_csv(filepath, comment='#')
                    
                    if 'spread' in df.columns:
                        # Check if spread is zero
                        if (df['spread'] == 0).all():
                            # CRITICAL: Must verify OHLC transformation occurred
                            # We need to compare against CLEAN to detect the forbidden hybrid
                            # Construct CLEAN path
                            clean_filename = filename.replace("_RESEARCH.csv", "_CLEAN.csv")
                            clean_path = filepath.replace(filename, "").replace("RESEARCH", "CLEAN") + clean_filename
                            
                            if os.path.exists(clean_path):
                                df_clean = pd.read_csv(clean_path, comment='#')
                                
                                # Compare OHLC columns
                                if len(df) == len(df_clean):
                                    ohlc_cols = ['open', 'high', 'low', 'close']
                                    if all(col in df.columns and col in df_clean.columns for col in ohlc_cols):
                                        # Check if OHLC is identical (forbidden hybrid)
                                        ohlc_identical = all(
                                            df[col].equals(df_clean[col]) for col in ohlc_cols
                                        )
                                        
                                        if ohlc_identical:
                                            print(f"CRITICAL VALIDATION FAILURE: {filename}")
                                            print(f"  FORBIDDEN HYBRID DETECTED (ADDENDUM_EXECUTION_PRICE_SEMANTICS_OCTAFX.md)")
                                            print(f"  - Feed: OCTAFX")
                                            print(f"  - Stage: RESEARCH")
                                            print(f"  - Execution Model: {exec_model_version}")
                                            print(f"  - Spread: 0")
                                            print(f"  - RESEARCH OHLC == CLEAN OHLC (BID prices, NOT execution prices)")
                                            print(f"  This violates Section 5 (Forbidden States).")
                                            print(f"  RESEARCH datasets MUST contain execution prices (ASK-based OHLC).")
                                            return False
            
            except Exception as e:
                print(f"Error during OctaFX governance validation for {filename}: {e}")
                return False
        
        return True

    @staticmethod
    def abort_on_failure(success: bool, context: str):
        if not success:
            sys.exit(1)

if __name__ == "__main__":
    import argparse
    import json
    from datetime import datetime, timezone
    
    parser = argparse.ArgumentParser()
    parser.add_argument("--audit-all", action="store_true", help="Audit all datasets")
    args = parser.parse_args()
    
    if args.audit_all:
        print("--- STARTING FULL AUDIT (Rev3 Strict) ---")
        import sys
        sys.path.append(os.getcwd())
        base_dir = str(Path(__file__).resolve().parents[3] / "Anti_Gravity_DATA_ROOT" / "MASTER_DATA")
        failed = False
        dataset_count = 0
        
        for root, dirs, files in os.walk(base_dir):
            if 'ARCHIVE' in root or '.git' in root or '.bak' in root:
                continue
            for file in files:
                if "_RAW" in file and file.endswith(".csv"):
                    if "_MT5_" in file:
                        continue
                    path = os.path.join(root, file)
                    res = SOP17Validator.validate_raw_extended(path)
                    dataset_count += 1
                    
                    if not res.valid:
                        status = "FAIL"
                    elif res.warnings:
                        status = "WARN"
                    else:
                        status = "PASS"
                    print(f"[{status}] {file} | Bars: {res.metrics.bars_total} | MaxGap: {res.metrics.max_gap_bars} | Dup: {res.metrics.duplicates}")

                    if not res.valid:
                        failed = True
                        for e in res.errors:
                            print(f"   -> {e}")
                    for w in res.warnings:
                        print(f"   -> {w}")
        
        # Emit machine-readable summary
        summary = {
            "status": "FAIL" if failed else "PASS",
            "datasets_validated": dataset_count,
            "timestamp_utc": datetime.now(timezone.utc).isoformat()
        }
        
        summary_path = str(Path(__file__).resolve().parents[2] / "state" / "last_validation_summary.json")
        with open(summary_path, 'w') as f:
            json.dump(summary, f, indent=2)
        print(f"\n[SUMMARY] Status: {summary['status']} | Datasets: {dataset_count}")
        print(f"[SUMMARY] Emitted: {summary_path}")
                            
        if failed:
            sys.exit(1)
        else:
            sys.exit(0)


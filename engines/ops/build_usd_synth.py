
import os
import sys
import glob
import json
import logging
import hashlib
from datetime import datetime
import pandas as pd
import numpy as np

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constants
PAIRS = ["EURUSD", "GBPUSD", "AUDUSD", "USDJPY", "USDCAD"]
INVERT_PAIRS = ["EURUSD", "GBPUSD", "AUDUSD"]
KEEP_PAIRS = ["USDJPY", "USDCAD"]
REQUIRED_TIMEFRAME = "D1"
BASE_VALUE = 100.0
OUTPUT_DIR_REL = os.path.join("SYSTEM_FACTORS", "USD_SYNTH")
DATA_ROOT_NAME = "Anti_Gravity_DATA_ROOT"

def get_project_root():
    # Assuming script is in scripts/data_engineering/
    # And project root is 2 levels up
    script_path = os.path.abspath(__file__)
    return os.path.dirname(os.path.dirname(os.path.dirname(script_path)))

def find_data_root(project_root):
    # Data root should be a sibling of the project root
    parent_dir = os.path.dirname(project_root)
    data_root = os.path.join(parent_dir, DATA_ROOT_NAME)
    if os.path.exists(data_root):
        return data_root
    return None

def compute_file_hash(filepath):
    """Compute SHA256 hash of a file."""
    sha256_hash = hashlib.sha256()
    with open(filepath, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()

def load_pair_data(data_root, pair):
    """Load all RESEARCH daily CSVs for a pair."""
    # Pattern: MASTER_DATA/[PAIR]_OCTAFX_MASTER/RESEARCH/*_D1*.csv or *_1d*.csv
    # Note: Search patterns based on previous locate_fx_data results
    
    search_path = os.path.join(data_root, "MASTER_DATA", f"{pair}_OCTAFX_MASTER", "RESEARCH")
    
    if not os.path.exists(search_path):
        logger.error(f"Path not found: {search_path}")
        return None, []

    patterns = [f"*{pair}*_D1*.csv", f"*{pair}*_1d*.csv"]
    files = []
    for pat in patterns:
        files.extend(glob.glob(os.path.join(search_path, pat)))
    
    if not files:
        logger.error(f"No daily files found for {pair} in {search_path}")
        return None, []

    dfs = []
    file_hashes = {}
    
    for f in sorted(files):
        logger.info(f"Loading {f}")
        try:
            # Skip comment lines (lines starting with #)
            df = pd.read_csv(f, comment='#')
            
            # Map lowecase columns to expected names
            # Expected in file: time, open, high, low, close...
            rename_map = {
                "time": "Date",
                "close": "Close"
            }
            df.rename(columns=rename_map, inplace=True)

            # Ensure Date column exists and is datetime
            if "Date" not in df.columns:
                 logger.warning(f"File {f} missing 'time'/'Date' column")
                 continue
            
            df["Date"] = pd.to_datetime(df["Date"])
            df.set_index("Date", inplace=True)
            
            # Keep only Close
            if "Close" not in df.columns:
                 logger.warning(f"File {f} missing 'close'/'Close' column")
                 continue
            
            dfs.append(df[["Close"]])
            file_hashes[os.path.basename(f)] = compute_file_hash(f)
            
        except Exception as e:
            logger.error(f"Failed to read {f}: {e}")
            raise

    if not dfs:
        return None, []

    combined_df = pd.concat(dfs)
    combined_df = combined_df.sort_index()
    # Remove duplicates if any (though RESEARCH shouldn't have them ideally)
    combined_df = combined_df[~combined_df.index.duplicated(keep='last')]
    
    return combined_df, file_hashes

def construction_logic(project_root, data_root):
    logger.info("Starting Synthetic USD Construction...")
    
    pair_data = {}
    all_hashes = {}
    
    # 1. Load Data
    for pair in PAIRS:
        df, hashes = load_pair_data(data_root, pair)
        if df is None:
            logger.error(f"Failed to load data for {pair}. Aborting.")
            sys.exit(1)
        
        # Calculate log returns: ln(Ct / Ct-1)
        df["ret"] = np.log(df["Close"] / df["Close"].shift(1))
        
        # Drop initial NaN from return calculation
        df.dropna(inplace=True)
        
        pair_data[pair] = df
        all_hashes[pair] = hashes

    # 2. Merge and Align (Intersection)
    # Start with the first pair index
    common_index = pair_data[PAIRS[0]].index
    
    for pair in PAIRS[1:]:
        common_index = common_index.intersection(pair_data[pair].index)
    
    logger.info(f"Intersection Date Count: {len(common_index)}")
    
    if len(common_index) == 0:
        logger.error("No overlapping dates found! Aborting.")
        sys.exit(1)

    # Check for data loss
    # Using the first pair as a loose benchmark for total available history
    total_dates_primary = len(pair_data[PAIRS[0]])
    dropped_count = total_dates_primary - len(common_index)
    dropped_pct = (dropped_count / total_dates_primary) * 100
    
    logger.info(f"Data alignment: Dropped {dropped_count} dates ({dropped_pct:.2f}%) from primary pair baseline.")
    
    if dropped_pct > 5.0:
        logger.error(f"CRITICAL: Alignment dropped {dropped_pct:.2f}% of dates (Threshold: 5%). ABORTING.")
        sys.exit(1)

    # Align all dfs to common index
    aligned_returns = pd.DataFrame(index=common_index)
    
    for pair in PAIRS:
        # Reindex and select 'ret'
        series = pair_data[pair].loc[common_index, "ret"]
        
        # 3. Direction Normalization
        if pair in INVERT_PAIRS:
            series = series * -1.0
            
        aligned_returns[pair] = series

    # 4. Aggregation (Equal Weight Mean)
    # USD_SYNTH_RET_D1 = mean(...)
    aligned_returns["USD_SYNTH_RET"] = aligned_returns.mean(axis=1)
    
    # 5. Synthetic Close Reconstruction
    # Start at 100
    # Close = 100 * exp(cumsum(ret))
    synth_ret = aligned_returns["USD_SYNTH_RET"]
    synth_close = BASE_VALUE * np.exp(synth_ret.cumsum())
    
    # 6. Validation (Fail Hard)
    validate_outputs(aligned_returns, synth_ret, synth_close)
    
    # 7. Write Outputs
    output_dir = os.path.join(data_root, OUTPUT_DIR_REL)
    os.makedirs(output_dir, exist_ok=True)
    
    # Returns
    ret_df = synth_ret.to_frame(name="USD_SYNTH_RET_D1")
    ret_path = os.path.join(output_dir, "usd_synth_return_d1.csv")
    ret_df.to_csv(ret_path)
    logger.info(f"Wrote returns to {ret_path}")
    
    # Close
    close_df = synth_close.to_frame(name="USD_SYNTH_CLOSE_D1")
    close_path = os.path.join(output_dir, "usd_synth_close_d1.csv")
    close_df.to_csv(close_path)
    logger.info(f"Wrote close to {close_path}")
    
    # Metadata
    metadata = {
        "name": "USD_SYNTH",
        "version": "USD_SYNTH_D1_v1.0",
        "timeframe": REQUIRED_TIMEFRAME,
        "return_type": "log",
        "base_value": BASE_VALUE,
        "basket": PAIRS,
        "direction_rule": {
            "invert": INVERT_PAIRS,
            "keep": KEEP_PAIRS
        },
        "aggregation": "equal_weight_mean",
        "calendar_rule": "intersection_only",
        "source_data_hashes": all_hashes,
        "creation_timestamp": datetime.utcnow().isoformat() + "Z",
        "validation_stats": {
            "start_date": common_index[0].strftime("%Y-%m-%d"),
            "end_date": common_index[-1].strftime("%Y-%m-%d"),
            "count": len(common_index),
            "synth_vol_annualized": float(synth_ret.std() * np.sqrt(252)),
            "median_component_vol": float(aligned_returns[PAIRS].std().median() * np.sqrt(252))
        }
    }
    
    meta_path = os.path.join(output_dir, "metadata.json")
    with open(meta_path, "w") as f:
        json.dump(metadata, f, indent=2)
    logger.info(f"Wrote metadata to {meta_path}")
    
    print("\nSUCCESS: USD Synth Construction Complete.")

def validate_outputs(aligned_df, synth_ret, synth_close):
    logger.info("Running Mandatory Validations...")
    
    # Check 1: NaNs
    if synth_ret.isnull().any() or synth_close.isnull().any():
        logger.error("Validation Failed: NaNs found in output.")
        sys.exit(1)
        
    # Check 2: Volatility Reduction
    # Synth vol should be < median component vol (diversification effect)
    # Using realized volatility of the whole series
    comp_vols = aligned_df[PAIRS].std()
    median_vol = comp_vols.median()
    synth_vol = synth_ret.std()
    
    logger.info(f"Synth Vol: {synth_vol:.6f}, Median Component Vol: {median_vol:.6f}")
    
    if synth_vol >= median_vol:
        logger.error("Validation Failed: Synthetic volatility is not lower than median component volatility.")
        sys.exit(1)
        
    # Check 3: Sanity (Max Return)
    # No single-day return exceeds max(component daily return) * 1.1
    # We check row by row? Or global max?
    # "No single-day return exceeds max(component daily return) × 1.1" -> Implies strictly checking the worst case day?
    # Actually, it usually means the *global* max return of synth shouldn't be crazy compared to global max of components.
    # But let's be strict: For every day t: abs(Synth_t) < max(abs(Comp_i,t)) * 1.1 ??
    # A diversified basket *can* never theoretically exceed the max component return magnitude on a specific day if equal weighted.
    # Because mean(x) <= max(x). Equal weight mean is strictly bounded by min and max of components.
    # So if Synth > Max Component, math is wrong.
    # Wait, simple mean: |mean(x)| <= mean(|x|) <= max(|x|).
    # So Synth Return magnitude CANNOT exceed the max component return magnitude for that day. 
    # The 1.1 buffer is generous. If it exceeds, we interpret as a Calculation Error.
    
    # Let's check this row-wise constraint
    comps_abs = aligned_df[PAIRS].abs().max(axis=1)
    synth_abs = synth_ret.abs()
    
    # Check if any day violates
    violations = synth_abs > (comps_abs * 1.000001) # Floating point tolerance
    if violations.any():
        # This is mathematically impossible for an arithmetic mean unless normalizing logic is wrong.
        # However, we are in log returns. Mean of log returns = log of geometric mean of prices. 
        # Linearity holds for log returns.
        logger.error("Validation Failed: Synthetic return exceeds max component return (mathematically impossible for mean).")
        logger.error(aligned_df[violations].head())
        sys.exit(1)
        
    # Check 4: Correlation
    # No single pair has persistent |corr| approx 1.0
    # Check global correlation
    corrs = aligned_df[PAIRS].corrwith(synth_ret)
    logger.info(f"Correlations with Synth:\n{corrs}")
    
    if (corrs.abs() > 0.98).any():
        logger.error("Validation Failed: A single component has correlation > 0.98 with Synth. Too dominant.")
        sys.exit(1)

    logger.info("All Validations Passed.")

if __name__ == "__main__":
    project_root = get_project_root()
    data_root = find_data_root(project_root)
    
    if not data_root:
        logger.error(f"Could not find Data Root: {DATA_ROOT_NAME}")
        sys.exit(1)
        
    construction_logic(project_root, data_root)

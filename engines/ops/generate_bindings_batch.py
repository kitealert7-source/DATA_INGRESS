
import json
import os
import sys
import glob

# --- CONFIG ---
BCSL = {
    "XAUUSD_OCTAFX":  {"contract_value": 100,   "min_unit": 0.01, "lot_step": 0.01},
    "BTC_OCTAFX":     {"contract_value": 1,     "min_unit": 0.01, "lot_step": 0.01}, # Mapping internal
    "ETH_OCTAFX":     {"contract_value": 10,    "min_unit": 0.01, "lot_step": 0.01}, # Mapping internal
    "BTC_DELTA":      {"contract_value": 0.001, "min_unit": 1,    "lot_step": 1},
    "ETH_DELTA":      {"contract_value": 0.01,  "min_unit": 1,    "lot_step": 1},
}

# Mapping aliases for filenames
ASSET_ALIAS = {
    "ETH_OCTAFX": "ETHUSD",
    "BTC_OCTAFX": "BTCUSD",
    "XAUUSD_OCTAFX": "XAUUSD"
}

def find_research_file(asset, feed, timeframe):
    # Construct search pattern
    # Pattern: MASTER_DATA/<ASSET>_<FEED>_MASTER/RESEARCH/*<TF>*.csv
    # Need to handle aliases like ETHUSD for OCTAFX
    
    # Base folder
    # Try generic folder structure first: MASTER_DATA/{ASSET}_{FEED}_MASTER
    # Note: user might pass 'BTC' but folder is 'BTC_OCTAFX_MASTER'
    
    base_asset = asset
    master_folder = f"MASTER_DATA/{asset}_{feed}_MASTER/RESEARCH"
    
    if not os.path.exists(master_folder):
        print(f"  [Warn] Master folder not found: {master_folder}")
        return None, None

    # Filename search
    # Heuristic: Look for *_<TF>_*.csv or *_<TF>m_*.csv
    # Also handle 'ETHUSD' inside filename even if asset is 'ETH'
    
    search_patterns = [
        f"*{timeframe}*.csv",
        f"*{timeframe}m*.csv"
    ]
    
    for pat in search_patterns:
        full_pat = os.path.join(master_folder, pat)
        matches = glob.glob(full_pat)
        # Filter for "RESEARCH" (already in path, but double check)
        matches = [m for m in matches if "RESEARCH" in m]
        # Prefer 2025
        matches_2025 = [m for m in matches if "2025" in m]
        
        if matches_2025:
            return matches_2025[0], master_folder
        if matches:
            return matches[0], master_folder
            
    return None, None

def generate_binding_file(asset, feed, strategy, timeframe, params_file):
    print(f"Generating Binding: {asset} {feed} {timeframe}...")
    
    # 1. Load Params
    if not os.path.exists(params_file):
        print("  [Error] Params file not found")
        return
        
    with open(params_file, 'r') as f:
        params = json.load(f)

    # 2. Resolve BCSL
    bcsl_key = f"{asset}_{feed}"
    
    # Handle implicit 'USD' for OctaFX keys in manual BCSL dict
    # My internal logic usually uses clean keys XAUUSD_OCTAFX, BTC_DELTA
    if bcsl_key not in BCSL:
        # Try appending USD for OctaFX if not present
        if feed == "OCTAFX" and "USD" not in asset:
             # Try ETH -> ETH_OCTAFX (mapped above) .. wait.
             # My BCSL dict above has ETH_OCTAFX.
             pass
    
    if bcsl_key not in BCSL:
        print(f"  [Error] No BCSL data for {bcsl_key}")
        return

    spec = BCSL[bcsl_key]
    
    # 3. Find Data
    csv_path, folder_path = find_research_file(asset, feed, timeframe)
    if not csv_path:
        print(f"  [Error] No RESEARCH data found for {asset} {feed} {timeframe}")
        return
        
    csv_name = os.path.basename(csv_path)
    # Correct path for json (absolute or relative to workflow?) 
    # Usually absolute path in input_data_path
    abs_folder_path = os.path.abspath(folder_path)
    
    # 4. Create Binding Content
    binding = {
        "RUN_BINDING": {
            "asset": asset,
            "feed": feed,
            "strategy": strategy,
            "timeframe": timeframe,
            
            "input_data_path": abs_folder_path,
            "input_files": [csv_name],
            
            "initial_capital": 10000,
            
            "contract_value": spec["contract_value"],
            "minimum_tradable_unit": spec["min_unit"],
            "lot_step": spec["lot_step"],
            "lot_size": spec["min_unit"], # SOP v17 Strict
            
            "pyramiding": 0,
            "max_positions": 1,
            
            "parameters": params,
            "sop_version": "v17",
            
            "output_run_name": f"{asset}_{timeframe}_{strategy}_{feed}_Run"
        }
    }
    
    # 5. Save
    filename = f"{strategy}_{asset}_{feed}_{timeframe}_v17_binding.json"
    out_path = os.path.join("RUN_BINDINGS", filename)
    
    with open(out_path, 'w') as f:
        json.dump(binding, f, indent=4)
        
    print(f"  [Success] Saved to {out_path}")

# --- MAIN EXECUTION ---
if __name__ == "__main__":
    # Batch Requests
    requests = [
        ("XAUUSD", "OCTAFX", "HULLHKN", "1m"),
        ("XAUUSD", "OCTAFX", "HULLHKN", "3m"),
        ("XAUUSD", "OCTAFX", "HULLHKN", "5m"),
        ("ETH", "OCTAFX", "HULLHKN", "5m"),
        ("ETH", "DELTA", "HULLHKN", "5m"),
        ("BTC", "DELTA", "HULLHKN", "5m")
    ]
    
    for r in requests:
        generate_binding_file(r[0], r[1], r[2], r[3], "params.json")

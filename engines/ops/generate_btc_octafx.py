
import json
import os
import sys
import glob

# --- CONFIG ---
BCSL = {
    "XAUUSD_OCTAFX":  {"contract_value": 100,   "min_unit": 0.01, "lot_step": 0.01},
    "BTC_OCTAFX":     {"contract_value": 1,     "min_unit": 0.01, "lot_step": 0.01}, 
    "ETH_OCTAFX":     {"contract_value": 10,    "min_unit": 0.01, "lot_step": 0.01},
    "BTC_DELTA":      {"contract_value": 0.001, "min_unit": 1,    "lot_step": 1},
    "ETH_DELTA":      {"contract_value": 0.01,  "min_unit": 1,    "lot_step": 1},
}

def find_research_file(asset, feed, timeframe):
    master_folder = f"MASTER_DATA/{asset}_{feed}_MASTER/RESEARCH"
    
    if not os.path.exists(master_folder):
        print(f"  [Warn] Master folder not found: {master_folder}")
        return None, None

    search_patterns = [
        f"*{timeframe}*.csv",
        f"*{timeframe}m*.csv"
    ]
    
    for pat in search_patterns:
        full_pat = os.path.join(master_folder, pat)
        matches = glob.glob(full_pat)
        matches = [m for m in matches if "RESEARCH" in m]
        matches_2025 = [m for m in matches if "2025" in m]
        
        if matches_2025:
            return matches_2025[0], master_folder
        if matches:
            return matches[0], master_folder
            
    return None, None

def generate_binding_file(asset, feed, strategy, timeframe, params_file):
    print(f"Generating Binding: {asset} {feed} {timeframe}...")
    
    if not os.path.exists(params_file):
        print("  [Error] Params file not found")
        return
        
    with open(params_file, 'r') as f:
        params = json.load(f)

    bcsl_key = f"{asset}_{feed}"
    
    if bcsl_key not in BCSL:
        # Fallback for BTCUSD_OCTAFX if asset is passed as BTC
        if asset == "BTC" and feed == "OCTAFX":
             # The key in BCSL is BTC_OCTAFX, matches.
             pass
        else:
             print(f"  [Error] No BCSL data for {bcsl_key}")
             return

    spec = BCSL[bcsl_key]
    
    csv_path, folder_path = find_research_file(asset, feed, timeframe)
    if not csv_path:
        print(f"  [Error] No RESEARCH data found for {asset} {feed} {timeframe}")
        return
        
    csv_name = os.path.basename(csv_path)
    abs_folder_path = os.path.abspath(folder_path)
    
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
            "lot_size": spec["min_unit"],
            "pyramiding": 0,
            "max_positions": 1,
            "parameters": params,
            "sop_version": "v17",
            "output_run_name": f"{asset}_{timeframe}_{strategy}_{feed}_Run"
        }
    }
    
    filename = f"{strategy}_{asset}_{feed}_{timeframe}_v17_binding.json"
    out_path = os.path.join("RUN_BINDINGS", filename)
    
    with open(out_path, 'w') as f:
        json.dump(binding, f, indent=4)
        
    print(f"  [Success] Saved to {out_path}")

if __name__ == "__main__":
    generate_binding_file("BTC", "OCTAFX", "HULLHKN", "5m", "params.json")

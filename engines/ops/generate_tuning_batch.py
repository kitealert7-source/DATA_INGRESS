
import os
import json
import itertools
import hashlib
import argparse
from datetime import datetime

# SOP v1 HULLHKN Phase 1 Ranges
# hma_len: [8, 10, 12]
# dist_thresh_pct: [0.28, 0.35, 0.42]
# ha_streak: [1, 2, 3] # SOP name
# htf_ha_profit_threshold: [1.6, 2.0, 2.4] # SOP name

# SOP v1 HULLHKN Phase 1 Ranges
# ... (Ranges remain same)

PHASE1_RANGES = {
    "hma_len": [8, 10, 12],
    "dist_thresh_pct": [0.28, 0.35, 0.42],
    "ha_streak": [1, 2, 3],
    "htf_ha_profit_threshold": [1.6, 2.0, 2.4]
}

# ... (Baseline params remain same)

BASELINE_PARAMS = {
    "hma_timeframe": "30m",
    "htf_slope_timeframe": "15m",
    "ha_timeframe": "6m",
    "htf_ha_exit_timeframe": "12m",
    "use_distance_filter": True,
    "use_slope_filter": True,
    "use_htf_slope_filter": True,
    "use_ha_filter": True,
    "use_smart_filters": True,
    "use_htf_ha_exit": True,
    "htf_dead_zone_int": 5,
    "anti_flip_min_pct": 1.0,
    "anti_flip_bars": 30
}

def generate_tuning_batch(strategy_name, phase):
    print(f"Generating {phase} Tuning Batch for {strategy_name}...")
    
    if strategy_name != "HULLHKN":
        print("Only HULLHKN supported currently.")
        return
        
    if phase != "P1":
        print("Only Phase 1 supported currently.")
        return

    # Metadata SHA Calculations
    # Strategy File
    strategy_file = os.path.join(os.getcwd(), "strategies", "hullhkn.py")
    if not os.path.exists(strategy_file):
        print("ERROR: Strategy file strategies/hullhkn.py not found.")
        return
        
    with open(strategy_file, 'rb') as f:
        strat_sha = hashlib.sha256(f.read()).hexdigest()
        
    # Dataset File
    # Using verified path
    dataset_rel_path = os.path.join("MASTER_DATA", "XAUUSD_OCTAFX_MASTER", "RESEARCH", "XAUUSD_5m_2025_MT5_RESEARCH.csv")
    from scripts.utils.path_config import GET_DATA_ROOT
    dataset_path = os.path.join(GET_DATA_ROOT(), dataset_rel_path)
    
    # Validation: Dataset Availability
    if not os.path.exists(dataset_path):
        dataset_rel_path = os.path.join("MASTER_DATA", "XAUUSD_OCTAFX_MASTER", "RESEARCH", "XAUUSD_5m_2024_MT5_RESEARCH.csv")
        dataset_path = os.path.join(GET_DATA_ROOT(), dataset_rel_path)
        if not os.path.exists(dataset_path):
            print(f"ERROR: Dataset not found at {dataset_path}")
            return

    with open(dataset_path, 'rb') as f:
        data_sha = hashlib.sha256(f.read()).hexdigest()

    # Determine Test Period
    year = "2025" if "2025" in dataset_path else "2024"
    test_start = f"{year}-01-01"
    test_end = f"{year}-12-31"
    
    # Forward Test Metadata
    forward_test = {
        "window_days": 30,
        "anchor_end": test_end
    }
    
    # Validation: Alignment
    if forward_test["anchor_end"] != test_end:
         print("ERROR: Forward test anchor does not align with test period end.")
         return

    # 1. Generate Combinations
    keys = list(PHASE1_RANGES.keys())
    values = list(PHASE1_RANGES.values())
    combinations = list(itertools.product(*values))
    
    print(f"Generated {len(combinations)} combinations.")
    
    base_output_dir = os.path.join(os.getcwd(), "STRATEGIES", strategy_name, "TUNING", phase)
    if not os.path.exists(base_output_dir):
        os.makedirs(base_output_dir, exist_ok=True)

    # 2. Iterate and Create Bindings
    for i, combo in enumerate(combinations):
        run_id = f"RUN_{i+1:03d}"
        run_dir = os.path.join(base_output_dir, run_id)
        if not os.path.exists(run_dir):
            os.makedirs(run_dir, exist_ok=True)
            
        params = BASELINE_PARAMS.copy()
        combo_map = dict(zip(keys, combo))
        params.update(combo_map)
        
        binding = {
            "RUN_BINDING": {
                "strategy": strategy_name,
                "asset": "XAUUSD",
                "feed": "OCTAFX",
                "timeframe": "5m",
                "input_data_path": os.path.dirname(dataset_path),
                "input_files": [os.path.basename(dataset_path)],
                "initial_capital": 10000,
                "contract_value": 100,
                "minimum_tradable_unit": 0.01,
                "lot_step": 0.01,
                "lot_size": 0.01,
                "pyramiding": 0,
                "max_positions": 1,
                "parameters": params,
                "sop_version": "v17",
                "tuning_sop_version": "v1",
                "tuning_phase": phase,
                "output_run_name": f"{strategy_name}_{phase}_{run_id}",
                
                # SOP v1 Compliance
                "test_period": {
                    "start": test_start,
                    "end": test_end
                },
                "forward_test": forward_test,
                "strategy_sha": strat_sha,
                "dataset_sha": data_sha
            }
        }
        
        binding_path = os.path.join(run_dir, "binding.json")
        encoded_json = json.dumps(binding, indent=4)
        with open(binding_path, 'w') as f:
            f.write(encoded_json)
            
    print(f"Successfully generated {len(combinations)} bindings in {base_output_dir}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--strategy", required=True)
    parser.add_argument("--phase", required=True)
    args = parser.parse_args()
    
    generate_tuning_batch(args.strategy, args.phase)

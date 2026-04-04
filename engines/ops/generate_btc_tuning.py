
import os
import json
import itertools
import hashlib
from datetime import datetime

# BTC Tuning Configuration
BASELINE_FILE = os.path.join("strategies", "HULLHKN", "baseline_params.json")
DATASET_PATH = os.path.join(os.getcwd(), "MASTER_DATA", "BTCUSD_OCTAFX_MASTER", "RESEARCH", "BTCUSD_3m_2025_MT5_RESEARCH.csv")
STRATEGY_FILE = os.path.join("strategies", "hullhkn.py")

# Tuning Metadata
TEST_PERIOD = {
    "start": "2025-05-09",
    "end": "2025-12-07"
}
FORWARD_TEST = {
    "window_days": 30,
    "anchor_end": "2025-12-07"
}

def load_baseline():
    if not os.path.exists(BASELINE_FILE):
        raise FileNotFoundError(f"Baseline file not found: {BASELINE_FILE}")
    with open(BASELINE_FILE, 'r') as f:
        return json.load(f)

def calculate_ranges(baseline):
    # hma_len: [0.8x, 1.0x, 1.2x]
    base_hma = baseline.get("hma_len", 200)
    range_hma = sorted(list(set([
        int(base_hma * 0.8),
        int(base_hma),
        int(base_hma * 1.2)
    ])))
    
    # dist_thresh_pct: [0.8x, 1.0x, 1.2x]
    base_dist = baseline.get("dist_thresh_pct", 0.2)
    range_dist = sorted(list(set([
        round(base_dist * 0.8, 3),
        round(base_dist, 3),
        round(base_dist * 1.2, 3)
    ])))

    # ha_streak: [base-1, base, base+1], bounded [1, 6]
    base_streak = baseline.get("ha_streak", 2)
    start = max(1, base_streak - 1)
    end = min(6, base_streak + 1)
    range_streak = sorted(list(set([i for i in range(start, end + 1)])))

    # htf_ha_profit_threshold: [0.8x, 1.0x, 1.2x]
    base_profit = baseline.get("htf_ha_profit_threshold", 3.5)
    range_profit = sorted(list(set([
        round(base_profit * 0.8, 2),
        round(base_profit, 2),
        round(base_profit * 1.2, 2)
    ])))

    return {
        "hma_len": range_hma,
        "dist_thresh_pct": range_dist,
        "ha_streak": range_streak,
        "htf_ha_profit_threshold": range_profit
    }

def generate_btc_bindings():
    print("Generating BTC Tuning Phase 1 Bindings...")
    
    # 1. Load Baseline & Calc Ranges
    baseline = load_baseline()
    ranges = calculate_ranges(baseline)
    print(f"Ranges Derived: {ranges}")
    
    keys = list(ranges.keys())
    values = list(ranges.values())
    combinations = list(itertools.product(*values))
    print(f"Total Combinations: {len(combinations)}")
    
    # 2. SHA Calcs
    with open(STRATEGY_FILE, 'rb') as f:
        strat_sha = hashlib.sha256(f.read()).hexdigest()
    with open(DATASET_PATH, 'rb') as f:
        data_sha = hashlib.sha256(f.read()).hexdigest()
        
    # 3. Output Dir
    base_output_dir = os.path.join(os.getcwd(), "STRATEGIES", "HULLHKN", "TUNING", "BTC", "P1")
    if not os.path.exists(base_output_dir):
        os.makedirs(base_output_dir, exist_ok=True)
        
    # 4. Generate
    for i, combo in enumerate(combinations):
        run_id = f"RUN_{i+1:03d}"
        run_dir = os.path.join(base_output_dir, run_id)
        if not os.path.exists(run_dir):
            os.makedirs(run_dir, exist_ok=True)
            
        # Params: Start with baseline, update with combo
        params = baseline.copy()
        combo_map = dict(zip(keys, combo))
        params.update(combo_map)
        
        binding = {
            "RUN_BINDING": {
                "strategy": "HULLHKN",
                "asset": "BTCUSD",
                "feed": "OCTAFX",
                "timeframe": "3m",
                "input_data_path": os.path.dirname(DATASET_PATH),
                "input_files": [os.path.basename(DATASET_PATH)],
                
                # BCSL for BTC (Using generic config, user didn't specify, assuming 1 contract)
                "initial_capital": 10000,
                "contract_value": 1, # BTCUSD usually 1
                "minimum_tradable_unit": 0.01,
                "lot_step": 0.01,
                "lot_size": 0.01,
                "pyramiding": 0,
                "max_positions": 1,
                
                "parameters": params,
                "sop_version": "v17",
                "tuning_sop_version": "v1",
                "tuning_phase": "P1",
                "output_run_name": f"HULLHKN_BTC_P1_{run_id}",
                
                "test_period": TEST_PERIOD,
                "forward_test": FORWARD_TEST,
                "strategy_sha": strat_sha,
                "dataset_sha": data_sha
            }
        }
        
        with open(os.path.join(run_dir, "binding.json"), 'w') as f:
            json.dump(binding, f, indent=4)
            
    print(f"Success. Bindings written to {base_output_dir}")

if __name__ == "__main__":
    generate_btc_bindings()

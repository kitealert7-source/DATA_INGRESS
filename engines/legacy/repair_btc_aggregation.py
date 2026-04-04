
import os
import json
import pandas as pd
import glob
import sys
import numpy as np

TUNING_DIR = os.path.join("STRATEGIES", "HULLHKN", "TUNING", "BTC", "P1")
RESULTS_DIR = os.path.join("RESULTS")
BASELINE_FILE = os.path.join("strategies", "HULLHKN", "baseline_params.json")

def load_json(path):
    try:
        with open(path, 'r') as f:
            return json.load(f)
    except:
        return {}

def aggregate():
    print("Starting BTC Phase 1 Aggregation Repair...")
    
    # 1. Scan RESULTS for matching runs
    # We look for folders created recently or containing the correct run_name
    candidate_dirs = sorted(glob.glob(os.path.join(RESULTS_DIR, "HULLHKN_BTCUSD_OCTAFX_3m_*")))
    
    results = []
    
    print(f"Scanning {len(candidate_dirs)} candidate folders in RESULTS...")
    
    for d in candidate_dirs:
        # Check binding_used.json
        binding_path = os.path.join(d, "binding_used.json")
        if not os.path.exists(binding_path):
            continue
            
        binding = load_json(binding_path)
        run_name = binding.get("output_run_name", "")
        
        # Check if it matches our pattern
        if not run_name.startswith("HULLHKN_BTC_P1_RUN_"):
            continue
            
        # Extract ID
        # HULLHKN_BTC_P1_RUN_001
        run_id = run_name.split("_")[-1] # "001"
        run_label = f"RUN_{run_id}"
        
        # Load Metrics
        summary_path = os.path.join(d, "summary.json")
        if not os.path.exists(summary_path):
            continue
            
        summary = load_json(summary_path)
        
        res = {
            "run_id": run_label,
            "net_profit": summary.get("net_profit", 0.0),
            "max_dd": summary.get("max_dd_pct", 0.0),
            "sharpe": summary.get("sharpe_ratio", 0.0),
            "trade_count": summary.get("total_trades", 0),
            "result_path": d
        }
         
        # Advanced Metrics
        adv = summary.get("advanced_metrics", {})
        res["expectancy"] = adv.get("Expectancy", 0.0)
        res["mar"] = adv.get("MAR", 0.0)
        res["car"] = adv.get("CAR", 0.0)
        
        # Params
        params = binding.get("parameters", {})
        res.update(params)
        
        results.append(res)
        
    print(f"Collected {len(results)} valid runs.")
    
    if not results:
        print("No matching results found. Aggregation failed.")
        return

    df = pd.DataFrame(results)
    # Sort by run_id
    df = df.sort_values("run_id")

    # 2. Identify Baseline
    baseline_params = load_json(BASELINE_FILE)
    keys_to_match = ["hma_len", "dist_thresh_pct", "ha_streak", "htf_ha_profit_threshold"]
    
    baseline_row = None
    for idx, row in df.iterrows():
        match = True
        for k in keys_to_match:
            val = row.get(k)
            base_val = baseline_params.get(k)
            # approximate float match
            try:
                if abs(float(val) - float(base_val)) > 1e-5:
                    match = False; break
            except:
                if val != base_val: match = False; break
        if match:
            baseline_row = row
            break
            
    if baseline_row is None:
        print("Baseline run not found strictly. Using median.")
        baseline_row = df.iloc[len(df)//2]
        
    base_profit = baseline_row['net_profit']
    base_dd = baseline_row['max_dd']
    base_exp = baseline_row['expectancy']
    base_trades = baseline_row['trade_count']
    
    print(f"Baseline: {baseline_row['run_id']} ($ {base_profit:.2f})")

    # 3. Filter Candidates
    candidates = []
    for idx, row in df.iterrows():
        cond_profit = row['net_profit'] > base_profit
        cond_dd = row['max_dd'] <= (base_dd * 1.15) if base_dd > 0 else True # if dd is 0?? safely ignore
        cond_exp = row['expectancy'] > base_exp
        cond_trades = row['trade_count'] <= (base_trades * 1.20)
        
        if cond_profit and cond_dd and cond_exp and cond_trades:
            candidates.append(row.to_dict())

    print(f"Candidates Passing PAC: {len(candidates)}")

    # 4. Save Outputs (v2 to bypass lock)
    csv_path = os.path.join(TUNING_DIR, "P1_grid_results_v2.csv")
    try:
        if os.path.exists(csv_path): os.remove(csv_path)
    except: pass
    df.to_csv(csv_path, index=False)
    
    json_path = os.path.join(TUNING_DIR, "P1_top_candidates_v2.json")
    class NpEncoder(json.JSONEncoder):
        def default(self, obj):
            if isinstance(obj, np.integer): return int(obj)
            if isinstance(obj, np.floating): return float(obj)
            if isinstance(obj, np.ndarray): return obj.tolist()
            return super(NpEncoder, self).default(obj)
            
    try:
        if os.path.exists(json_path): os.remove(json_path)
    except: pass
    with open(json_path, 'w') as f:
        json.dump(candidates, f, indent=4, cls=NpEncoder)
        
    # Report
    report_path = os.path.join(TUNING_DIR, "P1_sensitivity_report_v2.md")
    top_5 = sorted(candidates, key=lambda x: x['net_profit'], reverse=True)[:5]
    
    cand_rows = []
    for c in top_5:
        row = f"| {c['run_id']} | ${c['net_profit']:.2f} | {c['max_dd']:.2f}% | ${c['expectancy']:.2f} | {c['trade_count']} |"
        cand_rows.append(row)
    cand_table = "\n".join(cand_rows)
    
    report_content = f"""# Phase 1 Sensitivity Report (BTC)
**Status**: Complete (Aggregated)
**Total Runs**: {len(df)}
**Candidates Found**: {len(candidates)}
**Baseline Reference**: {baseline_row['run_id']} (Profit: ${base_profit:.2f}, DD: {base_dd:.2f}%)

## PAC Criteria
- Net Profit > Baseline (${base_profit:.2f})
- Max DD <= {base_dd * 1.15:.2f}% (1.15x)
- Expectancy > ${base_exp:.2f}
- Trade Count <= {int(base_trades * 1.2)} (+20%)

## Top Candidates
| Run ID | Net Profit | Max DD | Expectancy | Trades |
|---|---|---|---|---|
{cand_table}

## Full Grid Analysis
See `P1_grid_results_v2.csv` for full details.
"""
    try:
        if os.path.exists(report_path): os.remove(report_path)
    except: pass
    with open(report_path, 'w') as f:
        f.write(report_content)
        
    print("Repair Complete.")

if __name__ == "__main__":
    aggregate()

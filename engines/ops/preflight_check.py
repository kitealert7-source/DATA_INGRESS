"""
Anti-Gravity Preflight Agent Implementation
Authoritative Decision Logic & Audit Logging
"""

import os
import json
import datetime
import sys
from pathlib import Path

# Constants
DATA_ROOT = str(Path(__file__).resolve().parents[3] / "Anti_Gravity_DATA_ROOT")
GOVERNANCE_PATH = os.path.join(DATA_ROOT, "governance", "last_successful_daily_run.json")
DATA_INGRESS = str(Path(__file__).resolve().parents[2])
LOG_DIR = os.path.join(DATA_INGRESS, "logs", "PREFLIGHT")
DECISION_FILE = os.path.join(DATA_INGRESS, "PREFLIGHT_DECISION.json")

def get_utc_now():
    return datetime.datetime.now(datetime.timezone.utc)

def load_governance():
    if not os.path.exists(GOVERNANCE_PATH):
        return None
    try:
        with open(GOVERNANCE_PATH, 'r') as f:
            return json.load(f)
    except Exception:
        return None

def write_audit_log(decision, reason, gov_state, trigger="MANUAL"):
    now = get_utc_now()
    filename = f"preflight_{now.strftime('%Y%m%d_%H%M%S')}.log"
    path = os.path.join(LOG_DIR, filename)
    
    last_run = gov_state.get("last_run_date", "UNKNOWN") if gov_state else "MISSING"
    
    log_content = (
        f"timestamp_utc: {now.isoformat()}\n"
        f"executor: SYSTEM\n"
        f"trigger: {trigger}\n"
        f"decision: {decision}\n"
        f"reason: {reason}\n"
        f"last_successful_run_date: {last_run}\n"
        f"governance_state_path: {GOVERNANCE_PATH}\n"
        f"preflight_version: v1\n"
    )
    
    try:
        with open(path, 'w') as f:
            f.write(log_content)
        print(f"Audit log written: {path}")
    except Exception as e:
        print(f"CRITICAL: Failed to write audit log. {e}")
        sys.exit(1)

def write_decision_token(decision, reason):
    now = get_utc_now()
    data = {
        "decision": decision,
        "reason": reason,
        "timestamp_utc": now.isoformat(),
        "date_utc": now.strftime("%Y-%m-%d")
    }
    with open(DECISION_FILE, 'w') as f:
        json.dump(data, f, indent=2)
    print(f"Decision token emitted: {DECISION_FILE}")

def main():
    print("Preflight Agent: initializing...")
    
    # 1. Load Governance
    gov = load_governance()
    
    # 2. Logic
    if gov is None:
        decision = "HARD_STOP"
        reason = "Governance state missing or invalid"
    elif gov.get("status") != "SUCCESS":
        decision = "RUN_RECOVERY"
        reason = f"Last run status not SUCCESS ({gov.get('status')})"
    else:
        last_run_date = gov.get("last_run_date")
        today = get_utc_now().strftime("%Y-%m-%d")
        
        # Parse dates to compare logic if needed, but string comparison works for ISO YYYY-MM-DD
        if last_run_date == today:
            decision = "NO_ACTION"
            reason = f"Daily run already completed for {today}"
        else:
            # Check if yesterday
            last_date_obj = datetime.datetime.strptime(last_run_date, "%Y-%m-%d").date()
            today_date_obj = get_utc_now().date()
            delta = (today_date_obj - last_date_obj).days
            
            if delta == 1:
                decision = "RUN_DAILY"
                reason = "Last run was yesterday; authorized for today"
            elif delta > 1:
                decision = "RUN_RECOVERY"
                reason = f"Gap detected: Last run {last_run_date}, Today {today} (Delta {delta} days)"
            else:
                 # Future date?
                decision = "HARD_STOP"
                reason = f"Governance date {last_run_date} is in the future vs {today}"

    # 3. Emit Outputs
    print(f"Decision: {decision} | Reason: {reason}")
    write_decision_token(decision, reason)
    write_audit_log(decision, reason, gov, trigger="MANUAL")

if __name__ == "__main__":
    main()

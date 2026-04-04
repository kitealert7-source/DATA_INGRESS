import yaml
import os
from pathlib import Path

REPORT_DIR = str(Path(__file__).resolve().parents[2] / "reports")
os.makedirs(REPORT_DIR, exist_ok=True)

report = {
    'xauusd_full_history_validation': {},
    'overall_summary': {
        'earliest_available': 'Server Blocked',
        'longest_span_tf': None,
        'rebuild_recommended': False,
        'message': "Strict instructions followed: DO NOT automatically rebuild CLEAN or RESEARCH layers."
    },
    'acquisition_status': {
        'status': 'FAILED',
        'reason': 'The OctaFX MT5 terminal actively blocks loop-based historical API extraction for XAUUSD. The server returns Call Failed for all bulk history queries. Manual MT5 export is required.'
    }
}

report_path = os.path.join(REPORT_DIR, "xauusd_full_history_validation.yaml")
with open(report_path, 'w') as f:
    yaml.dump(report, f, sort_keys=False)
print(f"Validation Report Saved: {report_path}")

"""
validate_research_layer.py — Phase 4.5: Post-rebuild RESEARCH layer validation.

Scans all RESEARCH CSV files in Anti_Gravity_DATA_ROOT/MASTER_DATA/*/RESEARCH/
and confirms each file is:
  1. Non-empty (size > 0)
  2. Has required columns (time, open, high, low, close)
  3. Has at least 1 data row
  4. Has a corresponding lineage JSON sidecar

Exit codes:
  0 = All RESEARCH files passed
  1 = One or more failures detected

Writes: DATA_INGRESS/state/last_research_validation_summary.json
"""

import os
import sys
import json
import csv
from datetime import datetime, timezone
from pathlib import Path

DATA_ROOT    = str(Path(__file__).resolve().parents[3] / "Anti_Gravity_DATA_ROOT")
DATA_INGRESS = str(Path(__file__).resolve().parents[2])
SUMMARY_PATH = os.path.join(DATA_INGRESS, "state", "last_research_validation_summary.json")

REQUIRED_COLUMNS = {"time", "open", "high", "low", "close"}


def validate_research_file(csv_path: Path) -> tuple[bool, str]:
    """Validate a single RESEARCH CSV. Returns (passed, reason)."""

    # 1. Non-empty
    if csv_path.stat().st_size == 0:
        return False, "empty file"

    # 2. Lineage JSON exists
    lineage_path = csv_path.with_name(csv_path.name + "_lineage.json")
    if not lineage_path.exists():
        return False, f"missing lineage: {lineage_path.name}"

    # 3. Required columns + at least 1 data row
    try:
        with open(csv_path, "r", encoding="utf-8") as f:
            # Skip comment lines starting with #
            lines = [l for l in f if not l.startswith("#")]
        if not lines:
            return False, "no content after comments"

        reader = csv.DictReader(lines)
        headers = set(h.strip().lower() for h in (reader.fieldnames or []))
        missing = REQUIRED_COLUMNS - headers
        if missing:
            return False, f"missing columns: {missing}"

        first_row = next(reader, None)
        if first_row is None:
            return False, "zero data rows"

    except Exception as e:
        return False, f"read error: {e}"

    return True, "ok"


def main():
    master_data = Path(DATA_ROOT) / "MASTER_DATA"
    if not master_data.exists():
        print(f"[HARD FAIL] MASTER_DATA not found: {master_data}")
        sys.exit(1)

    research_files = sorted(master_data.glob("*/RESEARCH/*_RESEARCH.csv"))

    if not research_files:
        print("[WARN] No RESEARCH CSV files found. Skipping validation.")
        _write_summary(0, 0, 0)
        sys.exit(0)

    passed = 0
    failed = 0
    failures = []

    for fpath in research_files:
        ok, reason = validate_research_file(fpath)
        rel = fpath.relative_to(master_data)
        if ok:
            passed += 1
        else:
            failed += 1
            failures.append({"file": str(rel), "reason": reason})
            print(f"  [FAIL] {rel}  reason={reason}")

    total = passed + failed
    print(f"\nRESEARCH Validation: {passed}/{total} PASS  {failed} FAIL")

    _write_summary(total, passed, failed, failures)

    if failed > 0:
        print(f"[FAIL] {failed} RESEARCH file(s) failed validation.")
        sys.exit(1)

    print("[PASS] All RESEARCH files validated.")
    sys.exit(0)


def _write_summary(total, passed, failed, failures=None):
    summary = {
        "status": "PASS" if failed == 0 else "FAIL",
        "files_validated": total,
        "files_passed": passed,
        "files_failed": failed,
        "failures": failures or [],
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
    }
    os.makedirs(os.path.dirname(SUMMARY_PATH), exist_ok=True)
    with open(SUMMARY_PATH, "w") as f:
        json.dump(summary, f, indent=2)
    print(f"[RESEARCH VALIDATION] Summary written: {SUMMARY_PATH}")


if __name__ == "__main__":
    main()

"""
validate_missing_baseline.py — Phase 2.5: Behavioral anomaly detection for missing_pct.

Responsibilities (STRICT):
  - Statistical drift detection on missing_pct using historical metrics files
  - Emit WARNING (pipeline continues) or FAIL (pipeline aborts) based on sigma thresholds
  - Maintain a persistent baseline registry with mean, std, sample_count, last_observed

Does NOT touch dataset_validator_sop17.py responsibilities.
  dataset_validator_sop17.py = structural truth (duplicates, monotonic, row counts, naming)
  This script              = behavioral anomaly detection (statistical drift)

Anomaly thresholds:
  WARNING : current > mean + 2*std
  FAIL    : current > mean + 3*std  AND  absolute delta > FAIL_ABS_DELTA (2%)
            (both conditions must hold — prevents false fails on ultra-clean files)
  COLD_START (new file, no history): WARNING only, never FAIL

Exit codes:
  0 = All files OK or WARNING only
  1 = One or more FAIL conditions detected

Writes:
  state/missing_rate_baseline.json    — persistent baseline registry (updated each run)
  state/missing_baseline_summary.json — this run's per-file results
"""

import os
import sys
import json
import shutil
import statistics
from datetime import datetime, timezone
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
DATA_INGRESS      = str(Path(__file__).resolve().parents[2])
LOG_DIR           = Path(DATA_INGRESS) / "logs" / "DATA_PIPELINE"
BASELINE_LOG_DIR  = Path(DATA_INGRESS) / "logs" / "BASELINE"
STATE_DIR         = Path(DATA_INGRESS) / "state"
BASELINE_FILE     = STATE_DIR / "missing_rate_baseline.json"
SUMMARY_FILE      = STATE_DIR / "missing_baseline_summary.json"

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
FULL_RUN_MIN_BYTES = 50_000   # files below this are partial/stub runs — excluded
BASELINE_LOOKBACK  = 14       # number of recent full runs used for mean/std
MIN_SAMPLES_FAIL   = 5        # need >= 5 samples before a FAIL can be emitted
WARN_SIGMA         = 2.0      # sigma threshold for WARNING
FAIL_SIGMA         = 3.0      # sigma threshold for FAIL (also requires abs delta check)
FAIL_ABS_DELTA     = 0.02     # absolute delta floor — must exceed this to FAIL (2%)

# ---------------------------------------------------------------------------
# Alerts (observer only — never raises, NO-OP if env vars absent)
# ---------------------------------------------------------------------------
sys.path.insert(0, str(Path(__file__).parent))
try:
    from alerts import send_alert as _send_alert
except ImportError:
    def _send_alert(event_type: str, message: str) -> None:
        pass


# ---------------------------------------------------------------------------
# Metrics loading
# ---------------------------------------------------------------------------

def load_full_run_files():
    """Return all full-run metrics JSON files, sorted oldest → newest by filename."""
    candidates = [
        f for f in LOG_DIR.glob("metrics_*.json")
        if f.stat().st_size >= FULL_RUN_MIN_BYTES
    ]
    return sorted(candidates, key=lambda f: f.name)


def parse_metrics_file(fpath: Path) -> list[dict]:
    """Parse a metrics JSON file for HISTORY use. Return list of entries or [] on error.
    Requires > 50 entries to filter out stub/partial historic runs."""
    try:
        with open(fpath, encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list) and len(data) > 50:
            return data
        return []
    except Exception:
        return []


def parse_any_metrics_file(fpath: Path) -> list[dict]:
    """Parse a metrics file without entry-count filtering.
    Used for today's values only — today's file is definitively Phase 1's output."""
    try:
        with open(fpath, encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            return data
        return []
    except Exception:
        return []


def load_today_file() -> Path | None:
    """Return the most recent metrics_*.json written by Phase 1, regardless of size.
    Independent of the full-run size filter used for baseline history."""
    all_files = sorted(LOG_DIR.glob("metrics_*.json"), key=lambda f: f.name)
    return all_files[-1] if all_files else None


def build_history(full_run_files: list[Path]) -> dict[str, list[float]]:
    """
    Build per-file chronological history of missing_pct.
    Returns: {filename: [missing_pct, ...]} oldest → newest.
    """
    history: dict[str, list[float]] = {}
    for fpath in full_run_files:
        entries = parse_metrics_file(fpath)
        for entry in entries:
            fname = entry.get("file", "")
            if not fname:
                continue
            mp = (entry.get("metrics") or {}).get("missing_pct")
            if mp is None:
                continue
            history.setdefault(fname, []).append(float(mp))
    return history


def get_today_metrics() -> dict[str, float]:
    """
    Return today's {filename: missing_pct} from the most recent metrics file.
    Uses Phase 1's fresh output directly — no size or entry-count filtering.
    Operates independently of full_run_files so quiet days (weekends, holidays)
    are never silently skipped due to small file size.
    """
    today_file = load_today_file()
    if not today_file:
        return {}
    entries = parse_any_metrics_file(today_file)
    result: dict[str, float] = {}
    for entry in entries:
        fname = entry.get("file", "")
        mp = (entry.get("metrics") or {}).get("missing_pct")
        if fname and mp is not None:
            result[fname] = float(mp)
    return result


# ---------------------------------------------------------------------------
# Baseline computation
# ---------------------------------------------------------------------------

def compute_baseline(history: dict[str, list[float]]) -> dict[str, dict]:
    """
    For each file, compute mean and std from last BASELINE_LOOKBACK readings.
    Returns baseline dict ready for registry write.
    """
    baseline: dict[str, dict] = {}
    for fname, readings in history.items():
        window = readings[-BASELINE_LOOKBACK:]
        n = len(window)
        mean = statistics.mean(window) if n >= 1 else 0.0
        std  = statistics.stdev(window) if n >= 2 else 0.0
        baseline[fname] = {
            "mean":         round(mean, 6),
            "std":          round(std,  6),
            "sample_count": n,
            "last_observed": round(readings[-1], 6),  # most recent observed value
            "last_updated":  datetime.now(timezone.utc).isoformat(),
        }
    return baseline


# ---------------------------------------------------------------------------
# Anomaly detection
# ---------------------------------------------------------------------------

def check_file(fname: str, current: float, b: dict | None) -> dict:
    """
    Check one file against its baseline entry. Returns result dict.
    b = None means cold start (no history for this file).
    """
    if b is None:
        return {
            "file":           fname,
            "status":         "COLD_START",
            "current":        round(current, 6),
            "last_observed":  round(current, 6),
            "delta":          None,
            "baseline_mean":  None,
            "baseline_std":   None,
            "sample_count":   0,
            "detail":         "No baseline yet — first observation recorded",
        }

    mean  = b["mean"]
    std   = b["std"]
    n     = b["sample_count"]
    last  = b["last_observed"]
    delta = current - mean

    warn_thresh = mean + WARN_SIGMA * std
    fail_thresh = mean + FAIL_SIGMA * std

    # Determine status
    if current <= warn_thresh:
        status = "OK"
    elif (
        current > fail_thresh
        and delta > FAIL_ABS_DELTA
        and n >= MIN_SAMPLES_FAIL
    ):
        status = "FAIL"
    else:
        status = "WARN"

    return {
        "file":           fname,
        "status":         status,
        "current":        round(current, 6),
        "last_observed":  round(last,    6),
        "delta":          round(delta,   6),
        "baseline_mean":  round(mean,    6),
        "baseline_std":   round(std,     6),
        "warn_threshold": round(warn_thresh, 6),
        "fail_threshold": round(fail_thresh, 6),
        "sample_count":   n,
        "detail": (
            f"delta={delta:+.5f}  "
            f"warn>{warn_thresh:.5f}  "
            f"fail>{fail_thresh:.5f}  "
            f"n={n}"
        ),
    }


# ---------------------------------------------------------------------------
# Registry I/O
# ---------------------------------------------------------------------------

def load_baseline_registry() -> dict:
    """Load existing baseline registry. Returns {} if missing or corrupt."""
    if not BASELINE_FILE.exists():
        return {}
    try:
        with open(BASELINE_FILE, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def save_baseline_registry(baseline: dict) -> None:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    with open(BASELINE_FILE, "w", encoding="utf-8") as f:
        json.dump(baseline, f, indent=2)


def save_summary(results: list[dict], counts: dict) -> None:
    summary = {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "total":         counts["total"],
        "ok":            counts["ok"],
        "warn":          counts["warn"],
        "fail":          counts["fail"],
        "cold_start":    counts["cold_start"],
        "status":        "FAIL" if counts["fail"] > 0 else "PASS",
        "results":       results,
    }
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    with open(SUMMARY_FILE, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    print("=" * 60)
    print("PHASE 2.5 — Missing Rate Baseline Check")
    print("=" * 60)

    # Load all full-run metrics files
    full_run_files = load_full_run_files()
    print(f"Full-run metrics files found: {len(full_run_files)}")

    if not full_run_files:
        print("[WARN] No full-run metrics files found. Cannot compute baseline.")
        print("[PASS] Phase 2.5 skipped — no history available yet.")
        sys.exit(0)

    # Build history and compute fresh baseline
    history  = build_history(full_run_files)
    baseline = compute_baseline(history)

    # Get today's values (most recent metrics file — no size filter)
    today = get_today_metrics()
    print(f"Files in today's metrics: {len(today)}")

    if not today:
        print("[WARN] Today's metrics file has no entries. Skipping check.")
        sys.exit(0)

    # Check each file
    results  = []
    counts   = {"total": 0, "ok": 0, "warn": 0, "fail": 0, "cold_start": 0}
    failures = []

    for fname, current in sorted(today.items()):
        b      = baseline.get(fname)
        result = check_file(fname, current, b)
        results.append(result)
        counts["total"] += 1
        status = result["status"]

        if status == "OK":
            counts["ok"] += 1
        elif status == "WARN":
            counts["warn"] += 1
            print(f"  [WARN]       {fname}  current={current:.5f}  {result['detail']}")
        elif status == "FAIL":
            counts["fail"] += 1
            failures.append(result)
            print(f"  [FAIL]       {fname}  current={current:.5f}  {result['detail']}")
        elif status == "COLD_START":
            counts["cold_start"] += 1
            print(f"  [COLD_START] {fname}  current={current:.5f}  (baseline initialised)")

    # Print summary
    print(f"\nResults: OK={counts['ok']}  WARN={counts['warn']}  "
          f"FAIL={counts['fail']}  COLD_START={counts['cold_start']}  "
          f"TOTAL={counts['total']}")

    # Save updated baseline registry (includes last_observed from today)
    save_baseline_registry(baseline)
    print(f"[BASELINE] Registry updated: {BASELINE_FILE}")

    # Save run summary
    save_summary(results, counts)
    print(f"[BASELINE] Summary written:  {SUMMARY_FILE}")

    # Archive summary to logs/BASELINE/ for audit trail (90-day retention via daily_pipeline)
    BASELINE_LOG_DIR.mkdir(parents=True, exist_ok=True)
    archive_name = f"baseline_summary_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.json"
    shutil.copy2(SUMMARY_FILE, BASELINE_LOG_DIR / archive_name)
    print(f"[BASELINE] Archive written:  {BASELINE_LOG_DIR / archive_name}")

    # Alert and exit
    if counts["fail"] > 0:
        fail_names = ", ".join(r["file"] for r in failures)
        _send_alert(
            "MISSING_RATE_ANOMALY",
            f"{counts['fail']} file(s) breached missing_pct baseline: {fail_names}"
        )
        print(f"\n[FAIL] {counts['fail']} file(s) exceeded anomaly threshold. Pipeline aborted.")
        sys.exit(1)

    print("[PASS] Phase 2.5 complete.")
    sys.exit(0)


if __name__ == "__main__":
    main()

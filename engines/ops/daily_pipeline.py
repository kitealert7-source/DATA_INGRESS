"""
Anti-Gravity Daily Pipeline — Master Orchestrator
Executes the complete daily data update pipeline.

Phases (in order):
1. RAW Update (incremental)
2. Validation (--audit-all)
3. CLEAN Rebuild (--all)
4. RESEARCH Rebuild (--register-lineage)
5. USD_SYNTH Update
6. Governance Update (last_successful_daily_run.json)

Exit Codes:
0 = All phases succeeded
1 = Phase failure (governance NOT updated)
"""

import subprocess
import sys
import os
import json
from datetime import datetime, timezone, timedelta
from pathlib import Path

# Alerts — observer only, never raises, NO-OP if env vars absent
sys.path.insert(0, os.path.dirname(__file__))
from alerts import send_alert

# Paths
DATA_INGRESS = str(Path(__file__).resolve().parents[2])
DATA_ROOT = str(Path(__file__).resolve().parents[3] / "Anti_Gravity_DATA_ROOT")
GOVERNANCE_FILE = os.path.join(DATA_ROOT, "governance", "last_successful_daily_run.json")
VALIDATION_SUMMARY = os.path.join(DATA_INGRESS, "state", "last_validation_summary.json")
REPORTS_DIR = os.path.join(DATA_INGRESS, "reports")

# Scripts
RAW_UPDATE        = os.path.join(DATA_INGRESS, "engines", "core", "raw_update_sop17.py")
VALIDATOR         = os.path.join(DATA_INGRESS, "engines", "core", "dataset_validator_sop17.py")
CLEAN_REBUILD     = os.path.join(DATA_INGRESS, "engines", "core", "clean_rebuild_sop17.py")
RESEARCH_REBUILD  = os.path.join(DATA_INGRESS, "engines", "core", "rebuild_research_sop17.py")
USD_SYNTH                = os.path.join(DATA_INGRESS, "engines", "ops", "build_usd_synth.py")
RESEARCH_VALIDATE        = os.path.join(DATA_INGRESS, "engines", "ops", "validate_research_layer.py")
MISSING_BASELINE_VALIDATE = os.path.join(DATA_INGRESS, "engines", "ops", "validate_missing_baseline.py")

# Log directories
DATA_PIPELINE_LOG_DIR = os.path.join(DATA_INGRESS, "logs", "DATA_PIPELINE")
SCHEDULER_LOG_DIR     = os.path.join(DATA_INGRESS, "logs", "SCHEDULER")
PREFLIGHT_LOG_DIR     = os.path.join(DATA_INGRESS, "logs", "PREFLIGHT")
BASELINE_LOG_DIR      = os.path.join(DATA_INGRESS, "logs", "BASELINE")
INTEGRITY_LOG         = os.path.join(DATA_INGRESS, "state", "integrity_events.log")

# Retention policy (days)
LOG_RETENTION_DAYS        = 30   # DATA_PIPELINE + SCHEDULER
PREFLIGHT_RETENTION_DAYS  = 90   # audit trail — keep longer
REPORTS_RETENTION_DAYS    = 90   # Daily_Report_*.md only

# Integrity log rollover — rotate when file exceeds this size
INTEGRITY_LOG_MAX_MB   = 5.0     # rotate at 5 MB
INTEGRITY_LOG_KEEP     = 3       # keep last N monthly archives


def cleanup_old_logs(log_dir: str, days: int = LOG_RETENTION_DAYS,
                     pattern: str = "*") -> None:
    """Delete files matching pattern older than N days from log directory.
    pattern defaults to '*' (all files). Never raises."""
    try:
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        deleted = 0
        for f in Path(log_dir).glob(pattern):
            if f.is_file():
                mtime = datetime.fromtimestamp(f.stat().st_mtime, tz=timezone.utc)
                if mtime < cutoff:
                    f.unlink()
                    deleted += 1
        if deleted:
            print(f"[CLEANUP] Deleted {deleted} file(s) older than {days}d "
                  f"({pattern}) from {log_dir}")
    except Exception as e:
        print(f"[CLEANUP WARN] Log cleanup failed (non-fatal): {e}")


def rotate_integrity_log(log_path: str = INTEGRITY_LOG,
                         max_mb: float = INTEGRITY_LOG_MAX_MB,
                         keep_rolled: int = INTEGRITY_LOG_KEEP) -> None:
    """Rotate integrity_events.log when it exceeds max_mb.
    Archives as integrity_events_YYYY-MM.log. Keeps last keep_rolled archives.
    Never raises."""
    try:
        p = Path(log_path)
        if not p.exists():
            return
        size_mb = p.stat().st_size / 1_048_576
        if size_mb < max_mb:
            return
        # Rotate: rename current file to dated archive
        now = datetime.now(timezone.utc)
        archive = p.parent / f"{p.stem}_{now.strftime('%Y-%m')}.log"
        p.rename(archive)
        print(f"[ROTATE] {p.name} → {archive.name} ({size_mb:.1f} MB)")
        # Purge oldest archives beyond keep_rolled
        archives = sorted(p.parent.glob(f"{p.stem}_????-??.log"), key=lambda f: f.name)
        for old in archives[:-keep_rolled]:
            old.unlink()
            print(f"[ROTATE] Purged old archive: {old.name}")
    except Exception as e:
        print(f"[ROTATE WARN] integrity_events.log rotation failed (non-fatal): {e}")


def run_phase(name, cmd):
    """Execute a phase and return success status."""
    print(f"\n{'='*60}")
    print(f"PHASE: {name}")
    print(f"CMD: {' '.join(cmd)}")
    print('='*60)
    
    result = subprocess.run(cmd, cwd=DATA_INGRESS)
    
    if result.returncode != 0:
        print(f"[FAIL] {name} exited with code {result.returncode}")
        return False
    
    print(f"[PASS] {name}")
    return True


def load_validation_summary():
    """Load validation summary from state file. HARD FAIL if missing or malformed."""
    if not os.path.exists(VALIDATION_SUMMARY):
        print(f"[HARD FAIL] Validation summary not found: {VALIDATION_SUMMARY}")
        sys.exit(1)
    
    try:
        with open(VALIDATION_SUMMARY, 'r') as f:
            summary = json.load(f)
        
        if "datasets_validated" not in summary or "status" not in summary:
            print(f"[HARD FAIL] Malformed validation summary: missing required fields")
            sys.exit(1)
        
        return summary
    except json.JSONDecodeError as e:
        print(f"[HARD FAIL] Invalid JSON in validation summary: {e}")
        sys.exit(1)


def update_governance(datasets_validated):
    """Update last_successful_daily_run.json with today's date."""
    now = datetime.now(timezone.utc)
    data = {
        "last_run_date": now.strftime("%Y-%m-%d"),
        "status": "SUCCESS",
        "run_type": "STANDARD_DAILY",
        "timestamp_utc": now.isoformat(),
        "datasets_validated": datasets_validated,
        "validator": "dataset_validator_sop17",
        "notes": "Full daily pipeline completed successfully (automated)"
    }
    
    with open(GOVERNANCE_FILE, 'w') as f:
        json.dump(data, f, indent=2)
    
    print(f"[GOVERNANCE] Updated: {GOVERNANCE_FILE}")
    print(f"[GOVERNANCE] Date: {data['last_run_date']}")
    return data


def generate_daily_report(run_date, datasets_validated):
    """Generate Daily Report markdown file."""
    filename = f"Daily_Report_{run_date.replace('-', '')}.md"
    filepath = os.path.join(REPORTS_DIR, filename)
    
    content = f"""# Daily Data Ingestion Report - {run_date}

**Status**: SUCCESS  
**Run Type**: STANDARD_DAILY  
**Datasets Validated**: {datasets_validated}

---

## Pipeline Summary

| Phase | Status |
|-------|--------|
| RAW Update | PASS |
| Validation | PASS |
| CLEAN Rebuild | PASS |
| RESEARCH Rebuild | PASS |
| USD_SYNTH | PASS |
| Governance Update | PASS |

---

**END OF REPORT**
"""
    
    os.makedirs(REPORTS_DIR, exist_ok=True)
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(content)
    
    return filepath


def print_completion_report(run_date, datasets_validated, report_path):
    """Print structured completion report to stdout."""
    print("\n" + "="*60)
    print("DAILY DATA UPDATE — COMPLETED")
    print("="*60)
    print(f"Date (UTC):            {run_date}")
    print(f"Status:                COMPLETED")
    print(f"Phases Executed:       6/6")
    print(f"Datasets Validated:    {datasets_validated}")
    print(f"Governance Updated:    YES")
    print(f"Report:                {report_path}")
    print("="*60)


def main():
    # Use deterministic Python interpreter (sys.executable)
    python = sys.executable
    
    print("="*60)
    print("ANTI-GRAVITY DAILY PIPELINE — MASTER ORCHESTRATOR")
    print(f"Started: {datetime.now(timezone.utc).isoformat()}")
    print(f"Python: {python}")
    print("="*60)

    # ── Retention & rotation (all non-fatal) ──────────────────────────
    cleanup_old_logs(DATA_PIPELINE_LOG_DIR)                                      # 30d
    cleanup_old_logs(SCHEDULER_LOG_DIR,    days=LOG_RETENTION_DAYS)              # 30d
    cleanup_old_logs(PREFLIGHT_LOG_DIR,    days=PREFLIGHT_RETENTION_DAYS)        # 90d
    cleanup_old_logs(BASELINE_LOG_DIR,    days=PREFLIGHT_RETENTION_DAYS)        # 90d — baseline audit trail
    cleanup_old_logs(REPORTS_DIR,          days=REPORTS_RETENTION_DAYS,
                     pattern="Daily_Report_*.md")                                # 90d — reports only
    rotate_integrity_log()                                                        # size-based (5 MB)
    # ─────────────────────────────────────────────────────────────────
    
    phases = [
        ("1. RAW Update",             [python, RAW_UPDATE,        "--incremental"]),
        ("2. Validation",             [python, VALIDATOR,              "--audit-all"]),
        ("2.5. Missing Rate Baseline", [python, MISSING_BASELINE_VALIDATE]),
        ("3. CLEAN Rebuild",          [python, CLEAN_REBUILD,          "--all"]),
        ("4. RESEARCH Rebuild",       [python, RESEARCH_REBUILD,  "--register-lineage"]),
        ("4.5. RESEARCH Validation",  [python, RESEARCH_VALIDATE]),
        ("5. USD_SYNTH",              [python, USD_SYNTH]),
    ]
    
    for name, cmd in phases:
        if not run_phase(name, cmd):
            print("\n" + "="*60)
            print("PIPELINE FAILED — Governance NOT updated")
            print("="*60)
            send_alert("PIPELINE_FAILED",
                f"Phase '{name}' failed. Governance NOT updated. "
                f"Check logs: {DATA_PIPELINE_LOG_DIR}")
            sys.exit(1)
    
    # Load validation summary (HARD FAIL if missing)
    validation_summary = load_validation_summary()
    datasets_validated = validation_summary["datasets_validated"]
    
    # Phase 6: Governance update (only if all prior phases succeeded)
    print(f"\n{'='*60}")
    print("PHASE: 6. Governance Update")
    print('='*60)
    governance_data = update_governance(datasets_validated)
    run_date = governance_data["last_run_date"]
    
    # Generate daily report (non-authoritative, cannot fail pipeline)
    try:
        report_path = generate_daily_report(run_date, datasets_validated)
    except Exception as e:
        print(f"[WARN] Report generation failed: {e}")
        report_path = "N/A"

    # Build freshness index + append section to daily report (non-fatal)
    try:
        from build_freshness_index import build_index, write_index, append_to_report
        master_data = Path(DATA_ROOT) / "MASTER_DATA"
        index = build_index(data_root=master_data)
        write_index(index, data_root=Path(DATA_ROOT))
        if report_path != "N/A":
            append_to_report(index, Path(report_path))
        n_stale = sum(
            1 for v in index.get("entries", {}).values()
            if v.get("days_behind", 0) > index.get("buffer_days", 3)
        )
        print(f"[FRESHNESS] Index written -> {Path(DATA_ROOT) / 'freshness_index.json'}")
        print(f"[FRESHNESS] {n_stale} stale symbol(s) detected.")
    except Exception as e:
        print(f"[WARN] Freshness index generation failed (non-fatal): {e}")

    # Print completion report to stdout (visible in scheduler logs)
    print_completion_report(run_date, datasets_validated, report_path)

    send_alert("PIPELINE_COMPLETE",
        f"Daily update complete. Date={run_date} "
        f"Datasets={datasets_validated} Phases=6/6 PASS")

    print(f"\nFinished: {datetime.now(timezone.utc).isoformat()}")
    sys.exit(0)


if __name__ == "__main__":
    main()

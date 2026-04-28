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
import atexit
import socket
from datetime import datetime, timezone, timedelta
from pathlib import Path

import psutil

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
RAW_COVERAGE      = os.path.join(DATA_INGRESS, "engines", "ops",  "assert_raw_coverage.py")
VALIDATOR         = os.path.join(DATA_INGRESS, "engines", "core", "dataset_validator_sop17.py")
CLEAN_REBUILD     = os.path.join(DATA_INGRESS, "engines", "core", "clean_rebuild_sop17.py")
RESEARCH_REBUILD  = os.path.join(DATA_INGRESS, "engines", "core", "rebuild_research_sop17.py")
USD_SYNTH                = os.path.join(DATA_INGRESS, "engines", "ops", "build_usd_synth.py")
NEWS_CALENDAR_BUILD      = os.path.join(DATA_INGRESS, "engines", "ops", "build_news_calendar.py")
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

# PID lock file — prevents overlapping pipeline runs (scheduler + manual)
# Payload is JSON: {pid, create_time, timestamp_utc, hostname, script}
# Acquisition is atomic via O_CREAT|O_EXCL; staleness is identity-checked
# against both psutil.pid_exists() and Process.create_time() (survives PID reuse).
PIPELINE_LOCK_FILE = os.path.join(DATA_INGRESS, "state", "daily_pipeline.lock")

# Tolerance when comparing process create_time across JSON serialization round-trip.
# psutil.create_time() on Windows derives from GetProcessTimes (100ns precision);
# JSON float round-trip preserves IEEE 754 double precision; so sub-ms drift is
# essentially quantization noise. 1ms is four orders of magnitude tighter than any
# plausible PID-reuse window while remaining robust against float repr quirks.
_CREATE_TIME_TOLERANCE_SEC = 0.001


def _build_lock_payload() -> dict:
    """Build the JSON payload describing the current process's lock claim."""
    pid = os.getpid()
    try:
        create_time = psutil.Process(pid).create_time()
    except (psutil.NoSuchProcess, psutil.AccessDenied, Exception):
        # Extremely unlikely (we are that process) — fall back to 0.0.
        # Release-side identity check will skip create_time comparison if ours is 0.0.
        create_time = 0.0
    return {
        "pid": pid,
        "create_time": create_time,
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "hostname": socket.gethostname(),
        "script": os.path.abspath(__file__),
    }


def _validate_existing_lock(lock_path: Path) -> tuple[bool, str, dict | None]:
    """Inspect an existing lock file.

    Returns (is_stale, reason, payload):
      is_stale = True  -> safe to delete and retry
        reasons: 'corrupt_file' | 'dead_pid' | 'pid_reuse'
      is_stale = False -> live owner; caller must abort
        payload returned so caller can log the owner PID
    """
    try:
        payload = json.loads(lock_path.read_text(encoding="utf-8"))
        pid = int(payload["pid"])
        stored_ct = float(payload["create_time"])
    except Exception:
        return True, "corrupt_file", None

    if not psutil.pid_exists(pid):
        return True, "dead_pid", payload

    try:
        actual_ct = psutil.Process(pid).create_time()
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        return True, "dead_pid", payload
    except Exception:
        # Can't verify identity — treat as live to avoid false eviction.
        return False, "", payload

    if stored_ct > 0.0 and abs(actual_ct - stored_ct) > _CREATE_TIME_TOLERANCE_SEC:
        # Same PID number, but a different process — the original died and
        # the OS recycled its PID (common after reboots).
        return True, "pid_reuse", payload

    return False, "", payload


def _acquire_pipeline_lock() -> None:
    """Atomically acquire the pipeline lock.

    Contract:
      - Atomic create via os.open(O_CREAT|O_EXCL|O_WRONLY). Two processes racing
        the scheduler cannot both succeed.
      - If the file already exists, validate the owner: PID must be live AND
        its create_time must match the one recorded in the lock payload.
      - Stale lock (dead PID, PID reuse, or corrupt file) is evicted once,
        then a single atomic-create retry is attempted. Further failure aborts.
      - Live lock: print [LOCK] Already running and exit(1).
      - On success, register atexit release.
    """
    lock_path = Path(PIPELINE_LOCK_FILE)
    lock_path.parent.mkdir(parents=True, exist_ok=True)

    payload = _build_lock_payload()
    payload_bytes = json.dumps(payload, indent=2).encode("utf-8")

    for attempt in (1, 2):
        # Attempt atomic create. This is the only path that grants the lock.
        # O_EXCL gives inter-process atomicity on the slot. We write-then-fsync-
        # then-close while still holding the exclusive fd, so the payload is
        # durable on disk before any other process could validate it. The only
        # partial-write window is the <1μs between O_EXCL success and os.write;
        # a crash there leaves an empty file, which the next run evicts as
        # corrupt_file.
        try:
            fd = os.open(
                str(lock_path),
                os.O_CREAT | os.O_EXCL | os.O_WRONLY,
                0o644,
            )
            try:
                os.write(fd, payload_bytes)
                os.fsync(fd)
            finally:
                os.close(fd)
            atexit.register(_release_pipeline_lock)
            print(f"[LOCK] Acquired (PID {payload['pid']})")
            return
        except FileExistsError:
            # Another process (or a stale file) holds the slot — validate.
            pass

        print(f"[LOCK] Exists -> validating")
        is_stale, reason, owner_payload = _validate_existing_lock(lock_path)

        if not is_stale:
            owner_pid = owner_payload.get("pid", "?") if owner_payload else "?"
            print(f"[LOCK] Already running (PID {owner_pid}). Aborting.")
            print(f"[LOCK] Lock file: {PIPELINE_LOCK_FILE}")
            sys.exit(1)

        print(f"[LOCK] Stale (reason={reason})")

        if attempt == 2:
            # Second attempt also found a lock and declared it stale — something
            # else is racing us into the slot. Refuse to loop forever.
            print(f"[LOCK] Unable to acquire after retry. Aborting.")
            sys.exit(1)

        # Evict stale lock, then retry the atomic create once. No backoff delay —
        # O_EXCL already guarantees correctness under contention; sleeping only
        # adds latency. If another process races us into the slot between our
        # unlink and our retry, the second loop iteration will treat them as a
        # live owner and abort cleanly.
        try:
            lock_path.unlink()
        except FileNotFoundError:
            pass
        except OSError as e:
            print(f"[LOCK] Could not remove stale lock: {e}. Aborting.")
            sys.exit(1)


def _release_pipeline_lock() -> None:
    """Release the lock iff this process is still the rightful owner.

    Identity check: PID must match ours AND create_time must match our own.
    This prevents a foreign lock (written after we crashed and another pipeline
    started) from being deleted by our atexit handler.
    """
    lock_path = Path(PIPELINE_LOCK_FILE)
    try:
        if not lock_path.exists():
            return

        try:
            payload = json.loads(lock_path.read_text(encoding="utf-8"))
            owner_pid = int(payload["pid"])
            owner_ct = float(payload["create_time"])
        except Exception:
            # Corrupt lock at release time — leave it; let the next run's
            # stale-detection handle eviction.
            print(f"[LOCK WARN] Lock unreadable at release; leaving in place")
            return

        if owner_pid != os.getpid():
            print(
                f"[LOCK WARN] Lock owned by PID {owner_pid}, not current "
                f"({os.getpid()}); not releasing"
            )
            return

        # Verify our own create_time — guards against the pathological case
        # where the lock was rewritten by a foreign process that happened to
        # reuse our PID. (Cheap; skipped only if acquisition had no create_time.)
        if owner_ct > 0.0:
            try:
                our_ct = psutil.Process(os.getpid()).create_time()
                if abs(our_ct - owner_ct) > _CREATE_TIME_TOLERANCE_SEC:
                    print(f"[LOCK WARN] create_time mismatch at release; not releasing")
                    return
            except Exception:
                # Can't verify — err on the side of not deleting.
                print(f"[LOCK WARN] Could not verify create_time at release; not releasing")
                return

        lock_path.unlink()
        print(f"[LOCK] Released")
    except OSError as e:
        print(f"[LOCK WARN] Could not remove lock file: {e}")


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
| NEWS_CALENDAR | PASS |
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
    # Prevent overlapping runs (scheduler + concurrent manual invocation)
    _acquire_pipeline_lock()

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
        ("1. RAW Update",              [python, RAW_UPDATE,              "--incremental"]),
        # 1.5 promotes "Phase 1 didn't crash" to "every expected (sym, tf) tuple
        # has fresh RAW data within threshold." Catches silent
        # MT5.copy_rates_from() returning None for unsubscribed symbols and
        # any other class of skip-without-error. Hard fail aborts before
        # Validation; governance is never updated on incomplete RAW.
        ("1.5. RAW Coverage Assertion", [python, RAW_COVERAGE]),
        ("2. Validation",              [python, VALIDATOR,               "--audit-all"]),
        ("2.5. Missing Rate Baseline", [python, MISSING_BASELINE_VALIDATE]),
        ("3. CLEAN Rebuild",           [python, CLEAN_REBUILD,           "--all"]),
        ("4. RESEARCH Rebuild",        [python, RESEARCH_REBUILD,        "--register-lineage"]),
        ("4.5. RESEARCH Validation",   [python, RESEARCH_VALIDATE]),
        ("5. USD_SYNTH",               [python, USD_SYNTH]),
        ("5.5. NEWS_CALENDAR",         [python, NEWS_CALENDAR_BUILD]),
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

"""
assert_raw_coverage.py

Phase 1.5 of the daily pipeline — RAW completeness invariant.

After Phase 1 RAW Update, every (sym_broker, tf) tuple in EXPECTED_COVERAGE
must have a 2026 RAW file with last_ts >= today_utc - threshold[tf].

Why this exists
---------------
"Phase 1 PASS" only means the RAW script didn't crash. It does NOT mean every
expected dataset received data. On 2026-04-28, MT5.copy_rates_from() silently
returned None for 28 (symbol, tf) combos at 1m/5m (cross pairs + indices that
weren't subscribed in Market Watch). Phase 1 reported PASS, no metric entries
existed for the affected tuples, and the gap was detected only 24h later via
a manual freshness scan.

This script promotes "phase didn't crash" to "every expected output exists
and is fresh." It is a hard fail — the daily pipeline must not advance to
governance update if coverage is incomplete.

Exit codes
----------
0  — every expected tuple has fresh data within threshold
1  — at least one expected tuple is missing or stale; details printed

Output
------
Prints a structured report to stdout AND writes
  state/last_coverage_assertion.json
with full results for downstream consumers (alerts, dashboards).
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

# DATA_INGRESS root resolved from this file's location: engines/ops/
_DI = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_DI))

from config.path_config import (  # noqa: E402
    MASTER_DATA,
    EXPECTED_COVERAGE,
    COVERAGE_MAX_DAYS_BEHIND,
    COVERAGE_EXCEPTIONS,
)

import pandas as pd  # noqa: E402

STATE_FILE = _DI / "state" / "last_coverage_assertion.json"


def _last_raw_ts(raw_path: Path) -> "pd.Timestamp | None":
    """Read last valid timestamp from a 2026 RAW CSV. Returns None on any
    failure or if no valid bars exist. Mirrors the strict UTC-only logic used
    by build_freshness_index._last_valid_ts after the IST/UTC fix."""
    try:
        df = pd.read_csv(raw_path, comment="#")
        t_col = "time" if "time" in df.columns else "timestamp"
        ts = pd.to_datetime(df[t_col], errors="coerce")
        ts = ts[ts.notna()]
        now = pd.Timestamp.utcnow().tz_localize(None)
        ts = ts[(ts > "2010-01-01") & (ts < now + pd.Timedelta(minutes=1))]
        if ts.empty:
            return None
        return ts.max()
    except Exception:
        return None


def assess_coverage(year: int | None = None) -> dict:
    """Run the assertion. Returns a dict with:
      - status: "PASS" or "FAIL"
      - now_utc:           ISO timestamp at check time
      - expected_count:    len(EXPECTED_COVERAGE)
      - exceptions_count:  len(COVERAGE_EXCEPTIONS) honoured
      - violations:        list of {sym_broker, tf, reason, detail}
        reason ∈ {"missing_file", "unreadable", "stale"}
      - violation_count:   len(violations) AFTER exceptions removed
    """
    now_utc = pd.Timestamp.utcnow().tz_localize(None)
    today = now_utc.normalize()
    target_year = year if year is not None else today.year

    violations: list[dict] = []

    for sym_broker, tf in sorted(EXPECTED_COVERAGE):
        if (sym_broker, tf) in COVERAGE_EXCEPTIONS:
            continue  # honoured exception — silenced by design

        raw_dir = MASTER_DATA / f"{sym_broker}_MASTER" / "RAW"
        raw_path = raw_dir / f"{sym_broker}_{tf}_{target_year}_RAW.csv"

        if not raw_path.exists():
            violations.append({
                "sym_broker": sym_broker, "tf": tf,
                "reason": "missing_file",
                "detail": f"expected at {raw_path.relative_to(MASTER_DATA)}",
            })
            continue

        last_ts = _last_raw_ts(raw_path)
        if last_ts is None:
            violations.append({
                "sym_broker": sym_broker, "tf": tf,
                "reason": "unreadable",
                "detail": str(raw_path.name),
            })
            continue

        days_behind = max(0, (today - last_ts.normalize()).days)
        threshold = COVERAGE_MAX_DAYS_BEHIND.get(tf, 5)
        if days_behind > threshold:
            violations.append({
                "sym_broker": sym_broker, "tf": tf,
                "reason": "stale",
                "detail": (
                    f"last_ts={last_ts.strftime('%Y-%m-%d %H:%M:%S')} "
                    f"days_behind={days_behind} threshold={threshold}"
                ),
            })

    return {
        "status":           "PASS" if not violations else "FAIL",
        "now_utc":          now_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "year_scanned":     target_year,
        "expected_count":   len(EXPECTED_COVERAGE),
        "exceptions_count": len(COVERAGE_EXCEPTIONS),
        "violations":       violations,
        "violation_count":  len(violations),
    }


def _write_state(report: dict) -> None:
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps(report, indent=2), encoding="utf-8")


def _print_report(report: dict) -> None:
    print("=" * 60)
    print("RAW COVERAGE ASSERTION (Phase 1.5)")
    print("=" * 60)
    print(f"  now_utc          : {report['now_utc']}")
    print(f"  year_scanned     : {report['year_scanned']}")
    print(f"  expected         : {report['expected_count']}")
    print(f"  exceptions       : {report['exceptions_count']}")
    print(f"  violations       : {report['violation_count']}")
    print(f"  status           : {report['status']}")

    if report["violations"]:
        print()
        print("  VIOLATIONS:")
        # Group by reason for readability
        by_reason: dict[str, list] = {}
        for v in report["violations"]:
            by_reason.setdefault(v["reason"], []).append(v)
        for reason in ("missing_file", "unreadable", "stale"):
            items = by_reason.get(reason, [])
            if not items:
                continue
            print(f"    [{reason}] ({len(items)}):")
            for v in sorted(items, key=lambda x: (x["sym_broker"], x["tf"])):
                print(f"      {v['sym_broker']:<22} {v['tf']:<4}  {v['detail']}")
    print("=" * 60)


def main() -> int:
    report = assess_coverage()
    _write_state(report)
    _print_report(report)
    return 0 if report["status"] == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())

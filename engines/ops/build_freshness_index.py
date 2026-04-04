"""
build_freshness_index.py

Standalone freshness index builder for DATA_INGRESS.

Designed to run at the end of each data ingestion cycle.
Appends a DATA FRESHNESS section to the existing ingestion report,
and writes freshness_index.json into MASTER_DATA root for Trade_Scan to read.

Usage (called from DATA_INGRESS post-ingest hook):
    from build_freshness_index import build_index, append_to_report, write_index
    index = build_index(data_root=MASTER_DATA_PATH)
    append_to_report(index, report_path=YOUR_REPORT_PATH)
    write_index(index, data_root=MASTER_DATA_PATH)

Standalone:
    python build_freshness_index.py --data-root /path/to/MASTER_DATA
    python build_freshness_index.py --data-root /path/to/MASTER_DATA --check
"""

import sys
import re
import json
import argparse
from pathlib import Path
from datetime import datetime, timezone
from collections import defaultdict

import pandas as pd

BUFFER_DAYS = 3


# ── Core: last valid timestamp from CSV ────────────────────────────────────────

def _last_valid_ts(csv_path: Path) -> "pd.Timestamp | None":
    """
    Read last valid timestamp from a RESEARCH CSV.
    Does NOT use mtime — reads actual data to guard against partial updates.
    Returns None on any failure.

    IMPORTANT: Data is stored in ISO format (YYYY-MM-DD HH:MM:SS).
    NEVER pass dayfirst=True to pd.to_datetime here — it silently corrupts
    ISO timestamps by swapping day and month (e.g. 2026-01-12 -> Dec 1, 2026;
    day > 12 -> NaT). The bug is undetectable without a sanity check.
    """
    try:
        df    = pd.read_csv(csv_path, comment="#")
        t_col = "time" if "time" in df.columns else "timestamp"
        ts    = pd.to_datetime(df[t_col], errors="coerce")
        ts    = ts[ts.notna()]
        ts    = ts[(ts > "2010-01-01") & (ts < pd.Timestamp.now())]
        if ts.empty:
            return None
        latest = ts.max()
        # Sanity check: if max parsed timestamp is in the future, parsing went wrong.
        # 5-minute buffer absorbs clock drift and ingestion timing edge cases
        # while still catching real errors (dayfirst bug fails by months, not minutes).
        now = pd.Timestamp.utcnow().tz_localize(None)
        if latest > now + pd.Timedelta(minutes=5):
            raise ValueError(
                f"Parsed timestamp {latest} exceeds current time — "
                f"likely a dayfirst/format parse error in {csv_path.name}"
            )
        return latest
    except Exception:
        return None


# ── Core: build index ──────────────────────────────────────────────────────────

def build_index(data_root: Path, buffer_days: int = BUFFER_DAYS) -> dict:
    """
    Scan all *_MASTER/RESEARCH directories under data_root.

    Two-phase:
      Phase 1 — glob *.meta.json for fast discovery (no CSV reads)
      Phase 2 — read actual last valid timestamp from latest year CSV per (sym, broker, tf)
               — read first valid timestamp from earliest year CSV (nrows=1, negligible cost)

    Returns index dict with entries, errors, and metadata.
    """
    today   = pd.Timestamp.now().normalize()
    entries: dict = {}
    errors:  list = []

    if not data_root.exists():
        return {"error": f"MASTER_DATA not found: {data_root}"}

    # Phase 1: Discovery
    groups: dict = defaultdict(dict)  # (sym, broker, tf) -> {year: csv_path}

    for master_dir in sorted(data_root.glob("*_MASTER")):
        if not master_dir.is_dir():
            continue

        base  = master_dir.name[: -len("_MASTER")]
        parts = base.split("_")
        if len(parts) < 2:
            continue
        broker = parts[-1]
        sym    = "_".join(parts[:-1])

        research_dir = master_dir / "RESEARCH"
        if not research_dir.exists():
            continue

        for meta in research_dir.glob("*.meta.json"):
            m = re.search(r"_([^_]+)_(\d{4})_RESEARCH\.csv\.meta\.json$", meta.name)
            if not m:
                continue
            tf   = m.group(1)
            year = int(m.group(2))

            csv_path = research_dir / meta.name.replace(".meta.json", "")
            if not csv_path.exists():
                continue

            key = (sym, broker, tf)
            if year not in groups[key] or year > max(groups[key]):
                groups[key][year] = csv_path

    # Phase 2: Truth read
    for (sym, broker, tf), year_map in sorted(groups.items()):
        latest_csv  = year_map[max(year_map)]
        earliest_csv = year_map[min(year_map)]

        ts = _last_valid_ts(latest_csv)
        if ts is None:
            errors.append(f"{sym}_{broker}_{tf}: unreadable — {latest_csv.name}")
            continue

        # first_date: nrows=1 read on earliest year file — negligible cost
        # Older RESEARCH files use tz-aware timestamps (+00:00) — strip before compare.
        first_date: str | None = None
        try:
            df_first = pd.read_csv(earliest_csv, nrows=1, comment="#")
            t_col    = "time" if "time" in df_first.columns else "timestamp"
            ft       = pd.to_datetime(df_first[t_col].iloc[0], errors="coerce")
            if not pd.isna(ft):
                if ft.tzinfo is not None:
                    ft = ft.tz_convert("UTC").tz_localize(None)
                # Lower bound: 1970-01-02 blocks epoch zeros only.
                # Do NOT use 2010-01-01 here — many symbols have valid history back to 1993+.
                if ft > pd.Timestamp("1970-01-02"):
                    first_date = ft.strftime("%Y-%m-%d")
        except Exception:
            pass  # first_date stays None — non-fatal

        days_behind = max(0, (today - ts.normalize()).days)
        entries[f"{sym}_{broker}_{tf}"] = {
            "first_date":  first_date,
            "latest_date": ts.strftime("%Y-%m-%d"),
            "days_behind": days_behind,
            "source_file": latest_csv.name,
        }

    return {
        "generated_at": datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "buffer_days":  buffer_days,
        "entries":      entries,
        **({"errors": errors} if errors else {}),
    }


# ── Write index to disk ────────────────────────────────────────────────────────

def write_index(index: dict, data_root: Path) -> Path:
    """Write freshness_index.json into data_root. Returns path written."""
    out = data_root / "freshness_index.json"
    out.write_text(json.dumps(index, indent=2), encoding="utf-8")
    return out


# ── Append section to existing ingestion report ────────────────────────────────

def append_to_report(index: dict, report_path: Path) -> None:
    """
    Append a DATA FRESHNESS section to an existing ingestion report file.
    Appends plain text — works with any text-based report format.
    """
    buffer  = index.get("buffer_days", BUFFER_DAYS)
    gen     = index.get("generated_at", "")[:10]
    entries = index.get("entries", {})
    errors  = index.get("errors", [])

    stale  = {k: v for k, v in entries.items() if v["days_behind"] > buffer}
    fresh  = len(entries) - len(stale)
    total  = len(entries)

    lines = [
        "",
        "=" * 60,
        f"DATA FRESHNESS REPORT  (generated: {gen}, buffer: {buffer}d)",
        "=" * 60,
        f"  Total combinations : {total}",
        f"  Fresh              : {fresh}",
        f"  Stale              : {len(stale)}",
    ]

    if stale:
        lines.append("")
        lines.append("  STALE SYMBOLS:")
        for key, v in sorted(stale.items(), key=lambda x: -x[1]["days_behind"]):
            lines.append(
                f"    {key:<32}  last: {v['latest_date']}   "
                f"{v['days_behind']}d behind"
            )
            lines.append(f"      source: {v['source_file']}")
        lines.append("")
        lines.append("  -> Verify data ingress for the above symbols.")
    else:
        lines.append(f"  All {total} combinations current.")

    if errors:
        lines.append("")
        lines.append(f"  ERRORS ({len(errors)}):")
        for e in errors:
            lines.append(f"    {e}")

    lines.append("=" * 60)
    lines.append("")

    with open(report_path, "a", encoding="utf-8") as f:
        f.write("\n".join(lines))


# ── CLI ────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-root", required=True, help="Path to MASTER_DATA directory")
    parser.add_argument("--report",    default=None,  help="Append freshness section to this report file")
    parser.add_argument("--check",     action="store_true", help="Print report only — do not write index")
    args = parser.parse_args()

    data_root = Path(args.data_root)
    print(f"[freshness] Scanning {data_root} ...")
    index = build_index(data_root)

    if "error" in index:
        print(f"[ERROR] {index['error']}")
        sys.exit(1)

    n     = len(index["entries"])
    stale = sum(1 for v in index["entries"].values() if v["days_behind"] > BUFFER_DAYS)

    if not args.check:
        out = write_index(index, data_root)
        print(f"[freshness] Index written -> {out}")

    if args.report:
        append_to_report(index, Path(args.report))
        print(f"[freshness] Freshness section appended -> {args.report}")

    if stale:
        print(f"[freshness] {stale} stale, {n - stale} current.")
        for key, v in sorted(index["entries"].items(), key=lambda x: -x[1]["days_behind"]):
            if v["days_behind"] > BUFFER_DAYS:
                print(f"  ⚠  {key:<32} last: {v['latest_date']}  {v['days_behind']}d")
    else:
        print(f"[freshness] All {n} combinations current (within {BUFFER_DAYS}d).")

    if index.get("errors"):
        print(f"\n[WARN] {len(index['errors'])} unreadable:")
        for e in index["errors"]:
            print(f"  {e}")

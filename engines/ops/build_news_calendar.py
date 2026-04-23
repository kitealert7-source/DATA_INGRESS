"""
build_news_calendar.py — News Calendar RAW → CLEAN → RESEARCH Pipeline
=======================================================================
Processes manually-placed ForexFactory CSV exports into the canonical
EXTERNAL_DATA/NEWS_CALENDAR/{RAW,CLEAN,RESEARCH} structure.

RAW:      Snapshot-based, never overwritten. Named FOREXFACTORY_<year>_<date>.csv
CLEAN:    Normalized timestamps (US/Eastern → UTC-naive), deduped, validated.
RESEARCH: Pass-through from CLEAN with manifest linkage for staleness chain.

Usage:
    python engines/ops/build_news_calendar.py           # standard run
    python engines/ops/build_news_calendar.py --force    # force full rebuild
"""

import os
import sys
import re
import json
import hashlib
import logging
from pathlib import Path
from datetime import datetime, timezone
from dataclasses import dataclass, field
from typing import List, Dict, Optional

import pandas as pd

# ── Path setup ───────────────────────────────────────────────
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "config"))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "core"))

from path_config import (
    NEWS_CALENDAR_RAW, NEWS_CALENDAR_CLEAN, NEWS_CALENDAR_RESEARCH,
    NEWS_CALENDAR,
)

# ── Logging ──────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [NEWS-CAL] %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

# ── Constants ────────────────────────────────────────────────
CALENDAR_TIMEZONE = "US/Eastern"
VALID_IMPACTS = frozenset({"High", "Medium", "Low"})
KNOWN_CURRENCIES = frozenset({
    "USD", "EUR", "GBP", "JPY", "AUD", "NZD", "CAD", "CHF",
    "CNY", "HKD", "SGD", "NOK", "SEK", "ZAR", "MXN", "TRY",
    "KRW", "INR", "BRL", "PLN", "CZK", "HUF", "ILS", "DKK",
    "CLP", "COP", "IDR", "MYR", "PHP", "THB", "TWD",
    "ALL",  # ForexFactory uses "ALL" for multi-currency events
})

# ForexFactory RAW file pattern: FOREXFACTORY_<year>_<snapshot_date>.csv
RAW_FILE_PATTERN = re.compile(
    r"^FOREXFACTORY_(\d{4})_(\d{4}-\d{2}-\d{2})\.csv$"
)


# ═══════════════════════════════════════════════════════════════
# SHA256 — reuse existing pattern (skip comment lines)
# ═══════════════════════════════════════════════════════════════

def compute_file_sha256(filepath):
    """Compute SHA256 of file contents, skipping comment lines."""
    h = hashlib.sha256()
    with open(filepath, "rb") as f:
        for line in f:
            if not line.startswith(b"#"):
                h.update(line)
    return h.hexdigest()


# ═══════════════════════════════════════════════════════════════
# Validator — follows ValidationResult interface from SOP17
# ═══════════════════════════════════════════════════════════════

@dataclass
class NewsValidationMetrics:
    events_total: int = 0
    duplicates_removed: int = 0
    null_timestamps: int = 0
    invalid_impacts: int = 0
    invalid_currencies: int = 0
    unusable_times: int = 0


@dataclass
class NewsValidationResult:
    file: str
    status: str   # "PASS", "FAIL", "WARN"
    metrics: NewsValidationMetrics
    valid: bool = True
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)


class NewsCalendarValidator:
    """Validates CLEAN/RESEARCH news calendar CSVs."""

    REQUIRED_COLUMNS = {"datetime_utc", "currency", "impact", "event", "source"}

    @staticmethod
    def validate(filepath: str) -> NewsValidationResult:
        filename = os.path.basename(filepath)
        metrics = NewsValidationMetrics()
        result = NewsValidationResult(
            file=filename, status="PASS", metrics=metrics
        )

        try:
            df = pd.read_csv(filepath, encoding="utf-8")
        except Exception as e:
            result.valid = False
            result.status = "FAIL"
            result.errors.append(f"Cannot read file: {e}")
            return result

        # Column presence
        missing = NewsCalendarValidator.REQUIRED_COLUMNS - set(df.columns)
        if missing:
            result.valid = False
            result.status = "FAIL"
            result.errors.append(f"Missing columns: {missing}")
            return result

        metrics.events_total = len(df)

        if metrics.events_total == 0:
            result.valid = False
            result.status = "FAIL"
            result.errors.append("File contains zero events")
            return result

        # Parse and check datetime
        df["datetime_utc"] = pd.to_datetime(df["datetime_utc"], errors="coerce")
        null_count = df["datetime_utc"].isna().sum()
        metrics.null_timestamps = int(null_count)
        if null_count > 0:
            result.warnings.append(f"{null_count} null timestamps detected")

        # Timezone check — must be UTC-naive
        if df["datetime_utc"].dt.tz is not None:
            result.valid = False
            result.status = "FAIL"
            result.errors.append("datetime_utc has timezone info — must be UTC-naive")
            return result

        # Year range sanity
        valid_dates = df["datetime_utc"].dropna()
        if len(valid_dates) > 0:
            min_year = valid_dates.min().year
            max_year = valid_dates.max().year
            if min_year < 2000:
                result.valid = False
                result.status = "FAIL"
                result.errors.append(f"Min year {min_year} < 2000 — likely parse error")
            if max_year > datetime.now().year + 2:
                result.warnings.append(f"Max year {max_year} is far in the future")

        # Impact values
        invalid_impact = ~df["impact"].isin(VALID_IMPACTS)
        metrics.invalid_impacts = int(invalid_impact.sum())
        if metrics.invalid_impacts > 0:
            result.warnings.append(
                f"{metrics.invalid_impacts} rows with non-standard impact values"
            )

        # Currency values
        invalid_ccy = ~df["currency"].str.upper().isin(KNOWN_CURRENCIES)
        metrics.invalid_currencies = int(invalid_ccy.sum())
        if metrics.invalid_currencies > 0:
            result.warnings.append(
                f"{metrics.invalid_currencies} rows with unknown currency codes"
            )

        # Duplicate check
        dup_count = df.duplicated(
            subset=["datetime_utc", "currency", "event"], keep="first"
        ).sum()
        metrics.duplicates_removed = int(dup_count)
        if dup_count > 0:
            result.warnings.append(f"{dup_count} duplicate events detected")

        return result


# ═══════════════════════════════════════════════════════════════
# Manifest helpers — aligned with existing _manifest.json pattern
# ═══════════════════════════════════════════════════════════════

def _write_manifest_atomic(manifest_path: str, manifest: dict):
    """Write manifest JSON atomically via .tmp + os.replace()."""
    tmp_path = manifest_path + ".tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)
    os.replace(tmp_path, manifest_path)


def _write_raw_manifest(raw_path: str, year: int, snapshot_date: str):
    """Write RAW manifest after file placement."""
    try:
        df = pd.read_csv(raw_path, encoding="utf-8")
        # Extract Date column range hint (unparsed, for debugging)
        date_hint = "Date column only (unparsed, US/Eastern)"
        if "Date" in df.columns:
            dates = df["Date"].dropna().astype(str)
            if len(dates) > 0:
                date_hint = f"{dates.iloc[0]} ... {dates.iloc[-1]} (unparsed, US/Eastern)"

        manifest = {
            "schema_version": "1.0.0",
            "source": "FOREXFACTORY",
            "year": year,
            "snapshot_date": snapshot_date,
            "row_count": len(df),
            "first_timestamp": None,
            "last_timestamp": None,
            "raw_time_range_hint": date_hint,
            "columns": list(df.columns),
            "sha256": compute_file_sha256(raw_path),
            "generated_utc": datetime.now(timezone.utc).isoformat(),
        }
        _write_manifest_atomic(raw_path + "_manifest.json", manifest)
        logger.info(f"  RAW manifest written: {os.path.basename(raw_path)}")
    except Exception as e:
        logger.warning(f"  RAW manifest write failed: {e}")


def _write_clean_manifest(clean_path: str, raw_paths: list):
    """Write CLEAN manifest with RAW linkage for staleness detection."""
    try:
        df = pd.read_csv(clean_path, encoding="utf-8")
        df["datetime_utc"] = pd.to_datetime(df["datetime_utc"])

        # RAW linkage — composite hash of all source RAW files
        raw_hashes = {}
        raw_total_rows = 0
        for rp in sorted(raw_paths):
            raw_hashes[os.path.basename(rp)] = compute_file_sha256(rp)
            raw_total_rows += sum(1 for _ in open(rp, encoding="utf-8")) - 1

        manifest = {
            "schema_version": "1.0.0",
            "clean_sha256": compute_file_sha256(clean_path),
            "row_count": len(df),
            "first_timestamp": str(df["datetime_utc"].min())[:19] if len(df) > 0 else None,
            "last_timestamp": str(df["datetime_utc"].max())[:19] if len(df) > 0 else None,
            "impact_distribution": df["impact"].value_counts().to_dict() if "impact" in df.columns else {},
            "currencies": sorted(df["currency"].unique().tolist()) if "currency" in df.columns else [],
            "raw_sha256": hashlib.sha256(
                json.dumps(raw_hashes, sort_keys=True).encode()
            ).hexdigest(),
            "raw_row_count": raw_total_rows,
            "raw_source_files": list(raw_hashes.keys()),
            "generated_utc": datetime.now(timezone.utc).isoformat(),
        }
        _write_manifest_atomic(clean_path + "_manifest.json", manifest)
        logger.info(f"  CLEAN manifest written: {os.path.basename(clean_path)}")
    except Exception as e:
        logger.warning(f"  CLEAN manifest write failed: {e}")


def _write_research_manifest(research_path: str, clean_path: str):
    """Write RESEARCH manifest with CLEAN linkage for staleness detection."""
    try:
        clean_manifest_path = clean_path + "_manifest.json"
        clean_sha = None
        clean_row_count = None

        if os.path.exists(clean_manifest_path):
            with open(clean_manifest_path, "r", encoding="utf-8") as f:
                clean_m = json.load(f)
            clean_sha = clean_m.get("clean_sha256")
            clean_row_count = clean_m.get("row_count")
        else:
            clean_sha = compute_file_sha256(clean_path)
            clean_df = pd.read_csv(clean_path, encoding="utf-8")
            clean_row_count = len(clean_df)

        manifest = {
            "clean_sha256": clean_sha,
            "clean_row_count": clean_row_count,
            "research_sha256": compute_file_sha256(research_path),
            "generated_utc": datetime.now(timezone.utc).isoformat(),
        }
        _write_manifest_atomic(research_path + "_manifest.json", manifest)
        logger.info(f"  RESEARCH manifest written: {os.path.basename(research_path)}")
    except Exception as e:
        logger.warning(f"  RESEARCH manifest write failed: {e}")


# ═══════════════════════════════════════════════════════════════
# Staleness detection — aligned with existing pattern
# ═══════════════════════════════════════════════════════════════

def _check_raw_staleness(raw_paths: list, clean_path: str) -> bool:
    """Check if CLEAN is stale relative to current RAW files.

    Computes a composite hash of all RAW files and compares against
    the raw_sha256 recorded in CLEAN's manifest.
    Returns True if CLEAN needs rebuild.
    """
    clean_manifest_path = clean_path + "_manifest.json"
    if not os.path.exists(clean_manifest_path):
        return True  # No manifest → must rebuild

    try:
        with open(clean_manifest_path, "r", encoding="utf-8") as f:
            clean_m = json.load(f)

        # Compute current RAW composite hash
        raw_hashes = {}
        for rp in sorted(raw_paths):
            raw_hashes[os.path.basename(rp)] = compute_file_sha256(rp)
        current_raw_sha = hashlib.sha256(
            json.dumps(raw_hashes, sort_keys=True).encode()
        ).hexdigest()

        if clean_m.get("raw_sha256") != current_raw_sha:
            return True

    except Exception:
        return True  # On error, assume stale

    return False


def _check_clean_staleness(clean_path: str, research_path: str) -> bool:
    """Check if RESEARCH is stale relative to current CLEAN file.

    Compares CLEAN sha256 in RESEARCH manifest against actual CLEAN manifest.
    Returns True if RESEARCH needs rebuild.
    """
    clean_manifest_path = clean_path + "_manifest.json"
    research_manifest_path = research_path + "_manifest.json"

    if not os.path.exists(research_manifest_path):
        return True

    try:
        with open(clean_manifest_path, "r", encoding="utf-8") as f:
            clean_m = json.load(f)
        with open(research_manifest_path, "r", encoding="utf-8") as f:
            res_m = json.load(f)

        if res_m.get("clean_sha256") != clean_m.get("clean_sha256"):
            return True
        if res_m.get("clean_row_count") != clean_m.get("row_count"):
            return True

    except Exception:
        return True

    return False


# ═══════════════════════════════════════════════════════════════
# RAW Phase — detect and catalog manually-placed files
# ═══════════════════════════════════════════════════════════════

def discover_raw_files() -> Dict[int, List[Path]]:
    """Discover RAW files grouped by calendar year.

    Returns dict: {year: [path1, path2, ...]} sorted by snapshot date
    (newest last, so newest wins on dedup with keep='last' if needed).
    """
    raw_dir = Path(NEWS_CALENDAR_RAW)
    if not raw_dir.exists():
        return {}

    files_by_year: Dict[int, List[Path]] = {}
    for f in sorted(raw_dir.glob("*.csv")):
        match = RAW_FILE_PATTERN.match(f.name)
        if not match:
            logger.warning(f"  Skipping non-conforming RAW file: {f.name}")
            continue
        year = int(match.group(1))
        files_by_year.setdefault(year, []).append(f)

    return files_by_year


def ensure_raw_manifests(files_by_year: Dict[int, List[Path]]):
    """Write RAW manifests for any files that lack them."""
    for year, paths in files_by_year.items():
        for p in paths:
            manifest_path = str(p) + "_manifest.json"
            if not os.path.exists(manifest_path):
                match = RAW_FILE_PATTERN.match(p.name)
                snapshot_date = match.group(2) if match else "unknown"
                _write_raw_manifest(str(p), year, snapshot_date)


# ═══════════════════════════════════════════════════════════════
# CLEAN Phase — normalize, deduplicate, partition by year
# ═══════════════════════════════════════════════════════════════

def _parse_forexfactory_raw(raw_paths: List[Path]) -> Optional[pd.DataFrame]:
    """Read and concatenate ForexFactory RAW CSVs into a single DataFrame."""
    frames = []
    for raw_path in raw_paths:
        try:
            df = pd.read_csv(raw_path, encoding="utf-8")
            if len(df) == 0:
                continue
            # Normalize column names (strip whitespace)
            df.columns = [c.strip() for c in df.columns]
            # Tag source file
            df["_source_file"] = raw_path.name
            # Extract year from filename for dateless format fallback
            match = RAW_FILE_PATTERN.match(raw_path.name)
            if match:
                df["_source_year"] = int(match.group(1))
            frames.append(df)
        except Exception as e:
            logger.error(f"  Failed to read {raw_path.name}: {e}")

    if not frames:
        return None
    return pd.concat(frames, ignore_index=True)


def _normalize_timestamps_clean(df: pd.DataFrame) -> pd.DataFrame:
    """Parse ForexFactory Date+Time → US/Eastern → UTC → strip timezone.

    This is the ONLY place timezone conversion happens. Downstream
    (RESEARCH, report layer) must NOT re-normalize.
    """
    df = df.copy()

    # --- Case 1: pre-normalized datetime_utc column ---
    if "datetime_utc" in df.columns:
        df["datetime_utc"] = pd.to_datetime(
            df["datetime_utc"], errors="coerce", utc=True
        )
        df["datetime_utc"] = df["datetime_utc"].dt.tz_localize(None)
        return df.dropna(subset=["datetime_utc"])

    # --- Case 2: Date + Time columns (ForexFactory format) ---
    if "Date" not in df.columns or "Time" not in df.columns:
        logger.error("  Missing 'Date' and/or 'Time' columns")
        return df

    # Forward-fill Date for same-day events (ForexFactory leaves blank)
    df["Date"] = df["Date"].ffill()

    # Drop unusable Time values
    time_lower = df["Time"].astype(str).str.strip().str.lower()
    usable = ~time_lower.isin(["all day", "tentative", "", "nan", "none"])
    dropped_count = int((~usable).sum())
    if dropped_count > 0:
        logger.info(f"  Dropped {dropped_count} rows with unusable Time values")
    df = df[usable].copy()

    if len(df) == 0:
        return df

    # Normalize Time: add space before am/pm (e.g. "1:45pm" → "1:45 pm")
    # dateutil cannot parse "1:45pm" but handles "1:45 pm" correctly
    time_fixed = df["Time"].astype(str).str.strip().str.replace(
        r"(\d)(am|pm)", r"\1 \2", regex=True
    )

    # Combine Date + Time and parse
    # format='mixed' required: dateutil infers different sub-formats across
    # the Series (day-of-week prefix, am/pm suffix) — without it, pandas
    # picks one format and silently NaTs everything that doesn't match.
    dt_combined = df["Date"].astype(str).str.strip() + " " + time_fixed
    parsed = pd.to_datetime(dt_combined, errors="coerce", format="mixed")

    # Fallback: if >50% failed and source_year available, try appending year
    if parsed.isna().mean() > 0.5 and "_source_year" in df.columns:
        dt_with_year = dt_combined + ", " + df["_source_year"].astype(str)
        parsed = pd.to_datetime(dt_with_year, errors="coerce", format="mixed")

    # Localize US/Eastern → convert to UTC → strip timezone
    localized = parsed.dt.tz_localize(
        CALENDAR_TIMEZONE, ambiguous="NaT", nonexistent="NaT"
    )
    df["datetime_utc"] = localized.dt.tz_convert("UTC").dt.tz_localize(None)

    before = len(df)
    df = df.dropna(subset=["datetime_utc"])
    failed = before - len(df)
    if failed > 0:
        logger.info(f"  Dropped {failed} rows with unparseable timestamps")

    return df


def build_clean(files_by_year: Dict[int, List[Path]], force: bool = False) -> List[str]:
    """Build CLEAN layer from RAW files. Returns list of CLEAN file paths built."""
    all_raw_paths = []
    for paths in files_by_year.values():
        all_raw_paths.extend(paths)

    if not all_raw_paths:
        logger.info("No RAW files found — skipping CLEAN build")
        return []

    # Parse all RAW files into one DataFrame
    raw_df = _parse_forexfactory_raw(all_raw_paths)
    if raw_df is None or len(raw_df) == 0:
        logger.warning("No data parsed from RAW files")
        return []

    # Normalize timestamps
    normalized = _normalize_timestamps_clean(raw_df)
    if len(normalized) == 0:
        logger.warning("No valid timestamps after normalization")
        return []

    # Normalize Impact
    if "Impact" in normalized.columns:
        normalized["impact"] = (
            normalized["Impact"].astype(str).str.strip().str.capitalize()
        )
    elif "impact" in normalized.columns:
        normalized["impact"] = (
            normalized["impact"].astype(str).str.strip().str.capitalize()
        )
    else:
        normalized["impact"] = "Unknown"

    # Normalize Currency
    if "Currency" in normalized.columns:
        normalized["currency"] = (
            normalized["Currency"].astype(str).str.strip().str.upper()
        )
    elif "currency" in normalized.columns:
        normalized["currency"] = (
            normalized["currency"].astype(str).str.strip().str.upper()
        )
    else:
        normalized["currency"] = "UNK"

    # Normalize Event
    if "Event" in normalized.columns:
        normalized["event"] = normalized["Event"].astype(str).str.strip()
    elif "event" in normalized.columns:
        normalized["event"] = normalized["event"].astype(str).str.strip()
    else:
        normalized["event"] = "Unknown"

    # Source tag
    normalized["source"] = normalized.get("_source_file", "ForexFactory")

    # Filter valid impacts
    valid_mask = normalized["impact"].isin(VALID_IMPACTS)
    invalid_count = int((~valid_mask).sum())
    if invalid_count > 0:
        logger.info(f"  Dropped {invalid_count} rows with invalid Impact values")
    normalized = normalized[valid_mask].copy()

    if len(normalized) == 0:
        logger.warning("No events remaining after impact filter")
        return []

    # Select canonical columns
    clean_df = normalized[
        ["datetime_utc", "currency", "impact", "event", "source"]
    ].copy()

    # Dedup: (datetime_utc, currency, event) — Impact excluded from key
    before_dedup = len(clean_df)
    clean_df = clean_df.drop_duplicates(
        subset=["datetime_utc", "currency", "event"], keep="first"
    )
    deduped = before_dedup - len(clean_df)
    if deduped > 0:
        logger.info(f"  Removed {deduped} duplicate events")

    # Sort by datetime
    clean_df = clean_df.sort_values("datetime_utc").reset_index(drop=True)

    # Partition by year (based on UTC timestamp, not source file year)
    clean_df["_year"] = clean_df["datetime_utc"].dt.year
    years = sorted(clean_df["_year"].unique())

    clean_dir = Path(NEWS_CALENDAR_CLEAN)
    clean_dir.mkdir(parents=True, exist_ok=True)

    built_files = []
    for year in years:
        year_df = clean_df[clean_df["_year"] == year].drop(columns=["_year"]).copy()
        clean_filename = f"NEWS_CALENDAR_{year}_CLEAN.csv"
        clean_path = str(clean_dir / clean_filename)

        # Staleness check: only rebuild if RAW changed
        year_raw_paths = files_by_year.get(year, all_raw_paths)
        if not force and not _check_raw_staleness(
            [str(p) for p in year_raw_paths], clean_path
        ):
            logger.info(f"  [SKIP] {clean_filename} — RAW unchanged")
            built_files.append(clean_path)
            continue

        # Write CLEAN file atomically
        tmp_path = clean_path + ".tmp"
        year_df.to_csv(tmp_path, index=False, encoding="utf-8")

        # Validate before commit
        result = NewsCalendarValidator.validate(tmp_path)
        if not result.valid:
            logger.error(f"  [FAIL] Validation failed for {clean_filename}:")
            for err in result.errors:
                logger.error(f"    {err}")
            os.remove(tmp_path)
            continue

        for warn in result.warnings:
            logger.warning(f"  {clean_filename}: {warn}")

        # Atomic commit
        os.replace(tmp_path, clean_path)
        logger.info(
            f"  [COMMIT] {clean_filename} — {len(year_df)} events"
        )

        # Write CLEAN manifest with RAW linkage
        _write_clean_manifest(clean_path, [str(p) for p in year_raw_paths])
        built_files.append(clean_path)

    return built_files


# ═══════════════════════════════════════════════════════════════
# RESEARCH Phase — pass-through with manifest linkage
# ═══════════════════════════════════════════════════════════════

def build_research(force: bool = False) -> List[str]:
    """Build RESEARCH layer from CLEAN files. Returns list of RESEARCH files built."""
    clean_dir = Path(NEWS_CALENDAR_CLEAN)
    research_dir = Path(NEWS_CALENDAR_RESEARCH)
    research_dir.mkdir(parents=True, exist_ok=True)

    if not clean_dir.exists():
        logger.info("No CLEAN directory — skipping RESEARCH build")
        return []

    clean_files = sorted(clean_dir.glob("NEWS_CALENDAR_*_CLEAN.csv"))
    if not clean_files:
        logger.info("No CLEAN files found — skipping RESEARCH build")
        return []

    built_files = []
    for clean_path in clean_files:
        # Derive RESEARCH filename
        research_filename = clean_path.name.replace("_CLEAN.csv", "_RESEARCH.csv")
        research_path = str(research_dir / research_filename)

        # Staleness check
        if not force and not _check_clean_staleness(str(clean_path), research_path):
            logger.info(f"  [SKIP] {research_filename} — CLEAN unchanged")
            built_files.append(research_path)
            continue

        # Pass-through: copy CLEAN → RESEARCH (no transformation)
        tmp_path = research_path + ".tmp"
        import shutil
        shutil.copy2(str(clean_path), tmp_path)

        # Validate
        result = NewsCalendarValidator.validate(tmp_path)
        if not result.valid:
            logger.error(f"  [FAIL] RESEARCH validation failed for {research_filename}:")
            for err in result.errors:
                logger.error(f"    {err}")
            os.remove(tmp_path)
            continue

        # Atomic commit
        os.replace(tmp_path, research_path)
        logger.info(f"  [COMMIT] {research_filename} — pass-through from CLEAN")

        # Write RESEARCH manifest with CLEAN linkage
        _write_research_manifest(research_path, str(clean_path))
        built_files.append(research_path)

    return built_files


# ═══════════════════════════════════════════════════════════════
# Metadata — top-level metadata.json (aligned with SYSTEM_FACTORS)
# ═══════════════════════════════════════════════════════════════

def write_metadata():
    """Write top-level metadata.json for NEWS_CALENDAR."""
    research_dir = Path(NEWS_CALENDAR_RESEARCH)
    research_files = sorted(research_dir.glob("NEWS_CALENDAR_*_RESEARCH.csv"))

    if not research_files:
        return

    source_hashes = {}
    total_events = 0
    years = []

    for rf in research_files:
        source_hashes[rf.name] = compute_file_sha256(str(rf))
        try:
            df = pd.read_csv(rf, encoding="utf-8")
            total_events += len(df)
            match = re.search(r"_(\d{4})_", rf.name)
            if match:
                years.append(int(match.group(1)))
        except Exception:
            pass

    metadata = {
        "name": "NEWS_CALENDAR",
        "version": "NEWS_CALENDAR_v1.0",
        "source": "ForexFactory",
        "timezone_input": "US/Eastern",
        "timezone_output": "UTC (naive)",
        "impact_levels": ["High", "Medium", "Low"],
        "source_data_hashes": source_hashes,
        "creation_timestamp": datetime.now(timezone.utc).isoformat(),
        "validation_stats": {
            "total_events": total_events,
            "year_range": f"{min(years)}-{max(years)}" if years else "N/A",
            "files_count": len(research_files),
        },
    }

    metadata_path = str(Path(NEWS_CALENDAR) / "metadata.json")
    _write_manifest_atomic(metadata_path, metadata)
    logger.info(f"  Metadata written: {metadata_path}")


# ═══════════════════════════════════════════════════════════════
# Main entry point
# ═══════════════════════════════════════════════════════════════

def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="News Calendar RAW → CLEAN → RESEARCH pipeline"
    )
    parser.add_argument(
        "--force", action="store_true",
        help="Force full rebuild (ignore staleness)"
    )
    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("NEWS CALENDAR PIPELINE — START")
    logger.info(f"  RAW:      {NEWS_CALENDAR_RAW}")
    logger.info(f"  CLEAN:    {NEWS_CALENDAR_CLEAN}")
    logger.info(f"  RESEARCH: {NEWS_CALENDAR_RESEARCH}")
    logger.info(f"  Force:    {args.force}")
    logger.info("=" * 60)

    # Phase 1: Discover and catalog RAW files
    logger.info("\n--- Phase 1: RAW Discovery ---")
    files_by_year = discover_raw_files()
    total_raw = sum(len(v) for v in files_by_year.values())
    if total_raw == 0:
        logger.info("No RAW files found. Nothing to do.")
        logger.info("Place ForexFactory CSVs in: %s", NEWS_CALENDAR_RAW)
        logger.info("Naming: FOREXFACTORY_<year>_<snapshot_date>.csv")
        sys.exit(0)
    logger.info(f"  Found {total_raw} RAW file(s) across {len(files_by_year)} year(s)")

    # Write RAW manifests for new files
    ensure_raw_manifests(files_by_year)

    # Phase 2: CLEAN build
    logger.info("\n--- Phase 2: CLEAN Build ---")
    clean_files = build_clean(files_by_year, force=args.force)
    logger.info(f"  CLEAN: {len(clean_files)} file(s)")

    # Phase 3: RESEARCH build
    logger.info("\n--- Phase 3: RESEARCH Build ---")
    research_files = build_research(force=args.force)
    logger.info(f"  RESEARCH: {len(research_files)} file(s)")

    # Phase 4: Metadata
    logger.info("\n--- Phase 4: Metadata ---")
    write_metadata()

    logger.info("\n" + "=" * 60)
    logger.info("NEWS CALENDAR PIPELINE — COMPLETE")
    logger.info("=" * 60)
    sys.exit(0)


if __name__ == "__main__":
    main()

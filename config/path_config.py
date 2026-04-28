"""
path_config.py — Centralized path derivation for DATA_INGRESS
==============================================================
ALL filesystem roots are derived from this file's physical location.
No hardcoded user paths. Import from here instead of defining paths inline.

Usage (from any file in DATA_INGRESS):
    import sys, os
    sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "config"))
    # -- or, if running from DATA_INGRESS root: --
    from config.path_config import DATA_INGRESS_ROOT, DATA_ROOT, MASTER_DATA

Repo layout assumed:
    <parent>/
        DATA_INGRESS/          ← this repo
        Anti_Gravity_DATA_ROOT/
            MASTER_DATA/
            governance/
"""

from pathlib import Path

# ── Repository Roots ─────────────────────────────────────────────
DATA_INGRESS_ROOT = Path(__file__).resolve().parents[1]          # DATA_INGRESS/
DATA_ROOT         = DATA_INGRESS_ROOT.parent / "Anti_Gravity_DATA_ROOT"

# ── Primary Data Directories ─────────────────────────────────────
MASTER_DATA   = DATA_ROOT / "MASTER_DATA"
EXTERNAL_DATA = DATA_ROOT / "EXTERNAL_DATA"
GOVERNANCE    = DATA_ROOT / "governance"

# ── External Data Sources ────────────────────────────────────
NEWS_CALENDAR          = EXTERNAL_DATA / "NEWS_CALENDAR"
NEWS_CALENDAR_RAW      = NEWS_CALENDAR / "RAW"
NEWS_CALENDAR_CLEAN    = NEWS_CALENDAR / "CLEAN"
NEWS_CALENDAR_RESEARCH = NEWS_CALENDAR / "RESEARCH"

# ── DATA_INGRESS Internal Directories ────────────────────────────
STATE_DIR     = DATA_INGRESS_ROOT / "state"
REPORTS_DIR   = DATA_INGRESS_ROOT / "reports"
LOGS_DIR      = DATA_INGRESS_ROOT / "logs"
TMP_DIR       = DATA_INGRESS_ROOT / "tmp"

# ── Pipeline Log Subdirectories ──────────────────────────────────
PIPELINE_LOG_DIR  = LOGS_DIR / "DATA_PIPELINE"
PREFLIGHT_LOG_DIR = LOGS_DIR / "PREFLIGHT"
BASELINE_LOG_DIR  = LOGS_DIR / "BASELINE"

# ── Engine Scripts (for subprocess calls in daily_pipeline.py) ───
ENGINES_CORE = DATA_INGRESS_ROOT / "engines" / "core"
ENGINES_OPS  = DATA_INGRESS_ROOT / "engines" / "ops"

# ── Governance File ──────────────────────────────────────────────
GOVERNANCE_FILE = GOVERNANCE / "last_successful_daily_run.json"

# ── Asset-Specific Shorthands ────────────────────────────────────
XAUUSD_MASTER = MASTER_DATA / "XAUUSD_OCTAFX_MASTER"
US10Y_MASTER  = MASTER_DATA / "US10Y_YAHOO_MASTER"
BTC_DELTA     = MASTER_DATA / "BTC_DELTA_MASTER"
ETH_DELTA     = MASTER_DATA / "ETH_DELTA_MASTER"
BTC_OCTAFX    = MASTER_DATA / "BTC_OCTAFX_MASTER"
ETH_OCTAFX    = MASTER_DATA / "ETHUSD_OCTAFX_MASTER"


def as_str(p: Path) -> str:
    """Convert Path to str for os.path.join compatibility."""
    return str(p)


# ── Canonical MASTER_DATA directory registry ─────────────────────────────────
# Every *_MASTER directory the pipeline is authorised to write into must be
# listed here. If the pipeline resolves a target path that is NOT in this set,
# it is a configuration error — not a runtime decision.
#
# Enforcement: call assert_canonical_master_dir(path) before any write.
# Directories prefixed with "archive__" are explicitly excluded from scanning
# and write assertions; they are read-only forensic artifacts.

CANONICAL_MASTER_DIRS: frozenset[str] = frozenset({
    # Forex — major
    "EURUSD_OCTAFX_MASTER",
    "GBPUSD_OCTAFX_MASTER",
    "USDJPY_OCTAFX_MASTER",
    "USDCHF_OCTAFX_MASTER",
    "AUDUSD_OCTAFX_MASTER",
    "NZDUSD_OCTAFX_MASTER",
    "USDCAD_OCTAFX_MASTER",
    # Forex — cross
    "GBPAUD_OCTAFX_MASTER",
    "GBPNZD_OCTAFX_MASTER",
    "AUDNZD_OCTAFX_MASTER",
    "EURAUD_OCTAFX_MASTER",
    "EURJPY_OCTAFX_MASTER",
    "GBPJPY_OCTAFX_MASTER",
    "CHFJPY_OCTAFX_MASTER",
    "AUDJPY_OCTAFX_MASTER",
    "NZDJPY_OCTAFX_MASTER",
    "CADJPY_OCTAFX_MASTER",
    "EURGBP_OCTAFX_MASTER",
    # Commodities
    "XAUUSD_OCTAFX_MASTER",
    # Indices
    "NAS100_OCTAFX_MASTER",
    "SPX500_OCTAFX_MASTER",
    "GER40_OCTAFX_MASTER",
    "AUS200_OCTAFX_MASTER",
    "UK100_OCTAFX_MASTER",
    "FRA40_OCTAFX_MASTER",
    "ESP35_OCTAFX_MASTER",
    "EUSTX50_OCTAFX_MASTER",
    "US30_OCTAFX_MASTER",
    "JPN225_OCTAFX_MASTER",
    # Crypto — OctaFX (MT5)
    "BTCUSD_OCTAFX_MASTER",
    "ETHUSD_OCTAFX_MASTER",
    # Crypto — BTC_OCTAFX (legacy short-name, active for BTC)
    "BTC_OCTAFX_MASTER",
    # Crypto — Delta Exchange
    "BTC_DELTA_MASTER",
    "ETH_DELTA_MASTER",
    # External data
    "US10Y_YAHOO_MASTER",
})


def assert_canonical_master_dir(target_path: Path) -> None:
    """Hard-fail if target_path resolves to an unregistered *_MASTER directory.

    Call this before any pipeline write that resolves a MASTER_DATA subdirectory
    at runtime. Prevents silent dataset forks like the ETH_OCTAFX_MASTER incident
    (Apr 2026) where a mis-named directory accumulated 6 days of orphaned data.

    Directories starting with 'archive__' are silently ignored (read-only).

    Raises:
        RuntimeError: if the resolved *_MASTER directory is not canonical.
    """
    for part in target_path.parts:
        if part.endswith("_MASTER"):
            if part.startswith("archive__"):
                return
            if part not in CANONICAL_MASTER_DIRS:
                raise RuntimeError(
                    f"[PATH INVARIANT] '{part}' is not a registered MASTER_DATA "
                    f"directory. Add it to CANONICAL_MASTER_DIRS in path_config.py "
                    f"if this is intentional, or fix the target path. "
                    f"(Unregistered directories cause silent dataset forks.)"
                )
            return


# ── Coverage Contract (RAW completeness invariant) ────────────────────────────
# After Phase 1 RAW Update, every (sym_broker, tf) tuple in EXPECTED_COVERAGE
# must have a 2026 RAW file with last_ts >= today_utc - COVERAGE_MAX_DAYS_BEHIND[tf].
# A passing Phase 1 with missing tuples = silent skip = bug. Enforced by
# engines/ops/assert_raw_coverage.py, called as Phase 1.5 of daily_pipeline.py.
#
# Background: 2026-04-28 incident — 28 (symbol, tf) combos at 1m/5m silently
# returned None from MT5.copy_rates_from() because the symbols weren't subscribed
# in Market Watch. Phase 1 reported [PASS] (no crash), but no metric entries
# existed for the affected tuples. Detected only 24h later via a manual
# freshness scan. This contract closes that gap by promoting "phase didn't
# crash" to "every expected output exists and is fresh."
#
# Adding a symbol or timeframe REQUIRES updating this contract. That is the
# whole point — the inventory is auditable and review-gated.

# Standard timeframe sets per asset class
_TFS_FX_INDEX           = ("1m", "5m", "15m", "30m", "1h", "4h", "1d")
_TFS_CRYPTO_OCTAFX_GOLD = ("1m", "3m", "5m", "15m", "30m", "1h", "4h", "1d")
_TFS_CRYPTO_DELTA       = ("1m", "3m", "5m", "15m", "1h", "4h", "1d")  # no 30m

# Symbol sets per asset class (sym_broker without "_MASTER" suffix)
_FX_INDEX_OCTAFX = (
    # Forex majors
    "EURUSD_OCTAFX", "GBPUSD_OCTAFX", "USDJPY_OCTAFX", "USDCHF_OCTAFX",
    "AUDUSD_OCTAFX", "NZDUSD_OCTAFX", "USDCAD_OCTAFX",
    # Forex crosses
    "GBPAUD_OCTAFX", "GBPNZD_OCTAFX", "AUDNZD_OCTAFX", "EURAUD_OCTAFX",
    "EURJPY_OCTAFX", "GBPJPY_OCTAFX", "CHFJPY_OCTAFX", "AUDJPY_OCTAFX",
    "NZDJPY_OCTAFX", "CADJPY_OCTAFX", "EURGBP_OCTAFX",
    # Index CFDs
    "NAS100_OCTAFX", "SPX500_OCTAFX", "GER40_OCTAFX", "AUS200_OCTAFX",
    "UK100_OCTAFX",  "FRA40_OCTAFX",  "ESP35_OCTAFX", "EUSTX50_OCTAFX",
    "US30_OCTAFX",   "JPN225_OCTAFX",
)
_CRYPTO_GOLD_OCTAFX = (
    "BTCUSD_OCTAFX", "ETHUSD_OCTAFX", "XAUUSD_OCTAFX",
    # NOTE: BTC_OCTAFX_MASTER is intentionally absent. It is a Windows
    # directory junction aliased to BTCUSD_OCTAFX_MASTER (verified 2026-04-28
    # — both paths resolve to the same inode). It produces no distinct data,
    # so there is no contract obligation to enforce on it. The freshness_index
    # double-counts it as a side effect of scanning both junction names; that
    # is a separate cosmetic issue, not a coverage gap.
)
_CRYPTO_DELTA_SET = ("BTC_DELTA", "ETH_DELTA")

# US10Y_YAHOO_MASTER is also absent. It ingests via Yahoo Finance with
# native multi-day publishing latency that does not fit the same threshold
# semantics as broker-feeds. If/when it migrates to a real-time source,
# add it here.

EXPECTED_COVERAGE: frozenset[tuple[str, str]] = frozenset(
    [(s, tf) for s in _FX_INDEX_OCTAFX     for tf in _TFS_FX_INDEX] +
    [(s, tf) for s in _CRYPTO_GOLD_OCTAFX  for tf in _TFS_CRYPTO_OCTAFX_GOLD] +
    [(s, tf) for s in _CRYPTO_DELTA_SET    for tf in _TFS_CRYPTO_DELTA]
)
# Sanity: 28 × 7 + 3 × 8 + 2 × 7 = 196 + 24 + 14 = 234
assert len(EXPECTED_COVERAGE) == 234, \
    f"EXPECTED_COVERAGE arithmetic broken: got {len(EXPECTED_COVERAGE)}, expected 234"

# Maximum acceptable days_behind per timeframe before coverage assertion fails.
# Buffer absorbs long weekends + occasional broker holidays. Tight enough that
# a missed weekday's-worth of data still trips the check.
COVERAGE_MAX_DAYS_BEHIND: dict[str, int] = {
    "1m":  5,  "3m":  5,  "5m":  5,  "15m": 5,  "30m": 5,
    "1h":  5,  "4h":  7,  "1d":  7,
}

# Known exceptions: tuples that are temporarily allowed to violate the contract.
# Format: {(sym_broker, tf): "reason — owner — review_date"}.
# Add an entry to silence a specific tuple while a known issue is being fixed.
# Empty by default — the contract is the whole inventory.
COVERAGE_EXCEPTIONS: dict[tuple[str, str], str] = {}

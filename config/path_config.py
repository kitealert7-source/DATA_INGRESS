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

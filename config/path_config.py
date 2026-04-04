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
GOVERNANCE    = DATA_ROOT / "governance"

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
ETH_OCTAFX    = MASTER_DATA / "ETH_OCTAFX_MASTER"


def as_str(p: Path) -> str:
    """Convert Path to str for os.path.join compatibility."""
    return str(p)

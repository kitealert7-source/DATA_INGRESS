# CLAUDE.md — Agent Session Brief (DATA_INGRESS)

## What This System Is

5-phase daily data pipeline. Ingests market data from MT5 and external APIs, validates, cleans, and produces research-ready datasets. Writes all data into Anti_Gravity_DATA_ROOT.

**Six-repo layout (all share same parent directory):**
- `Trade_Scan` — research pipeline (engine code, strategies, regime model)
- `TradeScan_State` — artifact store (portfolio_evaluation, strategy snapshots)
- `TS_Execution` — live execution bridge
- `DATA_INGRESS` (this repo) — data pipeline: RAW → CLEAN → RESEARCH
- `Anti_Gravity_DATA_ROOT` — master data store (governance + datasets)
- `DRY_RUN_VAULT` — dry run archive

**This repo is disposable tooling. All data truth lives in Anti_Gravity_DATA_ROOT.**

---

## Before Acting — Read Protocol

1. **This file** — architecture, path rules, pipeline overview
2. **README.md** — pipeline phases, key files, separation of concerns
3. **DAILY_EXECUTION_CONTRACT.md** — scheduler, preflight, failure semantics
4. **data_ingress_knowledge_base.md** — deep domain knowledge

---

## Pipeline Phases (daily execution order)

| Phase | Script | Responsibility |
|-------|--------|----------------|
| 1 | `engines/core/raw_update_sop17.py` | Incremental RAW ingestion from MT5 + Delta Exchange |
| 1.5 | `engines/ops/assert_raw_coverage.py` | RAW completeness invariant — every (sym, tf) in `EXPECTED_COVERAGE` must be fresh within threshold; hard fail otherwise |
| 2 | `engines/core/dataset_validator_sop17.py` | Structural validation |
| 2.5 | `engines/ops/validate_missing_baseline.py` | Behavioral anomaly detection (statistical drift) |
| 3 | `engines/core/clean_rebuild_sop17.py` | CLEAN layer rebuild from RAW |
| 4 | `engines/core/rebuild_research_sop17.py` | RESEARCH layer rebuild with lineage |
| 4.5 | `engines/ops/validate_research_layer.py` | Post-rebuild RESEARCH integrity check |
| 5 | `engines/ops/build_usd_synth.py` | Synthetic USD dataset construction |
| 5.5 | `engines/ops/build_news_calendar.py` | External macro-event calendar ingest (RAW → CLEAN → RESEARCH) |
| 6 | Governance update | `last_successful_daily_run.json` — only if phases 1–5.5 all pass |

Pipeline entry point: `engines/ops/daily_pipeline.py`
Scheduler entry point: `engines/ops/invoke_preflight.ps1`

---

## Path Authority

`config/path_config.py` — defines every filesystem root. Never hardcode.

### Path Portability Rules (ENFORCED BY PRE-COMMIT HOOK)

**NEVER hardcode absolute user paths** like `C:\Users\faraw\...` or `/home/user/...`. The pre-commit hook (`tools/lint_no_hardcoded_paths.py`) will block any commit that contains them.

**How to derive paths:**
```python
from pathlib import Path

# From engines/core/*.py or engines/ops/*.py:
_DI = Path(__file__).resolve().parents[2]                     # DATA_INGRESS root
_AG = _DI.parent / "Anti_Gravity_DATA_ROOT"                   # data root
MASTER_DATA = _AG / "MASTER_DATA"                             # all datasets

# From reports/*.py:
_DI = Path(__file__).resolve().parents[1]                     # DATA_INGRESS root
_AG = _DI.parent / "Anti_Gravity_DATA_ROOT"                   # data root
```

**Or import from centralized config:**
```python
from config.path_config import DATA_INGRESS_ROOT, DATA_ROOT, MASTER_DATA
```

**Depth rules for this repo:**
| File location | To get DATA_INGRESS root | To get Anti_Gravity parent |
|---|---|---|
| `engines/core/*.py` | `.parents[2]` | `.parents[3]` |
| `engines/ops/*.py` | `.parents[2]` | `.parents[3]` |
| `engines/legacy/*.py` | `.parents[2]` | `.parents[3]` |
| `reports/*.py` | `.parents[1]` | `.parents[2]` |
| `config/*.py` | `.parents[1]` | `.parents[2]` |
| `tools/*.py` | `.parents[1]` | `.parents[2]` |

**Lint check:** `python tools/lint_no_hardcoded_paths.py` — run anytime to scan all active .py files.

**Exempt directories:** `tmp/`, `archive/` — throwaway scripts and retired legacy code are not scanned.

---

## Critical Invariants

1. **Preflight Gate** — no pipeline run without a valid same-day preflight decision token
2. **No Retries** — failed runs require human diagnosis; no automatic recovery
3. **Governance Write-Once** — `last_successful_daily_run.json` only written after all phases pass
4. **Validation Separation** — structural (`dataset_validator_sop17.py`) and behavioral (`validate_missing_baseline.py`) must never be merged
5. **DATA_INGRESS defines no rules** — all governance lives in Anti_Gravity_DATA_ROOT

---

## Key Operational Commands

```bash
# Full daily pipeline
python engines/ops/daily_pipeline.py

# Preflight check only
python engines/ops/preflight_check.py

# RAW update only
python engines/core/raw_update_sop17.py --symbols XAUUSD

# Full audit
python engines/core/dataset_validator_sop17.py --audit-all

# Lint check (path portability)
python tools/lint_no_hardcoded_paths.py
```

---

## Secrets

Delta Exchange API credentials are loaded from `.secrets/delta_api.env` (git-ignored). If absent, Delta ingestion is silently skipped. Telegram alerting requires `TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID` environment variables.

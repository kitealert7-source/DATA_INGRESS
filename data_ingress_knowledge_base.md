# DATA INGRESS AGENT KNOWLEDGE BASE (INFRASTRUCTURE QUIRKS)

*Authoritative supplemental reference for Anti-Gravity data ingestion routines.*

---

## 1. MT5 Broker Deep History Limitations (The "Loop Bypass")

**Problem:** Requesting deep continuous history (e.g. `2014` to `present`) in a single or chunked `copy_rates_from_pos` call on local MT5 terminals often results in a silent rejection, API timeout, or `Call failed` return, especially for Timeframes H1 and higher across all asset classes.
**Solution:** Do **not** use `copy_rates_from` for deep history extraction. Instead, utilize `copy_rates_range` bounded exactly to **1-month increments**.

- Write a `date_range` generator.
- Loop sequentially from the target start date up to `datetime.now()`.
- Extract each month into a pandas dataframe, drop the active boundary overlap, and append.
- This micro-chunking method bypasses the MT5 server request throttles for deep historical tracking.

## 2. Intraday Recontamination (Broker Resampling Artifacts)

**Problem:** When requesting intraday data (`1h`, `4h`) from deep history (e.g. earlier than 2015) from some brokers (such as OctaFX), the MT5 server may erroneously return bars containing intraday timestamps but spaced exactly 1 day apart (`86400s` median delta). This is a backfill resampling artifact that permanently corrupts the structural continuity of the dataset.
**Solution:** `dataset_validator_sop17.py` contains a dynamic resampling artifact check that flags this. To permanently prevent recontamination during automated updates:

- A structural guard exists in `validate_timeframe_delta` within the RAW ingestion pipeline (`raw_update_sop17.py`).
- If `tf_label` is `"1h"` or `"4h"` and the `median_delta_sec` is exactly `86400.0`, the pipeline strictly aborts that specific ingestion loop rather than poisoning the RAW layer.

## 3. Timezone Awareness and RAW Immutability

**Problem:** `pandas` will throw a `TypeError: Cannot compare tz-naive and tz-aware datetime-like objects` if incoming broker data is timezone-naive (`datetime64[ns]`) but compared against timezone-aware history files (e.g. legacy `XAUUSD` datasets with `+00:00` artifacts).
**Solution:** The Anti-Gravity pipelines enforce **strictly Timezone-Naive (UTC-assumed)** processing, but **only starting at the CLEAN layer**.

- **RAW Strict Immutability:** The RAW ingestion layer (`raw_update_sop17.py`) must *never* mutate or standardize broker timestamps. If a dataset historically contains `+00:00` artifacts, the new naive rows are appended, but the old artifacts remain at rest. RAW is append-only.
- **CLEAN Downstream Normalization:** The `clean_rebuild_sop17.py` script is exclusively responsible for interpreting the `RAW` CSV and dynamically stripping any timezone offsets (`.dt.tz_localize(None)`) in-memory before saving the unified, timezone-naive `_CLEAN.csv`.
- Ensure all isolated cutoff dates inside the scripts (e.g., `future_cutoff = pd.Timestamp.utcnow()`) have `.tz_localize(None)` applied before comparison.

## 4. Minimum Row Thresholds & Continuous CFDs

**Problem:** Global validation logic enforcing "minimum row counts per year" (e.g. `8000` bars for `15m`, `4000` bars for `30m`) works perfectly for 24/5 FX pairs. However, continuous Index CFDs (like `ESP35_OCTAFX`) trade shorter daily sessions. They will legitimately fall short of global thresholds (e.g. producing `7983` bars instead of `8000`), failing pipeline validation.
**Solution:** The validation thresholds for `15m` and `30m` within `dataset_validator_sop17.py` have been structurally lowered to `7500` and `3500` respectively to cleanly support these shorter-session assets cleanly without masking actual corruption. Do not arbitrarily raise thresholds without checking CFD daily footprints.

**Problem**: Changing the internal composition of a RAW file (such as a deeper historical merge) or **updating the CLEAN layer schema (e.g., adding the `spread` column)** will organically change the manifest's SHA256 hash. When `rebuild_research_sop17.py` runs, it compares the new CLEAN hash to the existing registry entry. If it diverges, the orchestrator actively aborts with `[REGISTRY VIOLATION]` to prevent silent lineage mutation.
**Solution**: This is intentional, defensive infrastructure behavior.
To clear a legitimate registry block (e.g., after a governance-mandated CLEAN rebuild):

1. Delete the `METADATA/research_registry.json` file.
2. Re-run `rebuild_research_sop17.py` to re-register the new spread-aware baseline.

## 6. Long-Term Historical Data Ingestion (Monthly Strategy)

**Problem:** When building a new authoritative dataset from scratch (e.g. extending a new asset like `XAUUSD` or indexing an old Forex pair back 10+ years), attempting to ingest the entire history in one pass or relying on standard incremental updates is structurally unsafe and prone to API failures.
**Solution:** A dedicated historical acquisition script (e.g., `acquire_<asset>_history.py`) MUST be utilized, following a strict monthly ingestion strategy:

1. **Reverse-Walking Micro-Chunks**: Start from the current date and walk backward in discrete 1-month chunks using `copy_rates_range` until the broker returns zero rows (the absolute inception date).
2. **Intermediate Staging**: Never write directly to the `MASTER_DATA/RAW` directory during a deep history scan. Isolate the output into temporary contiguous files exactly bound to timeframe constraints (e.g., `tmp/XAUUSD_HISTORY`).
3. **Partition & Merge**: Once the full history is staged and locally validated for gaps, partition the unified dataset by year, and append it atomically to the canonical `RAW` file structure.
4. **Preservation**: This method uniquely protects the registry from intermediate hashing chaos and ensures that massive multi-gigabyte historical syncs never mutate existing pipeline invariants.

## 7. Artifact & Infrastructure Filing Discipline

**Problem:** Development, debugging, and auditing often generate one-off scripts, temporary JSON outputs, ad-hoc python files, or raw text logs. Accumulating these in the `DATA_INGRESS` root directory pollutes the canonical reference structure and confuses orchestrator paths.
**Solution:** Strict adherence to proper filing infrastructure is required for all generated artifacts and diagnostic tools:

- **Core Systems**: All permanent pipeline extraction mapping scripts must live in `engines/ops/` (Execution) or `engines/core/` (Logic).
- **Temporary Scripts**: One-off diagnostic scripts (e.g., `audit_d1_integrity.py`) that are run once and no longer used **must not** sit in the root. If they contain valuable logic for future reference, they must be saved under `archive/legacy_audits/` and documented in the `ARCHIVE_INDEX.md`.
- **Outputs & Reports**: Text dumps, JSON structural responses, or `.yaml` reports must be stored strictly in the `tmp/`, `state/`, or `reports/` directories respectively.
- **Root Sanctity**: The `DATA_INGRESS` root is strictly reserved for canonical `.md` profiles, state JSONs, and authoritative orchestrator routing logic.

## 8. OctaFX Execution Governance (v3.1 Architecture)

**Problem**: For OctaFX backtesting, RESEARCH datasets require execution-realistic prices (Bid + Spread), but brokers often provide these as separate columns in RAW.
**Solution**: The Anti-Gravity pipeline enforces a three-stage spread isolation model:

- **CLEAN Layer**: Acts as the authoritative intermediate check. It contains the **calculated/interpolated spread** column (integer points).
- **RESEARCH Layer**: Consumes the CLEAN spread to build **ASK-based OHLC prices**. The metadata `spread` column in RESEARCH is strictly set to `0` to signal to execution engines that costs are already built-in.
- **Enforcement**: `dataset_validator_sop17.py` will hard-fail any RESEARCH partition where `CLEAN OHLC == RESEARCH OHLC` and `Spread == 0` (The "Forbidden State").

## 9. Zero-Spread Backfill (Forward Median Rule)

**Problem**: Historical OctaFX RAW data (especially for Metals and Indices pre-2016) frequently contains 100% zero-spread columns, making the data un-executable according to Section 8.
**Solution**: The `v3.1` engine implements the **Forward Median Method**:

- If a partition is 100% zero-spread, the engine scans future partitions for the same asset/TF to find the nearest valid non-zero spread distribution.
- It extracts the **median** value from the first 5,000 rows of that reference file.
- It injects this median spread into the RESEARCH price-embedding logic while logging a `ZERO_SPREAD_BACKFILL_INJECTION` event.
- This maintains trading realism without introducing lookahead bias (as spread is a cost cost, not a price signal).

## 10. Data Freshness Gate (Silent Staleness Prevention)

**Problem**: The MT5 broker API can fail to return new rows (yielding 0 rows during ingestion) while `dataset_validator_sop17.py`'s internal gap checks pass perfectly because the old data is contiguous up to the stall date. This causes a "silent staleness" where the pipeline reports 100% success on dead datasets.
**Solution**: A wall-clock **Data Freshness Gate** is embedded into the core validation layer (`SOP17Validator._check_freshness`).
- The validator compares `datetime.utcnow()` against the last timestamp of the current year's file.
- Exceeding the threshold triggers a hard `STALE DATA` fail, propagating to the daily report.
- **Thresholds (Wall-Clock Hours)**:
  - `FOREX` & `CRYPTO_CFD`: **72h** limit (covers standard 48h weekends).
  - `INDEX_CFD`: **96h** limit (covers full 75h+ gap from Friday afternoon close to Monday morning).
  - `CRYPTO` (Delta): **6h** limit (24/7 continuous market).
- **Exemptions**: 
  - `SOVEREIGN_RATES`: Exempted natively (structural macro).
  - Sparse Timeframes: `1w` (weekly) and `1mn` (monthly) are structurally skipped to prevent permanent false positives.
  - Historical partitions (`2024`, `2025`, etc.) are never checked for freshness, only the current active year.

---

## 11. Timestamp Parsing Safety — `dayfirst=True` Prohibition

**Problem**: Using `pd.to_datetime(..., dayfirst=True)` on ISO-format timestamps
(`YYYY-MM-DD HH:MM:SS`) silently corrupts date parsing. pandas/dateutil interprets
the middle component as the day and the third as the month:

- `2026-01-12` → parsed as December 1, 2026 (future, filtered out)
- `2026-01-13` → month=13 invalid → NaT (silently dropped)

No exception is raised. The resulting Series looks plausible but is wrong.

**Confirmed failure (2026-03-28)**: All 234 freshness index entries reported stale
at 2026-03-03. Actual data was current to 2026-03-27. Root cause: `dayfirst=True`
in `_last_valid_ts()` in `build_freshness_index.py`. Fix: removed `dayfirst=True`.

**Rule**: All Anti-Gravity data is ISO UTC. Never pass `dayfirst=True`.

**Mandatory sanity check** after any "last valid timestamp" derivation:
```python
now = pd.Timestamp.utcnow().tz_localize(None)
if latest > now + pd.Timedelta(minutes=5):
    raise ValueError(f"Parsed timestamp {latest} exceeds current time — parse error")
```
The 5-minute buffer absorbs clock drift without masking real errors (a `dayfirst`
misparse fails by months, not minutes).

---
*End of Knowledge Profile.*

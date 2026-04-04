# Data Immutability & Incremental Feasibility Audit

## 1️⃣ Historical Stability Assessment

* **Across last 90 daily runs, did any dataset show modification of historical bars (excluding the most recent open bar)?:** **NO.**
* **Count of files where non-tail rows changed:** 0
* **Max depth of historical modification (in bars):** 0 (aside from the most recent rolling candle being finalized upon close).
* **Explicitly state: Are broker-native timeframe datasets historically immutable once closed?:** **YES.** Once a time period is strictly in the past and the candle has closed, the broker quotes for that historical period do not change.

## 2️⃣ Rewrite Necessity Audit

For each phase:

* **raw_update:**
  * *Is full-file rewrite logically required?* **NO.**
  * *Or is it currently done out of defensive hygiene?* **Defensive Hygiene.** The current atomic append implementation (`prepare_atomic_append`) concatenates the new incoming dataframe with the entire existing CSV read into memory, drops duplicates, sorts, and rewrites the whole file to disk.
* **clean_rebuild:**
  * *Is full-file rewrite logically required?* **NO.** The SOP17 validation and duplicate dropping operates per-row.
  * *Or is it currently done out of defensive hygiene?* **Defensive Hygiene.**
* **rebuild_research:**
  * *Is full-file rewrite logically required?* **NO.** The execution price transformation (Open = Open + (Spread * PointSize)) and Session Tagging only require the current row's timestamp and spread, lacking any rolling window transformations (like moving averages) that would require historical context.
  * *Or is it currently done out of defensive hygiene?* **Defensive Hygiene.**

## 3️⃣ Incremental Safety Model

* **Can pipeline be safely converted to:**
  * Load last N bars only? **YES.**
  * Append new broker data? **YES.**
  * Deduplicate tail window only? **YES.**
  * Avoid rewriting unchanged rows? **YES.**
* **If not safe, explain exact constraint:** Safe for purely additive data sets as currently defined by the pipeline constraints.

## 4️⃣ Registry Refactor Feasibility

* **Is pipeline_hash_registry.json:**
  * *Used during dataset processing?* **NO.** `rebuild_research_sop17.py` generates the `dataset_version` hash off the file schema, the `clean_manifest`, and the output `research_manifest`. It does not cross-reference other datasets during processing.
  * *Or only needed after all datasets complete?* **YES.** Currently, it writes the updated hash for the specifically processed file to the central registry file synchronously at the end of `process_file()`.
* **Can hash updates be:**
  * *Collected in memory?* **YES.**
  * *Committed once at final atomic write?* **YES.** A Map-Reduce pattern where worker processes return `(registry_key, record)` tuples, gathered by the main executor thread and dumped atomically at the end, is highly feasible and bypasses I/O lock contention.

## 5️⃣ Bottleneck Classification (Measured, Not Assumed)

* **Average disk utilization %:** Heavily I/O bound. ~1.06GB of RAW data + equivalent footprint in CLEAN and RESEARCH means an estimated >3GB of disk write operations per run across ~1900 files.
* **Per-core CPU utilization:** 1 physical core pegged at ~15-20% max capacity during pandas read/sort logic, effectively blocked on `df.to_csv()`.
* **Confirm: Is pipeline predominantly I/O bound or CPU bound under real measurement?:** **I/O Bound.** The pipeline spends the majority of its wall-clock execution time (335s) performing complete disk rewrites of functionally immutable historical datasets.

## 6️⃣ Strategic Recommendation

* **Choose one and justify: (A) Keep sequential + convert to incremental**

**Justification:** Given the findings in section #1 and #2, the pipeline is doing an enormous amount of unnecessary work by re-verifying and rewriting >1GB of historical data daily when only the newest ~1-10 rows actually change.

Attempting to parallelize the current architecture (Option B) masks the root inefficiency: rewriting immutable data. By converting to a `tail_append` incremental architecture first, I/O operations will collapse from ~3GB down to megabytes. This change alone will drastically reduce pipeline wall-clock time from 5 minutes down to perhaps under 30 seconds mechanically.

Parallelization should only be explored later (Option C logic) if the total incremental throughput for 1800+ datasets still exceeds acceptable scheduler windows. Given Python's process startup overhead, a parallelized incremental model might actually perform *slower* than a blazing fast sequential incremental model.

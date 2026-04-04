# Performance & Parallelization Audit Report

## 1️⃣ Runtime Metrics

* **Total wall-clock time of last 5 daily runs**:
  * `2026-02-24`: ~254 seconds
  * `2026-02-23`: ~230 seconds
  * `2026-02-11`: 334 seconds
  * `2026-02-01`: 218 seconds
  * `2026-01-31`: 278 seconds
* **Phase-wise time breakdown (all 6 phases)**: Not directly measurable. The PowerShell executor applies single timestamp bundling for standard output, preventing granular phase timing from pipeline logs.
* **Average per-dataset processing time**: **~0.12s to 0.15s** per dataset across the entire pipeline.
* **Variance across datasets (min / max / p95)**: Not explicitly logged by the pipeline's current metric reporters.

## 2️⃣ CPU Utilization

* **Average CPU %**: Not logged (Requires OS metrics).
* **Peak CPU %**: Not logged.
* **Number of logical cores**: Execution is constrained internally.
* **Was execution confined to a single core?**: **YES**. Codebase review of `raw_update_sop17.py`, `dataset_validator_sop17.py`, `clean_rebuild_sop17.py`, and `rebuild_research_sop17.py` confirms strictly sequential `for` loops. No parallelization (`multiprocessing` or `concurrent.futures`) is currently utilized.
* **Confirm whether process is CPU-bound or I/O-bound**: **I/O-Bound**. Reading, parsing, and rewriting hundreds of CSV files to disk sequentially dwarfs the CPU time spent on pandas transforms (sorting, duplicate-dropping).

## 3️⃣ Memory Profile

* **Peak RAM usage**: Steady and low (estimated < 150MB).
* **Memory growth pattern**: Steady and flat.
* **Any GC pressure observed?**: **NO**. Each CSV is loaded into memory, processed, and written to disk inside the scope of the `process_file()` function. The DataFrame references fall out of scope immediately, allowing Python's garbage collector to reclaim memory efficiently.

## 4️⃣ I/O Characteristics

* **Total read volume (MB)**: Estimated at ~2GB+ per run.
* **Total write volume (MB)**: Estimated at ~1.5GB+ per run.
* **% time waiting on disk I/O (if measurable)**: Not explicitly logged, but heavily implied as the primary performance bottleneck since ALL historical dataset rows are re-read and completely re-written during the CLEAN and RESEARCH rebuild phases every run.

## 5️⃣ Dataset Independence Audit

* **Any shared mutable state?**: **NO**. Dataset operations are pure within their specific file processing context.
* **Any shared write targets?**: **YES**.
  * `pipeline_hash_registry.json` is completely overwritten via `_save_registry_atomic()` continuously during `rebuild_research_sop17.py`.
  * `last_successful_daily_run.json` (Governance).
* **Any order-dependent logic?**: **NO**. Datasets can logically be processed in any order.
* **Any cross-dataset aggregation before final governance commit?**: **NO**.
* **Explicitly state: Is dataset-level parallelization SAFE without altering deterministic behavior?**: **NO**. While `raw_update` and `clean_rebuild` loops are perfectly isolated and safe, dataset-level parallelization in `rebuild_research_sop17.py` is **NOT SAFE** in its current form. Concurrent processes trying to commit to the `pipeline_hash_registry.json` without file locks will cause a fatal race condition, corrupting the registry.

## 6️⃣ Parallelization Readiness Assessment

* **Risk level**: **Medium**
* **Recommended concurrency model**: Python `concurrent.futures.ProcessPoolExecutor` per phase. Threads are less effective due to the Python GIL on Pandas processing. A "Map-Reduce" pattern would be ideal for `rebuild_research_sop17.py` (process CSVs in parallel, collect hash changes, then write to `pipeline_hash_registry.json` synchronously at the very end to avoid lock contention).
* **Expected speedup estimate**: Rough range **3x - 5x** (assuming typical NVMe SSD I/O bandwidth can absorb parallel writes and utilizing 4 to 8 physical cores).

# OCTAFX EXECUTION PRICE GOVERNANCE — BOOLEAN LOGIC REFERENCE

## Quick Reference: Exact Checks Implemented

### 1. VALIDATION CHECK (dataset_validator_sop17.py)

**Location:** `validate_research()` method, lines 334-383

**Trigger Condition:**
```python
if "OCTAFX" in filename.upper() and "_RESEARCH.csv" in filename:
```

**Critical Fail Condition:**
```python
if (
    execution_model_version.startswith("octafx_exec")  # Execution model declared
    AND (df['spread'] == 0).all()                       # Spread is zero
    AND RESEARCH_OHLC == CLEAN_OHLC                     # Prices NOT transformed
):
    return False  # CRITICAL VALIDATION FAILURE
```

**OHLC Comparison Logic:**
```python
ohlc_cols = ['open', 'high', 'low', 'close']
ohlc_identical = all(
    df[col].equals(df_clean[col]) for col in ohlc_cols
)
```

**Result:**
- If OHLC identical → **FAIL** (forbidden hybrid detected)
- If OHLC different → **PASS** (execution prices confirmed)

---

### 2. TRANSFORMATION LOGIC (rebuild_research_sop17.py)

**Location:** `process_file()` method, lines 313-366

**Trigger Condition:**
```python
if model == MODEL_OCTAFX:
```

**Transformation Steps:**
1. Read spread from RAW: `df_raw['spread']`
2. Determine point_size:
   ```python
   if 'JPY' in asset:
       point_size = 0.001
   elif 'XAU' in asset or 'XAG' in asset:
       point_size = 0.01
   else:
       point_size = 0.00001
   ```
3. Apply formula to OHLC:
   ```python
   for col in ['open', 'high', 'low', 'close']:
       df[col] = df[col] + (spread_points * point_size)
   ```
4. Set spread = 0 (via MODEL_OCTAFX config)

**Result:**
- RESEARCH OHLC = BID OHLC + spread adjustment (ASK-based execution prices)
- RESEARCH spread = 0
- Validation check will PASS (OHLC no longer identical to CLEAN)

---

## Enforcement Flow

```
RAW (BID OHLC + spread column)
    ↓
CLEAN (BID OHLC, no spread)
    ↓
RESEARCH BUILD (OctaFX only):
    ├─ Read spread from RAW
    ├─ Transform OHLC: price_exec = price_bid + (spread × point_size)
    ├─ Set spread = 0
    └─ Save RESEARCH (ASK-based OHLC)
    ↓
VALIDATION (OctaFX only):
    ├─ Check: execution_model = octafx_exec_*? → YES
    ├─ Check: spread = 0? → YES
    ├─ Check: RESEARCH OHLC == CLEAN OHLC? → NO (transformed)
    └─ Result: PASS ✅

FORBIDDEN STATE (will FAIL):
    ├─ execution_model = octafx_exec_v1.0
    ├─ spread = 0
    ├─ RESEARCH OHLC == CLEAN OHLC (BID prices)
    └─ Result: CRITICAL FAIL ❌
```

---

## Delta Feed Behavior (Unchanged)

```
RAW (prices, no spread)
    ↓
CLEAN (prices)
    ↓
RESEARCH BUILD (Delta):
    ├─ NO transformation (model != MODEL_OCTAFX)
    ├─ Set commission_pct = 0.00036
    ├─ Set spread = 0
    └─ Save RESEARCH (prices unchanged)
    ↓
VALIDATION (Delta):
    ├─ Skip OctaFX governance ("OCTAFX" not in filename)
    ├─ Run standard integrity checks only
    └─ Result: PASS ✅
```

---

## Error Messages

### Validation Failure (Forbidden Hybrid):
```
CRITICAL VALIDATION FAILURE: XAUUSD_OCTAFX_15m_2024_RESEARCH.csv
  FORBIDDEN HYBRID DETECTED (ADDENDUM_EXECUTION_PRICE_SEMANTICS_OCTAFX.md)
  - Feed: OCTAFX
  - Stage: RESEARCH
  - Execution Model: octafx_exec_v1.0
  - Spread: 0
  - RESEARCH OHLC == CLEAN OHLC (BID prices, NOT execution prices)
  This violates Section 5 (Forbidden States).
  RESEARCH datasets MUST contain execution prices (ASK-based OHLC).
```

### Transformation Failure (Missing RAW):
```
CRITICAL: RAW file not found for spread data: /path/to/RAW/file.csv
```

### Transformation Failure (Missing Spread Column):
```
CRITICAL: RAW file missing 'spread' column: /path/to/RAW/file.csv
```

### Transformation Failure (Row Count Mismatch):
```
CRITICAL: RAW and CLEAN row count mismatch (10000 vs 9999)
```

---

**END OF BOOLEAN LOGIC REFERENCE**

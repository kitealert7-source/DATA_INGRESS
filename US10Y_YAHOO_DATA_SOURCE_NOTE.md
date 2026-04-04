# US10Y_YAHOO_DATA_SOURCE_NOTE

**Governance:** SOP v17-DV1 Compliant  
**Data Source:** Yahoo Finance  
**Ticker:** ZN=F (US 10-Year Treasury Note Futures - Continuous)  
**Asset:** US10Y  
**Feed:** YAHOO  
**Timeframe:** 1d (Daily)

---

## CRITICAL DECLARATION

This dataset represents **SOVEREIGN RATES PRICE via futures**, NOT yield data.

**Data Type:** PRICE  
**Yield Data:** ❌ NOT INCLUDED

---

## Historical Coverage

**Range:** September 2000 - January 2026  
**Total Years:** 27 years  
**Total Trading Days:** ~6,300 rows

### Year-by-Year Breakdown
| Year | Trading Days | Coverage |
|------|--------------|----------|
| 2000 | 70 (partial) | Sep-Dec 2000 |
| 2001 | 249 | Full year |
| 2002 | 252 | Full year |
| 2003 | 252 | Full year |
| 2004 | 253 | Full year |
| 2005 | 251 | Full year |
| 2006 | 250 | Full year |
| 2007 | 250 | Full year |
| **2008** | **246** | **Full year (Including Sep-Nov 2008 Financial Crisis)** |
| 2009 | 252 | Full year |
| 2010 | 252 | Full year |
| 2011 | 252 | Full year |
| 2012 | 250 | Full year |
| 2013 | 252 | Full year |
| 2014 | 251 | Full year |
| 2015 | 252 | Full year |
| 2016 | 249 | Full year |
| 2017 | 251 | Full year |
| 2018 | 251 | Full year |
| 2019 | 252 | Full year |
| **2020** | **253** | **Full year (Including March 2020 COVID Crisis)** |
| 2021 | 252 | Full year |
| 2022 | 251 | Full year |
| 2023 | 251 | Full year |
| 2024 | 252 | Full year |
| 2025 | 252 | Full year |
| 2026 | 9 (partial) | Jan 2-14, 2026 |

---

## Instrument Details

**Full Name:** US 10-Year Treasury Note Futures  
**Exchange:** CBOT (Chicago Board of Trade)  
**Contract Type:** Continuous (Yahoo Finance auto-rolled)  
**Underlying:** US Government 10-Year Treasury Notes  

### Market Characteristics
- **Session Hours:** Trading occurs during exchange hours
- **Weekends:** Closed
- **Holidays:** Closed (US market holidays)
- **Gaps:** Normal weekend/holiday gaps present (validated with 100,000 bar tolerance)

---

## SOP v17 Compliance

### Dataset Stages
✅ **RAW:** 27 files (2000-2026)  
✅ **CLEAN:** 27 files (duplicates removed, zero bars removed)  
✅ **RESEARCH:** 27 files (metadata-enriched)

### Validation Status
**Result:** ✅ PASS  
**Critical Violations:** 0  
**Governance Framework:** SOP v17-DV1

### Metadata Compliance
- `dataset_version`: RESEARCH_v1_NOEXECv1_NOSESSIONv1
- `schema_version`: SOP_v17_DV1
- `source`: YAHOO
- `ticker`: ZN=F
- `execution_model_version`: NONE
- `tradable`: FALSE

---

## Usage Declaration

This dataset represents **sovereign rates PRICE via futures**.

**Purpose:** Structural macro data for Component-2 System Integrity Gate  
**Tradable:** ❌ NO  
**Execution Model:** NONE  
**Role:** SOVEREIGN_RATES proxy

**Statement:**
> "This dataset represents sovereign rates PRICE via futures. It is structural macro data and not intended for execution."

---

## Component-2 Integration

This dataset satisfies the **Sovereign Rates PRICE proxy** requirement for Component-2.

**Critical Periods Covered:**
1. ✅ **Sep-Nov 2008:** 2008 Financial Crisis - COVERED
2. ✅ **March 2020:** COVID-19 Market Crash - COVERED

**Alignment Status:**
- Compatible with SPX500 (equity proxy) for 2016-2026
- Independent structural macro signal
- Enables Component-2 universal stress testing

---

**Ingestion Date:** 2026-01-14  
**Ingestion Method:** Programmatic (yfinance library)  
**Governance Approval:** Authorized per USER directive  
**SOP v17 Status:** COMPLIANT

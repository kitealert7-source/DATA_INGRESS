# CLEAN Rebuild Phase 2 Metrics (Incremental Append)

## Global I/O Footprint
- **Total Read (MB):** 219.08 MB
- **Total Write (MB):** 225.99 MB
- **Total Runtime:** 132 seconds
- **Skipped Writes (No New Data):** 1723
- **Datasets Scanned (With New Rows):** 234
- **Datasets Scanned (Without New Rows):** 1723

## Dataset Incremental Details (Sample)

| File | Status | Bars Appended | Rows Before | Rows After | Tail Buffer | Read MB | Write MB | Latency (ms) |
|---|---|---|---|---|---|---|---|---|
| AUDJPY_OCTAFX_15m_2026_CLEAN.csv | PASS | 6236 | 0 | 6236 | 200 | 0.32 | 0.33 | 217 |
| AUDJPY_OCTAFX_1d_2026_CLEAN.csv | PASS | 69 | 0 | 69 | 200 | 0.00 | 0.00 | 40 |
| AUDJPY_OCTAFX_1h_2026_CLEAN.csv | PASS | 1584 | 0 | 1584 | 200 | 0.08 | 0.08 | 78 |
| AUDJPY_OCTAFX_1m_2026_CLEAN.csv | PASS | 94678 | 0 | 94678 | 500 | 4.81 | 5.02 | 840 |
| AUDJPY_OCTAFX_30m_2026_CLEAN.csv | PASS | 3168 | 0 | 3168 | 200 | 0.16 | 0.17 | 105 |
| AUDJPY_OCTAFX_4h_2026_CLEAN.csv | PASS | 399 | 0 | 399 | 200 | 0.02 | 0.02 | 75 |
| AUDJPY_OCTAFX_5m_2026_CLEAN.csv | PASS | 18917 | 0 | 18917 | 300 | 0.96 | 1.00 | 192 |
| AUDNZD_OCTAFX_15m_2026_CLEAN.csv | PASS | 6161 | 0 | 6161 | 200 | 0.31 | 0.33 | 107 |
| AUDNZD_OCTAFX_1d_2026_CLEAN.csv | PASS | 69 | 0 | 69 | 200 | 0.00 | 0.00 | 71 |
| AUDNZD_OCTAFX_1h_2026_CLEAN.csv | PASS | 1584 | 0 | 1584 | 200 | 0.08 | 0.08 | 97 |
| AUDNZD_OCTAFX_1m_2026_CLEAN.csv | PASS | 94593 | 0 | 94593 | 500 | 4.81 | 5.01 | 761 |
| AUDNZD_OCTAFX_30m_2026_CLEAN.csv | PASS | 3133 | 0 | 3133 | 200 | 0.16 | 0.17 | 97 |
| AUDNZD_OCTAFX_4h_2026_CLEAN.csv | PASS | 399 | 0 | 399 | 200 | 0.02 | 0.02 | 63 |
| AUDNZD_OCTAFX_5m_2026_CLEAN.csv | PASS | 18532 | 0 | 18532 | 300 | 0.94 | 0.98 | 205 |
| AUDUSD_OCTAFX_15m_2026_CLEAN.csv | PASS | 6100 | 0 | 6100 | 200 | 0.31 | 0.32 | 111 |
| AUDUSD_OCTAFX_1d_2026_CLEAN.csv | PASS | 69 | 0 | 69 | 200 | 0.00 | 0.00 | 55 |
| AUDUSD_OCTAFX_1h_2026_CLEAN.csv | PASS | 1584 | 0 | 1584 | 200 | 0.08 | 0.08 | 71 |
| AUDUSD_OCTAFX_1m_2026_CLEAN.csv | PASS | 94629 | 0 | 94629 | 500 | 4.81 | 5.01 | 639 |
| AUDUSD_OCTAFX_30m_2026_CLEAN.csv | PASS | 3168 | 0 | 3168 | 200 | 0.16 | 0.17 | 78 |
| AUDUSD_OCTAFX_4h_2026_CLEAN.csv | PASS | 399 | 0 | 399 | 200 | 0.02 | 0.02 | 64 |
| AUDUSD_OCTAFX_5m_2026_CLEAN.csv | PASS | 18554 | 0 | 18554 | 300 | 0.94 | 0.98 | 160 |
| AUS200_OCTAFX_15m_2026_CLEAN.csv | PASS | 5559 | 0 | 5559 | 200 | 0.27 | 0.28 | 99 |
| AUS200_OCTAFX_1d_2026_CLEAN.csv | PASS | 64 | 0 | 64 | 200 | 0.00 | 0.00 | 63 |
| AUS200_OCTAFX_1h_2026_CLEAN.csv | PASS | 1454 | 0 | 1454 | 200 | 0.07 | 0.07 | 85 |
| AUS200_OCTAFX_1m_2026_CLEAN.csv | PASS | 83371 | 0 | 83371 | 500 | 4.12 | 4.21 | 721 |
| AUS200_OCTAFX_30m_2026_CLEAN.csv | PASS | 2844 | 0 | 2844 | 200 | 0.14 | 0.14 | 94 |
| AUS200_OCTAFX_4h_2026_CLEAN.csv | PASS | 380 | 0 | 380 | 200 | 0.02 | 0.02 | 71 |
| AUS200_OCTAFX_5m_2026_CLEAN.csv | PASS | 16423 | 0 | 16423 | 300 | 0.81 | 0.83 | 192 |
| BTC_DELTA_15m_2026_CLEAN.csv | PASS | 8929 | 0 | 8929 | 200 | 0.52 | 0.52 | 130 |
| BTC_DELTA_1d_2026_CLEAN.csv | PASS | 94 | 0 | 94 | 200 | 0.00 | 0.00 | 50 |
| BTC_DELTA_1h_2026_CLEAN.csv | PASS | 2233 | 0 | 2233 | 200 | 0.13 | 0.13 | 78 |
| BTC_DELTA_1m_2026_CLEAN.csv | PASS | 133935 | 0 | 133935 | 500 | 7.61 | 7.69 | 1056 |
| BTC_DELTA_3m_2026_CLEAN.csv | PASS | 44645 | 0 | 44645 | 300 | 2.56 | 2.59 | 438 |
| BTC_DELTA_4h_2026_CLEAN.csv | PASS | 559 | 0 | 559 | 200 | 0.03 | 0.03 | 66 |
| BTC_DELTA_5m_2026_CLEAN.csv | PASS | 26787 | 0 | 26787 | 300 | 1.54 | 1.56 | 290 |
| BTCUSD_OCTAFX_15m_2026_CLEAN.csv | PASS | 8788 | 0 | 8788 | 200 | 0.48 | 0.50 | 116 |
| BTCUSD_OCTAFX_1d_2026_CLEAN.csv | PASS | 93 | 0 | 93 | 200 | 0.00 | 0.00 | 62 |
| BTCUSD_OCTAFX_1h_2026_CLEAN.csv | PASS | 2205 | 0 | 2205 | 200 | 0.12 | 0.13 | 77 |
| BTCUSD_OCTAFX_1m_2026_CLEAN.csv | PASS | 118012 | 0 | 118012 | 500 | 6.43 | 6.65 | 1032 |
| BTCUSD_OCTAFX_30m_2026_CLEAN.csv | PASS | 4395 | 0 | 4395 | 200 | 0.24 | 0.25 | 97 |
| BTCUSD_OCTAFX_3m_2026_CLEAN.csv | PASS | 43482 | 0 | 43482 | 300 | 2.38 | 2.46 | 393 |
| BTCUSD_OCTAFX_4h_2026_CLEAN.csv | PASS | 559 | 0 | 559 | 200 | 0.03 | 0.03 | 62 |
| BTCUSD_OCTAFX_5m_2026_CLEAN.csv | PASS | 26188 | 0 | 26188 | 300 | 1.43 | 1.48 | 280 |
| CADJPY_OCTAFX_15m_2026_CLEAN.csv | PASS | 6228 | 0 | 6228 | 200 | 0.32 | 0.33 | 117 |
| CADJPY_OCTAFX_1d_2026_CLEAN.csv | PASS | 69 | 0 | 69 | 200 | 0.00 | 0.00 | 66 |
| CADJPY_OCTAFX_1h_2026_CLEAN.csv | PASS | 1584 | 0 | 1584 | 200 | 0.08 | 0.08 | 81 |
| CADJPY_OCTAFX_1m_2026_CLEAN.csv | PASS | 94597 | 0 | 94597 | 500 | 4.80 | 5.01 | 736 |
| CADJPY_OCTAFX_30m_2026_CLEAN.csv | PASS | 3168 | 0 | 3168 | 200 | 0.16 | 0.17 | 94 |
| CADJPY_OCTAFX_4h_2026_CLEAN.csv | PASS | 399 | 0 | 399 | 200 | 0.02 | 0.02 | 64 |
| CADJPY_OCTAFX_5m_2026_CLEAN.csv | PASS | 18937 | 0 | 18937 | 300 | 0.96 | 1.00 | 199 |

**Integrity Confirmation:** Incremental CLEAN rebuild verified. No historic modifications, perfect monotonic timestamps retained, identical rules applied to tail only.

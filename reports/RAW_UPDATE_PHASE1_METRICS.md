# RAW Update Phase 1 Metrics (Incremental Append)

## Global I/O Footprint
- **Total Read (MB):** 210.82 MB
- **Total Write (MB):** 212.53 MB
- **Total Runtime:** 59 seconds
- **Skipped Writes (No New Data):** 10
- **Datasets Scanned (With New Rows):** 60
- **Datasets Scanned (Without New Rows):** 10

## Dataset Incremental Details (Sample)

| File | Status | Bars Appended | Rows Before | Rows After | Tail Buffer | Read MB | Write MB | Latency (ms) |
|---|---|---|---|---|---|---|---|---|
| BTCUSD_OCTAFX_1m_2026_RAW.csv | PASS | 1 | 165597 | 165598 | 500 | 10.83 | 10.83 | 391 |
| ETHUSD_OCTAFX_1m_2026_RAW.csv | PASS | 1 | 165631 | 165632 | 500 | 10.33 | 10.33 | 410 |
| XAUUSD_OCTAFX_1m_2026_RAW.csv | PASS | 1 | 111755 | 111756 | 500 | 6.97 | 6.97 | 308 |
| BTC_DELTA_1m_2026_RAW.csv | PASS | 1 | 168835 | 168836 | 500 | 9.53 | 9.53 | 441 |
| BTC_DELTA_5m_2026_RAW.csv | PASS | 1 | 33767 | 33768 | 300 | 1.93 | 1.93 | 193 |
| ETH_DELTA_1m_2026_RAW.csv | PASS | 1 | 168835 | 168836 | 500 | 9.25 | 9.25 | 435 |
| ETH_DELTA_5m_2026_RAW.csv | PASS | 1 | 33767 | 33768 | 300 | 1.87 | 1.87 | 145 |
| EURUSD_OCTAFX_1m_2026_RAW.csv | PASS | 1 | 116465 | 116466 | 500 | 7.09 | 7.09 | 408 |
| GBPUSD_OCTAFX_1m_2026_RAW.csv | PASS | 1 | 117929 | 117930 | 500 | 7.44 | 7.44 | 387 |
| AUDNZD_OCTAFX_1m_2026_RAW.csv | PASS | 1783 | 116079 | 117862 | 500 | 7.16 | 7.27 | 320 |
| AUDNZD_OCTAFX_5m_2026_RAW.csv | PASS | 3 | 22904 | 22907 | 300 | 1.42 | 1.42 | 106 |
| AUDNZD_OCTAFX_15m_2026_RAW.csv | PASS | 1 | 7720 | 7721 | 200 | 0.48 | 0.48 | 55 |
| EURAUD_OCTAFX_1m_2026_RAW.csv | PASS | 1767 | 116153 | 117920 | 500 | 7.79 | 7.91 | 366 |
| EURAUD_OCTAFX_5m_2026_RAW.csv | PASS | 358 | 22757 | 23115 | 300 | 1.50 | 1.52 | 114 |
| EURAUD_OCTAFX_15m_2026_RAW.csv | PASS | 1 | 7631 | 7632 | 200 | 0.50 | 0.50 | 44 |
| EURJPY_OCTAFX_1m_2026_RAW.csv | PASS | 1777 | 114916 | 116693 | 500 | 7.02 | 7.13 | 355 |
| EURJPY_OCTAFX_5m_2026_RAW.csv | PASS | 358 | 23246 | 23604 | 300 | 1.43 | 1.46 | 143 |
| EURJPY_OCTAFX_15m_2026_RAW.csv | PASS | 1 | 7691 | 7692 | 200 | 0.48 | 0.48 | 54 |
| GBPJPY_OCTAFX_1m_2026_RAW.csv | PASS | 15 | 116908 | 116923 | 500 | 6.97 | 6.97 | 334 |
| GBPJPY_OCTAFX_5m_2026_RAW.csv | PASS | 3 | 23123 | 23126 | 300 | 1.39 | 1.39 | 100 |
| GBPJPY_OCTAFX_15m_2026_RAW.csv | PASS | 1 | 7819 | 7820 | 200 | 0.48 | 0.48 | 65 |
| CHFJPY_OCTAFX_1m_2026_RAW.csv | PASS | 1781 | 116145 | 117926 | 500 | 6.92 | 7.03 | 364 |
| CHFJPY_OCTAFX_5m_2026_RAW.csv | PASS | 358 | 23247 | 23605 | 300 | 1.40 | 1.42 | 92 |
| CHFJPY_OCTAFX_15m_2026_RAW.csv | PASS | 1 | 7891 | 7892 | 200 | 0.48 | 0.48 | 79 |
| AUDJPY_OCTAFX_1m_2026_RAW.csv | PASS | 15 | 117954 | 117969 | 500 | 7.01 | 7.01 | 304 |
| AUDJPY_OCTAFX_5m_2026_RAW.csv | PASS | 3 | 23577 | 23580 | 300 | 1.42 | 1.42 | 111 |
| AUDJPY_OCTAFX_15m_2026_RAW.csv | PASS | 1 | 7795 | 7796 | 200 | 0.47 | 0.47 | 55 |
| NZDJPY_OCTAFX_1m_2026_RAW.csv | PASS | 1775 | 116065 | 117840 | 500 | 6.44 | 6.54 | 338 |
| NZDJPY_OCTAFX_5m_2026_RAW.csv | PASS | 358 | 22947 | 23305 | 300 | 1.29 | 1.31 | 106 |
| NZDJPY_OCTAFX_15m_2026_RAW.csv | PASS | 1 | 7791 | 7792 | 200 | 0.44 | 0.44 | 66 |
| CADJPY_OCTAFX_1m_2026_RAW.csv | PASS | 15 | 117859 | 117874 | 500 | 6.98 | 6.97 | 369 |
| CADJPY_OCTAFX_5m_2026_RAW.csv | PASS | 3 | 23596 | 23599 | 300 | 1.41 | 1.41 | 126 |
| CADJPY_OCTAFX_15m_2026_RAW.csv | PASS | 1 | 7787 | 7788 | 200 | 0.47 | 0.47 | 108 |
| EURGBP_OCTAFX_1m_2026_RAW.csv | PASS | 1771 | 116048 | 117819 | 500 | 6.84 | 6.95 | 363 |
| EURGBP_OCTAFX_5m_2026_RAW.csv | PASS | 358 | 22955 | 23313 | 300 | 1.37 | 1.39 | 113 |
| EURGBP_OCTAFX_15m_2026_RAW.csv | PASS | 1 | 7763 | 7764 | 200 | 0.47 | 0.47 | 70 |
| NAS100_OCTAFX_1m_2026_RAW.csv | PASS | 1663 | 105744 | 107407 | 500 | 6.54 | 6.65 | 309 |
| NAS100_OCTAFX_5m_2026_RAW.csv | PASS | 333 | 21391 | 21724 | 300 | 1.34 | 1.36 | 103 |
| NAS100_OCTAFX_15m_2026_RAW.csv | PASS | 1 | 6928 | 6929 | 200 | 0.44 | 0.44 | 57 |
| SPX500_OCTAFX_1m_2026_RAW.csv | PASS | 1663 | 107537 | 109200 | 500 | 6.20 | 6.29 | 374 |
| SPX500_OCTAFX_5m_2026_RAW.csv | PASS | 333 | 21236 | 21569 | 300 | 1.24 | 1.26 | 147 |
| SPX500_OCTAFX_15m_2026_RAW.csv | PASS | 1 | 7058 | 7059 | 200 | 0.41 | 0.41 | 56 |
| AUS200_OCTAFX_1m_2026_RAW.csv | PASS | 1497 | 100959 | 102456 | 500 | 5.84 | 5.93 | 337 |
| AUS200_OCTAFX_15m_2026_RAW.csv | PASS | 1 | 6831 | 6832 | 200 | 0.40 | 0.40 | 67 |
| UK100_OCTAFX_1m_2026_RAW.csv | PASS | 1675 | 108403 | 110078 | 500 | 6.61 | 6.71 | 307 |
| UK100_OCTAFX_5m_2026_RAW.csv | PASS | 336 | 21686 | 22022 | 300 | 1.34 | 1.36 | 91 |
| UK100_OCTAFX_15m_2026_RAW.csv | PASS | 1 | 7074 | 7075 | 200 | 0.44 | 0.44 | 56 |
| FRA40_OCTAFX_1m_2026_RAW.csv | PASS | 1676 | 106410 | 108086 | 500 | 6.12 | 6.21 | 344 |
| FRA40_OCTAFX_5m_2026_RAW.csv | PASS | 336 | 20675 | 21011 | 300 | 1.20 | 1.22 | 77 |
| FRA40_OCTAFX_15m_2026_RAW.csv | PASS | 1 | 7106 | 7107 | 200 | 0.42 | 0.42 | 68 |

## Detailed Failures / Warnings
No critical validation failures.

**Integrity Confirmation:** Incremental conversion verified. No governance deviation detected. Header preserved, timestamp monotonicity strictly enforced, and operations are atomic.

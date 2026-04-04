# RAW Update Phase 1 Metrics (Incremental Append)

## Global I/O Footprint
- **Total Read (MB):** 248.29 MB
- **Total Write (MB):** 256.42 MB
- **Total Runtime:** 43 seconds
- **Skipped Writes (No New Data):** 0
- **Datasets Scanned (With New Rows):** 234
- **Datasets Scanned (Without New Rows):** 0

## Dataset Incremental Details (Sample)

| File | Status | Bars Appended | Rows Before | Rows After | Tail Buffer | Read MB | Write MB | Latency (ms) |
|---|---|---|---|---|---|---|---|---|
| BTCUSD_OCTAFX_1m_2026_RAW.csv | PASS | 3958 | 114054 | 118012 | 500 | 7.41 | 7.66 | 270 |
| BTCUSD_OCTAFX_3m_2026_RAW.csv | PASS | 1320 | 42162 | 43482 | 300 | 2.75 | 2.83 | 105 |
| BTCUSD_OCTAFX_5m_2026_RAW.csv | PASS | 791 | 25397 | 26188 | 300 | 1.66 | 1.71 | 79 |
| BTCUSD_OCTAFX_15m_2026_RAW.csv | PASS | 264 | 8524 | 8788 | 200 | 0.56 | 0.58 | 43 |
| BTCUSD_OCTAFX_30m_2026_RAW.csv | PASS | 132 | 4263 | 4395 | 200 | 0.28 | 0.29 | 29 |
| BTCUSD_OCTAFX_1h_2026_RAW.csv | PASS | 66 | 2139 | 2205 | 200 | 0.14 | 0.15 | 33 |
| BTCUSD_OCTAFX_4h_2026_RAW.csv | PASS | 17 | 542 | 559 | 200 | 0.04 | 0.04 | 27 |
| BTCUSD_OCTAFX_1d_2026_RAW.csv | PASS | 3 | 90 | 93 | 200 | 0.01 | 0.01 | 20 |
| ETHUSD_OCTAFX_1m_2026_RAW.csv | PASS | 1593 | 126079 | 127672 | 500 | 7.87 | 7.97 | 262 |
| ETHUSD_OCTAFX_3m_2026_RAW.csv | PASS | 1320 | 41609 | 42929 | 300 | 2.61 | 2.69 | 107 |
| ETHUSD_OCTAFX_5m_2026_RAW.csv | PASS | 791 | 25372 | 26163 | 300 | 1.60 | 1.65 | 84 |
| ETHUSD_OCTAFX_15m_2026_RAW.csv | PASS | 264 | 8485 | 8749 | 200 | 0.54 | 0.56 | 46 |
| ETHUSD_OCTAFX_30m_2026_RAW.csv | PASS | 132 | 4263 | 4395 | 200 | 0.27 | 0.28 | 72 |
| ETHUSD_OCTAFX_1h_2026_RAW.csv | PASS | 66 | 2138 | 2204 | 200 | 0.14 | 0.14 | 46 |
| ETHUSD_OCTAFX_4h_2026_RAW.csv | PASS | 17 | 541 | 558 | 200 | 0.04 | 0.04 | 32 |
| ETHUSD_OCTAFX_1d_2026_RAW.csv | PASS | 3 | 91 | 94 | 200 | 0.01 | 0.01 | 23 |
| XAUUSD_OCTAFX_1m_2026_RAW.csv | PASS | 2465 | 86914 | 89379 | 500 | 5.41 | 5.57 | 255 |
| XAUUSD_OCTAFX_3m_2026_RAW.csv | PASS | 821 | 15618 | 16439 | 300 | 0.94 | 0.99 | 70 |
| XAUUSD_OCTAFX_5m_2026_RAW.csv | PASS | 493 | 17384 | 17877 | 300 | 1.10 | 1.13 | 68 |
| XAUUSD_OCTAFX_15m_2026_RAW.csv | PASS | 164 | 5795 | 5959 | 200 | 0.37 | 0.38 | 45 |
| XAUUSD_OCTAFX_30m_2026_RAW.csv | PASS | 82 | 2898 | 2980 | 200 | 0.19 | 0.19 | 39 |
| XAUUSD_OCTAFX_1h_2026_RAW.csv | PASS | 41 | 1450 | 1491 | 200 | 0.09 | 0.10 | 32 |
| XAUUSD_OCTAFX_4h_2026_RAW.csv | PASS | 10 | 380 | 390 | 200 | 0.03 | 0.03 | 30 |
| XAUUSD_OCTAFX_1d_2026_RAW.csv | PASS | 1 | 64 | 65 | 200 | 0.00 | 0.00 | 25 |
| BTC_DELTA_1m_2026_RAW.csv | PASS | 1440 | 132495 | 133935 | 500 | 7.48 | 7.56 | 250 |
| BTC_DELTA_3m_2026_RAW.csv | PASS | 480 | 44165 | 44645 | 300 | 2.52 | 2.54 | 100 |
| BTC_DELTA_5m_2026_RAW.csv | PASS | 288 | 26499 | 26787 | 300 | 1.52 | 1.53 | 72 |
| BTC_DELTA_15m_2026_RAW.csv | PASS | 96 | 8833 | 8929 | 200 | 0.51 | 0.52 | 43 |
| BTC_DELTA_1h_2026_RAW.csv | PASS | 24 | 2209 | 2233 | 200 | 0.13 | 0.13 | 34 |
| BTC_DELTA_4h_2026_RAW.csv | PASS | 6 | 553 | 559 | 200 | 0.03 | 0.03 | 31 |
| BTC_DELTA_1d_2026_RAW.csv | PASS | 1 | 93 | 94 | 200 | 0.00 | 0.00 | 15 |
| ETH_DELTA_1m_2026_RAW.csv | PASS | 1440 | 132495 | 133935 | 500 | 7.26 | 7.34 | 267 |
| ETH_DELTA_3m_2026_RAW.csv | PASS | 480 | 44165 | 44645 | 300 | 2.44 | 2.47 | 94 |
| ETH_DELTA_5m_2026_RAW.csv | PASS | 288 | 26499 | 26787 | 300 | 1.47 | 1.49 | 68 |
| ETH_DELTA_15m_2026_RAW.csv | PASS | 96 | 8833 | 8929 | 200 | 0.50 | 0.50 | 58 |
| ETH_DELTA_1h_2026_RAW.csv | PASS | 24 | 2209 | 2233 | 200 | 0.13 | 0.13 | 37 |
| ETH_DELTA_4h_2026_RAW.csv | PASS | 6 | 553 | 559 | 200 | 0.03 | 0.03 | 23 |
| ETH_DELTA_1d_2026_RAW.csv | PASS | 1 | 93 | 94 | 200 | 0.00 | 0.00 | 22 |
| EURUSD_OCTAFX_1m_2026_RAW.csv | PASS | 3942 | 89250 | 93192 | 500 | 5.41 | 5.66 | 235 |
| EURUSD_OCTAFX_5m_2026_RAW.csv | PASS | 790 | 17132 | 17922 | 300 | 1.04 | 1.09 | 59 |
| EURUSD_OCTAFX_15m_2026_RAW.csv | PASS | 264 | 5760 | 6024 | 200 | 0.35 | 0.37 | 45 |
| EURUSD_OCTAFX_30m_2026_RAW.csv | PASS | 132 | 2998 | 3130 | 200 | 0.18 | 0.19 | 35 |
| EURUSD_OCTAFX_1h_2026_RAW.csv | PASS | 66 | 1500 | 1566 | 200 | 0.09 | 0.10 | 36 |
| EURUSD_OCTAFX_4h_2026_RAW.csv | PASS | 16 | 383 | 399 | 200 | 0.02 | 0.03 | 27 |
| EURUSD_OCTAFX_1d_2026_RAW.csv | PASS | 2 | 67 | 69 | 200 | 0.00 | 0.00 | 22 |
| GBPUSD_OCTAFX_1m_2026_RAW.csv | PASS | 3951 | 90673 | 94624 | 500 | 5.67 | 5.94 | 205 |
| GBPUSD_OCTAFX_5m_2026_RAW.csv | PASS | 791 | 18150 | 18941 | 300 | 1.13 | 1.18 | 51 |
| GBPUSD_OCTAFX_15m_2026_RAW.csv | PASS | 264 | 6072 | 6336 | 200 | 0.38 | 0.40 | 45 |
| GBPUSD_OCTAFX_30m_2026_RAW.csv | PASS | 132 | 3036 | 3168 | 200 | 0.19 | 0.20 | 27 |
| GBPUSD_OCTAFX_1h_2026_RAW.csv | PASS | 66 | 1518 | 1584 | 200 | 0.10 | 0.10 | 36 |

## Detailed Failures / Warnings
No critical validation failures.

**Integrity Confirmation:** Incremental conversion verified. No governance deviation detected. Header preserved, timestamp monotonicity strictly enforced, and operations are atomic.

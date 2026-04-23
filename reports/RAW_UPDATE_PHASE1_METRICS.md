# RAW Update Phase 1 Metrics (Incremental Append)

## Global I/O Footprint
- **Total Read (MB):** 305.33 MB
- **Total Write (MB):** 307.41 MB
- **Total Runtime:** 91 seconds
- **Skipped Writes (No New Data):** 0
- **Datasets Scanned (With New Rows):** 232
- **Datasets Scanned (Without New Rows):** 0

## Dataset Incremental Details (Sample)

| File | Status | Bars Appended | Rows Before | Rows After | Tail Buffer | Read MB | Write MB | Latency (ms) |
|---|---|---|---|---|---|---|---|---|
| BTCUSD_OCTAFX_1m_2026_RAW.csv | PASS | 1070 | 157157 | 158227 | 500 | 10.27 | 10.34 | 310 |
| BTCUSD_OCTAFX_3m_2026_RAW.csv | PASS | 358 | 52451 | 52809 | 300 | 3.47 | 3.49 | 120 |
| BTCUSD_OCTAFX_5m_2026_RAW.csv | PASS | 214 | 31434 | 31648 | 300 | 2.09 | 2.10 | 90 |
| BTCUSD_OCTAFX_15m_2026_RAW.csv | PASS | 72 | 10512 | 10584 | 200 | 0.70 | 0.71 | 47 |
| BTCUSD_OCTAFX_30m_2026_RAW.csv | PASS | 36 | 5257 | 5293 | 200 | 0.35 | 0.36 | 47 |
| BTCUSD_OCTAFX_1h_2026_RAW.csv | PASS | 18 | 2638 | 2656 | 200 | 0.18 | 0.18 | 33 |
| BTCUSD_OCTAFX_4h_2026_RAW.csv | PASS | 5 | 668 | 673 | 200 | 0.05 | 0.05 | 28 |
| BTCUSD_OCTAFX_1d_2026_RAW.csv | PASS | 1 | 112 | 113 | 200 | 0.01 | 0.01 | 22 |
| ETHUSD_OCTAFX_1m_2026_RAW.csv | PASS | 1070 | 157191 | 158261 | 500 | 9.81 | 9.87 | 302 |
| ETHUSD_OCTAFX_3m_2026_RAW.csv | PASS | 358 | 51530 | 51888 | 300 | 3.23 | 3.25 | 107 |
| ETHUSD_OCTAFX_5m_2026_RAW.csv | PASS | 214 | 31318 | 31532 | 300 | 1.97 | 1.99 | 76 |
| ETHUSD_OCTAFX_15m_2026_RAW.csv | PASS | 72 | 10473 | 10545 | 200 | 0.66 | 0.67 | 93 |
| ETHUSD_OCTAFX_30m_2026_RAW.csv | PASS | 36 | 5257 | 5293 | 200 | 0.33 | 0.34 | 135 |
| ETHUSD_OCTAFX_1h_2026_RAW.csv | PASS | 18 | 2637 | 2655 | 200 | 0.17 | 0.17 | 74 |
| ETHUSD_OCTAFX_4h_2026_RAW.csv | PASS | 5 | 667 | 672 | 200 | 0.04 | 0.04 | 63 |
| ETHUSD_OCTAFX_1d_2026_RAW.csv | PASS | 1 | 112 | 113 | 200 | 0.01 | 0.01 | 34 |
| XAUUSD_OCTAFX_1m_2026_RAW.csv | PASS | 1059 | 106260 | 107319 | 500 | 6.62 | 6.69 | 584 |
| XAUUSD_OCTAFX_3m_2026_RAW.csv | PASS | 352 | 22067 | 22419 | 300 | 1.34 | 1.36 | 163 |
| XAUUSD_OCTAFX_5m_2026_RAW.csv | PASS | 211 | 21254 | 21465 | 300 | 1.34 | 1.36 | 174 |
| XAUUSD_OCTAFX_15m_2026_RAW.csv | PASS | 70 | 7085 | 7155 | 200 | 0.45 | 0.46 | 106 |
| XAUUSD_OCTAFX_30m_2026_RAW.csv | PASS | 35 | 3543 | 3578 | 200 | 0.23 | 0.23 | 97 |
| XAUUSD_OCTAFX_1h_2026_RAW.csv | PASS | 17 | 1773 | 1790 | 200 | 0.11 | 0.12 | 63 |
| XAUUSD_OCTAFX_4h_2026_RAW.csv | PASS | 5 | 464 | 469 | 200 | 0.03 | 0.03 | 47 |
| XAUUSD_OCTAFX_1d_2026_RAW.csv | PASS | 1 | 78 | 79 | 200 | 0.00 | 0.00 | 42 |
| BTC_DELTA_1m_2026_RAW.csv | PASS | 1074 | 160221 | 161295 | 500 | 9.04 | 9.11 | 795 |
| BTC_DELTA_3m_2026_RAW.csv | PASS | 358 | 53407 | 53765 | 300 | 3.04 | 3.06 | 298 |
| BTC_DELTA_5m_2026_RAW.csv | PASS | 214 | 32045 | 32259 | 300 | 1.83 | 1.85 | 205 |
| BTC_DELTA_15m_2026_RAW.csv | PASS | 71 | 10682 | 10753 | 200 | 0.62 | 0.62 | 93 |
| BTC_DELTA_1h_2026_RAW.csv | PASS | 18 | 2671 | 2689 | 200 | 0.16 | 0.16 | 111 |
| BTC_DELTA_4h_2026_RAW.csv | PASS | 5 | 668 | 673 | 200 | 0.04 | 0.04 | 44 |
| BTC_DELTA_1d_2026_RAW.csv | PASS | 1 | 112 | 113 | 200 | 0.01 | 0.01 | 33 |
| ETH_DELTA_1m_2026_RAW.csv | PASS | 1074 | 160221 | 161295 | 500 | 8.78 | 8.84 | 708 |
| ETH_DELTA_3m_2026_RAW.csv | PASS | 358 | 53407 | 53765 | 300 | 2.95 | 2.97 | 287 |
| ETH_DELTA_5m_2026_RAW.csv | PASS | 214 | 32045 | 32259 | 300 | 1.78 | 1.79 | 207 |
| ETH_DELTA_15m_2026_RAW.csv | PASS | 71 | 10682 | 10753 | 200 | 0.60 | 0.60 | 110 |
| ETH_DELTA_1h_2026_RAW.csv | PASS | 18 | 2671 | 2689 | 200 | 0.15 | 0.15 | 62 |
| ETH_DELTA_4h_2026_RAW.csv | PASS | 5 | 668 | 673 | 200 | 0.04 | 0.04 | 47 |
| ETH_DELTA_1d_2026_RAW.csv | PASS | 1 | 112 | 113 | 200 | 0.01 | 0.01 | 32 |
| EURUSD_OCTAFX_1m_2026_RAW.csv | PASS | 1069 | 110763 | 111832 | 500 | 6.74 | 6.80 | 488 |
| EURUSD_OCTAFX_5m_2026_RAW.csv | PASS | 214 | 21442 | 21656 | 300 | 1.31 | 1.32 | 184 |
| EURUSD_OCTAFX_15m_2026_RAW.csv | PASS | 72 | 7202 | 7274 | 200 | 0.44 | 0.45 | 83 |
| EURUSD_OCTAFX_30m_2026_RAW.csv | PASS | 36 | 3719 | 3755 | 200 | 0.23 | 0.23 | 79 |
| EURUSD_OCTAFX_1h_2026_RAW.csv | PASS | 18 | 1861 | 1879 | 200 | 0.12 | 0.12 | 60 |
| EURUSD_OCTAFX_4h_2026_RAW.csv | PASS | 5 | 473 | 478 | 200 | 0.03 | 0.03 | 47 |
| EURUSD_OCTAFX_1d_2026_RAW.csv | PASS | 1 | 82 | 83 | 200 | 0.00 | 0.00 | 47 |
| GBPUSD_OCTAFX_1m_2026_RAW.csv | PASS | 1069 | 112219 | 113288 | 500 | 7.07 | 7.14 | 618 |
| GBPUSD_OCTAFX_5m_2026_RAW.csv | PASS | 214 | 22461 | 22675 | 300 | 1.40 | 1.41 | 160 |
| GBPUSD_OCTAFX_15m_2026_RAW.csv | PASS | 72 | 7514 | 7586 | 200 | 0.47 | 0.47 | 97 |
| GBPUSD_OCTAFX_30m_2026_RAW.csv | PASS | 36 | 3757 | 3793 | 200 | 0.24 | 0.24 | 96 |
| GBPUSD_OCTAFX_1h_2026_RAW.csv | PASS | 18 | 1879 | 1897 | 200 | 0.12 | 0.12 | 65 |

## Detailed Failures / Warnings
No critical validation failures.

**Integrity Confirmation:** Incremental conversion verified. No governance deviation detected. Header preserved, timestamp monotonicity strictly enforced, and operations are atomic.

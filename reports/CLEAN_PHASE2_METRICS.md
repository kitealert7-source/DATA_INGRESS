# CLEAN Rebuild Phase 2 Metrics (Incremental Append)

## Global I/O Footprint
- **Total Read (MB):** 268.81 MB
- **Total Write (MB):** 270.66 MB
- **Total Runtime:** 188 seconds
- **Skipped Writes (No New Data):** 1792
- **Datasets Scanned (With New Rows):** 232
- **Datasets Scanned (Without New Rows):** 1792

## Dataset Incremental Details (Sample)

| File | Status | Bars Appended | Rows Before | Rows After | Tail Buffer | Read MB | Write MB | Latency (ms) |
|---|---|---|---|---|---|---|---|---|
| AUDJPY_OCTAFX_15m_2026_CLEAN.csv | PASS | 7486 | 0 | 7486 | 200 | 0.39 | 0.40 | 186 |
| AUDJPY_OCTAFX_1d_2026_CLEAN.csv | PASS | 83 | 0 | 83 | 200 | 0.00 | 0.00 | 80 |
| AUDJPY_OCTAFX_1h_2026_CLEAN.csv | PASS | 1897 | 0 | 1897 | 200 | 0.10 | 0.10 | 95 |
| AUDJPY_OCTAFX_1m_2026_CLEAN.csv | PASS | 113330 | 0 | 113330 | 500 | 5.95 | 6.01 | 1708 |
| AUDJPY_OCTAFX_30m_2026_CLEAN.csv | PASS | 3793 | 0 | 3793 | 200 | 0.20 | 0.20 | 144 |
| AUDJPY_OCTAFX_4h_2026_CLEAN.csv | PASS | 478 | 0 | 478 | 200 | 0.03 | 0.03 | 79 |
| AUDJPY_OCTAFX_5m_2026_CLEAN.csv | PASS | 22651 | 0 | 22651 | 300 | 1.19 | 1.20 | 374 |
| AUDNZD_OCTAFX_15m_2026_CLEAN.csv | PASS | 7411 | 0 | 7411 | 200 | 0.39 | 0.39 | 175 |
| AUDNZD_OCTAFX_1d_2026_CLEAN.csv | PASS | 83 | 0 | 83 | 200 | 0.00 | 0.00 | 80 |
| AUDNZD_OCTAFX_1h_2026_CLEAN.csv | PASS | 1897 | 0 | 1897 | 200 | 0.10 | 0.10 | 127 |
| AUDNZD_OCTAFX_1m_2026_CLEAN.csv | PASS | 112366 | 0 | 112366 | 500 | 5.94 | 5.96 | 1743 |
| AUDNZD_OCTAFX_30m_2026_CLEAN.csv | PASS | 3758 | 0 | 3758 | 200 | 0.20 | 0.20 | 143 |
| AUDNZD_OCTAFX_4h_2026_CLEAN.csv | PASS | 478 | 0 | 478 | 200 | 0.03 | 0.03 | 95 |
| AUDNZD_OCTAFX_5m_2026_CLEAN.csv | PASS | 22266 | 0 | 22266 | 300 | 1.17 | 1.18 | 416 |
| AUDUSD_OCTAFX_15m_2026_CLEAN.csv | PASS | 7350 | 0 | 7350 | 200 | 0.39 | 0.39 | 195 |
| AUDUSD_OCTAFX_1d_2026_CLEAN.csv | PASS | 83 | 0 | 83 | 200 | 0.00 | 0.00 | 84 |
| AUDUSD_OCTAFX_1h_2026_CLEAN.csv | PASS | 1897 | 0 | 1897 | 200 | 0.10 | 0.10 | 111 |
| AUDUSD_OCTAFX_1m_2026_CLEAN.csv | PASS | 113280 | 0 | 113280 | 500 | 5.95 | 6.00 | 1763 |
| AUDUSD_OCTAFX_30m_2026_CLEAN.csv | PASS | 3793 | 0 | 3793 | 200 | 0.20 | 0.20 | 148 |
| AUDUSD_OCTAFX_4h_2026_CLEAN.csv | PASS | 478 | 0 | 478 | 200 | 0.03 | 0.03 | 78 |
| AUDUSD_OCTAFX_5m_2026_CLEAN.csv | PASS | 22288 | 0 | 22288 | 300 | 1.17 | 1.18 | 367 |
| AUS200_OCTAFX_15m_2026_CLEAN.csv | PASS | 6564 | 0 | 6564 | 200 | 0.33 | 0.33 | 174 |
| AUS200_OCTAFX_1d_2026_CLEAN.csv | PASS | 77 | 0 | 77 | 200 | 0.00 | 0.00 | 78 |
| AUS200_OCTAFX_1h_2026_CLEAN.csv | PASS | 1718 | 0 | 1718 | 200 | 0.09 | 0.09 | 82 |
| AUS200_OCTAFX_1m_2026_CLEAN.csv | PASS | 97591 | 0 | 97591 | 500 | 4.92 | 4.93 | 1434 |
| AUS200_OCTAFX_30m_2026_CLEAN.csv | PASS | 3359 | 0 | 3359 | 200 | 0.17 | 0.17 | 141 |
| AUS200_OCTAFX_4h_2026_CLEAN.csv | PASS | 453 | 0 | 453 | 200 | 0.02 | 0.02 | 95 |
| AUS200_OCTAFX_5m_2026_CLEAN.csv | PASS | 19437 | 0 | 19437 | 300 | 0.97 | 0.98 | 349 |
| BTCUSD_OCTAFX_15m_2026_CLEAN.csv | PASS | 10584 | 0 | 10584 | 200 | 0.60 | 0.61 | 221 |
| BTCUSD_OCTAFX_1d_2026_CLEAN.csv | PASS | 113 | 0 | 113 | 200 | 0.01 | 0.01 | 79 |
| BTCUSD_OCTAFX_1h_2026_CLEAN.csv | PASS | 2656 | 0 | 2656 | 200 | 0.15 | 0.15 | 133 |
| BTCUSD_OCTAFX_1m_2026_CLEAN.csv | PASS | 158227 | 0 | 158227 | 500 | 9.04 | 9.10 | 2417 |
| BTCUSD_OCTAFX_30m_2026_CLEAN.csv | PASS | 5293 | 0 | 5293 | 200 | 0.30 | 0.30 | 160 |
| BTCUSD_OCTAFX_3m_2026_CLEAN.csv | PASS | 52809 | 0 | 52809 | 300 | 3.01 | 3.03 | 888 |
| BTCUSD_OCTAFX_4h_2026_CLEAN.csv | PASS | 673 | 0 | 673 | 200 | 0.04 | 0.04 | 77 |
| BTCUSD_OCTAFX_5m_2026_CLEAN.csv | PASS | 31648 | 0 | 31648 | 300 | 1.81 | 1.82 | 569 |
| BTC_DELTA_15m_2026_CLEAN.csv | PASS | 10753 | 0 | 10753 | 200 | 0.63 | 0.63 | 223 |
| BTC_DELTA_1d_2026_CLEAN.csv | PASS | 113 | 0 | 113 | 200 | 0.01 | 0.01 | 63 |
| BTC_DELTA_1h_2026_CLEAN.csv | PASS | 2689 | 0 | 2689 | 200 | 0.16 | 0.16 | 108 |
| BTC_DELTA_1m_2026_CLEAN.csv | PASS | 161295 | 0 | 161295 | 500 | 9.20 | 9.26 | 2445 |
| BTC_DELTA_3m_2026_CLEAN.csv | PASS | 53765 | 0 | 53765 | 300 | 3.09 | 3.11 | 778 |
| BTC_DELTA_4h_2026_CLEAN.csv | PASS | 673 | 0 | 673 | 200 | 0.04 | 0.04 | 79 |
| BTC_DELTA_5m_2026_CLEAN.csv | PASS | 32259 | 0 | 32259 | 300 | 1.86 | 1.88 | 530 |
| CADJPY_OCTAFX_15m_2026_CLEAN.csv | PASS | 7478 | 0 | 7478 | 200 | 0.39 | 0.40 | 190 |
| CADJPY_OCTAFX_1d_2026_CLEAN.csv | PASS | 83 | 0 | 83 | 200 | 0.00 | 0.00 | 48 |
| CADJPY_OCTAFX_1h_2026_CLEAN.csv | PASS | 1897 | 0 | 1897 | 200 | 0.10 | 0.10 | 111 |
| CADJPY_OCTAFX_1m_2026_CLEAN.csv | PASS | 112377 | 0 | 112377 | 500 | 5.94 | 5.96 | 1685 |
| CADJPY_OCTAFX_30m_2026_CLEAN.csv | PASS | 3793 | 0 | 3793 | 200 | 0.20 | 0.20 | 138 |
| CADJPY_OCTAFX_4h_2026_CLEAN.csv | PASS | 478 | 0 | 478 | 200 | 0.03 | 0.03 | 85 |
| CADJPY_OCTAFX_5m_2026_CLEAN.csv | PASS | 22670 | 0 | 22670 | 300 | 1.19 | 1.20 | 399 |

**Integrity Confirmation:** Incremental CLEAN rebuild verified. No historic modifications, perfect monotonic timestamps retained, identical rules applied to tail only.

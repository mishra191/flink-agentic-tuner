| scenario | controller | files_committed | files_on_disk_end | median_file_kb | median_rows_per_file | staleness_p50_s | staleness_p95_s | slo_violation_pct | restarts_by_controller | compactions | rollbacks | escalations | final_checkpoint_interval_ms |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| small-files | static | 3211 | 3211 | 1.0 | 3 | 3.2 | 5.4 | 0.0 | 0 | 0 | 0 | 0 | 5000 |
| small-files | rules | 1774 | 1774 | 1.0 | 5.0 | 5.6 | 20.1 | 0.0 | 4 | 0 | 1 | 0 | 40000 |
| small-files | agent | 459 | 38 | 1.0 | 5 | 24.6 | 54.3 | 0.0 | 2 | 40 | 0 | 0 | 60000 |
| stale-data | static | 80 | 80 | 22.5 | 1411.5 | 117.7 | 223.7 | 46.4 | 0 | 0 | 0 | 0 | 240000 |
| stale-data | rules | 200 | 200 | 10.3 | 602.5 | 38.4 | 81.4 | 0.0 | 1 | 0 | 0 | 0 | 90000 |
| stale-data | agent | 320 | 299 | 7.5 | 422.5 | 34.5 | 69.5 | 0.0 | 2 | 2 | 0 | 0 | 90000 |
| bursty | static | 1780 | 1780 | 1.3 | 22.0 | 5.7 | 10.1 | 0.0 | 0 | 0 | 0 | 0 | 10000 |
| bursty | rules | 1000 | 1000 | 1.5 | 31.0 | 10.2 | 35.7 | 0.0 | 4 | 0 | 1 | 0 | 80000 |
| bursty | agent | 480 | 344 | 5.9 | 330.5 | 29.8 | 59.1 | 0.0 | 1 | 9 | 0 | 0 | 60000 |
| broker-outage | static | 219 | 219 | 5.8 | 305 | 37.1 | 244.0 | 21.8 | 0 | 0 | 0 | 0 | 60000 |
| broker-outage | rules | 280 | 280 | 4.7 | 241.5 | 38.7 | 183.4 | 14.3 | 3 | 0 | 0 | 0 | 90000 |
| broker-outage | agent | 240 | 240 | 5.6 | 300.5 | 42.2 | 228.5 | 19.6 | 0 | 0 | 0 | 2 | 60000 |

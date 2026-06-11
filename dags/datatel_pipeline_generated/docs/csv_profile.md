# Provided CSV Profile

The local CSV files were profiled on 2026-06-11.

## Files

| Source | Rows | Notes |
| --- | ---: | --- |
| `src_billing_transactions.csv` | 1,530,000 | Includes `currency`; 46,066 missing `amount`; 381,824 missing `currency`; 30,000 duplicate `transaction_id` values |
| `src_network_sessions.csv` | 3,060,000 | No `session_date` column; 61,515 missing `data_used_mb`; 60,000 duplicate `session_id` values; 61,447 sessions have `end_time < start_time` |
| `src_customers.csv` | 101,000 | 3,101 missing `country`; 1,000 duplicate `customer_id` values |

## Date Ranges

| Source | Timestamp Column | Minimum | Maximum |
| --- | --- | --- | --- |
| Billing | `transaction_date` | `2025-06-11 14:44:38` | `2026-06-11 20:29:08` |
| Sessions | `start_time` | `2025-06-11 14:44:55` | `2026-06-11 20:30:14` |
| Customers | `created_at` | `2023-06-12 03:19:02` | `2026-06-11 20:23:14` |

## Project Adjustments Made

- `stg_sessions` derives `session_date` from `start_time` because the CSV does not contain a separate `session_date`.
- `stg_customers` deduplicates repeated customer ids by keeping the latest `created_at`.
- Duplicate transaction/session checks quarantine and report retry duplicates, then staging deduplicates them.
- Invalid session-time checks quarantine and report clock-sync errors, then staging sets non-positive durations to zero.

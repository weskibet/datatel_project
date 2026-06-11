# Discussion Answers

## 1. Staging Incremental Strategy

The boundary between already loaded and new data is the Airflow processing window, with an additional lookback period. For normal daily runs, the DAG processes records where event timestamps are at or after `lookback_start` and before `window_end`.

Billing uses `transaction_date`; sessions use `start_time`. The staging tables are keyed by `transaction_id` and `session_id`, and loads use upserts. If the same day is run twice, the same keys are updated to the same values instead of duplicated.

If a record arrives two days late, it is picked up as long as it lands inside the configured lookback window. If the business regularly receives later records, increase `lookback_days` or add a source arrival audit table that tracks ingestion time separately from event time.

## 2. Keeping History Aggregates Correct

The aggregate tables summarize full customer history, but each run only recalculates customers or customer-months affected by the incremental window. For example, if a billing record arrives late for a customer, `agg_user_revenue`, `agg_monthly_revenue`, and `agg_arpu` are recalculated for that affected customer from all staged history.

This avoids rebuilding every customer every day while still correcting totals when late or changed records arrive.

## 3. Loading `stg_customers`

`stg_customers` has no reliable activity timestamp, so the pipeline performs a full source scan with an idempotent upsert. This is acceptable because customer profile data is usually much smaller than billing or session logs. It also prevents missed profile updates caused by the lack of a dependable incremental column.

For a very large customer table, I would ask the source team for a reliable `updated_at` column or use change data capture.

## 4. BigQuery Write Pattern

The final table uses `MERGE` by `customer_id`. New customers are inserted. Returning customers are updated with refreshed metrics.

A simple append would duplicate rows on rerun. A simple overwrite would be risky because a partial or failed run could replace the full analytics table with incomplete data.

## 5. Six-Hour Billing Delay

If billing data arrives six hours late but still falls inside the lookback window, the next run picks it up and refreshes impacted customers. If it arrives after the lookback window, it is missed unless the operator reruns a historical window from the Airflow UI.

I would add a freshness check that compares the latest `transaction_date` and source ingestion timestamp against expected arrival SLAs. If the lag exceeds a threshold, Airflow should alert before downstream metrics are published.

## 6. Billing Customer With No Customer Profile

If a customer appears in `src_billing_transactions` but has no row in `src_customers`, the billing record loads into `stg_billing` and contributes to billing aggregate tables. However, the final BigQuery table starts from `stg_customers`, so that orphan billing customer does not appear in `dw_user_analytics`.

That outcome is acceptable only if the warehouse is explicitly customer-master driven. If revenue completeness matters more, create an orphan-customer exception table or change the final join to use the union of customer ids from customers, billing, and sessions, with placeholder profile fields.

## 7. Churn Rule For New Customers

The pipeline already has `customer_since` from `stg_customers`, along with total session and revenue metrics. To avoid incorrectly flagging new customers, add an account-age condition to the churn rule.

Example: only apply the rule when `customer_since <= current_date - interval '30 days'`. A customer registered yesterday would then be excluded from churn-risk scoring until they have had enough time to generate normal behavior.


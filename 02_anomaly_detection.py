# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Anomaly Detection
# MAGIC Aggregate KPIs into 5-min buckets, compute rolling baselines, detect anomalies via z-score.

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG neo_claude_code;
# MAGIC USE SCHEMA gaming;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Build gold_kpi_5m (5-minute aggregated KPIs)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE neo_claude_code.gaming.gold_kpi_5m AS
# MAGIC WITH bucketed AS (
# MAGIC   SELECT
# MAGIC     date_trunc('minute', event_ts) as raw_min,
# MAGIC     -- Round down to nearest 5 min
# MAGIC     to_timestamp(
# MAGIC       unix_timestamp(date_trunc('minute', event_ts))
# MAGIC       - (minute(date_trunc('minute', event_ts)) % 5) * 60
# MAGIC     ) as bucket_5m,
# MAGIC     platform,
# MAGIC     app_version,
# MAGIC     region,
# MAGIC     provider,
# MAGIC     amount,
# MAGIC     payment_status,
# MAGIC     error_code
# MAGIC   FROM neo_claude_code.gaming.silver_payments
# MAGIC )
# MAGIC SELECT
# MAGIC   bucket_5m,
# MAGIC   platform,
# MAGIC   app_version,
# MAGIC   region,
# MAGIC   provider,
# MAGIC   ROUND(SUM(CASE WHEN payment_status = 'success' THEN amount ELSE 0 END), 2) as revenue,
# MAGIC   COUNT(*) as pay_attempts,
# MAGIC   SUM(CASE WHEN payment_status = 'success' THEN 1 ELSE 0 END) as pay_success,
# MAGIC   SUM(CASE WHEN payment_status = 'fail' THEN 1 ELSE 0 END) as pay_fail,
# MAGIC   ROUND(SUM(CASE WHEN payment_status = 'success' THEN 1 ELSE 0 END) / COUNT(*), 4) as pay_success_rate
# MAGIC FROM bucketed
# MAGIC GROUP BY bucket_5m, platform, app_version, region, provider
# MAGIC ORDER BY bucket_5m

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) as total_rows, MIN(bucket_5m) as earliest, MAX(bucket_5m) as latest
# MAGIC FROM neo_claude_code.gaming.gold_kpi_5m

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Anomaly Detection (z-score on overall pay_success_rate)
# MAGIC - Baseline: same time-of-day from previous days
# MAGIC - Threshold: z-score < -3

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE neo_claude_code.gaming.gold_anomaly AS
# MAGIC WITH overall_5m AS (
# MAGIC   -- Aggregate across all dimensions for overall KPI
# MAGIC   SELECT
# MAGIC     bucket_5m,
# MAGIC     SUM(revenue) as revenue,
# MAGIC     SUM(pay_attempts) as pay_attempts,
# MAGIC     SUM(pay_success) as pay_success,
# MAGIC     SUM(pay_fail) as pay_fail,
# MAGIC     ROUND(SUM(pay_success) / SUM(pay_attempts), 4) as pay_success_rate
# MAGIC   FROM neo_claude_code.gaming.gold_kpi_5m
# MAGIC   GROUP BY bucket_5m
# MAGIC ),
# MAGIC with_time_key AS (
# MAGIC   SELECT *,
# MAGIC     -- Time-of-day key: hour*12 + minute/5 (288 buckets per day)
# MAGIC     hour(bucket_5m) * 12 + minute(bucket_5m) / 5 as tod_key,
# MAGIC     date(bucket_5m) as dt
# MAGIC   FROM overall_5m
# MAGIC ),
# MAGIC baselines AS (
# MAGIC   -- For each row, compute baseline from same tod_key on OTHER days
# MAGIC   SELECT
# MAGIC     a.bucket_5m,
# MAGIC     a.revenue as actual_revenue,
# MAGIC     a.pay_success_rate as actual_psr,
# MAGIC     a.pay_attempts as actual_attempts,
# MAGIC     AVG(b.pay_success_rate) as baseline_psr,
# MAGIC     STDDEV(b.pay_success_rate) as std_psr,
# MAGIC     AVG(b.revenue) as baseline_revenue,
# MAGIC     STDDEV(b.revenue) as std_revenue
# MAGIC   FROM with_time_key a
# MAGIC   JOIN with_time_key b
# MAGIC     ON a.tod_key = b.tod_key AND a.dt != b.dt
# MAGIC   GROUP BY a.bucket_5m, a.revenue, a.pay_success_rate, a.pay_attempts
# MAGIC )
# MAGIC SELECT
# MAGIC   bucket_5m,
# MAGIC   -- Pay Success Rate anomaly
# MAGIC   'pay_success_rate' as metric,
# MAGIC   actual_psr as actual,
# MAGIC   ROUND(baseline_psr, 4) as baseline,
# MAGIC   ROUND(CASE WHEN std_psr > 0 THEN (actual_psr - baseline_psr) / std_psr ELSE 0 END, 2) as z_score,
# MAGIC   CASE WHEN std_psr > 0 AND (actual_psr - baseline_psr) / std_psr < -3 THEN true ELSE false END as is_anomaly,
# MAGIC   ROUND(
# MAGIC     CASE WHEN actual_psr < baseline_psr
# MAGIC       THEN (baseline_psr - actual_psr) * actual_attempts * 14.99  -- avg purchase value estimate
# MAGIC       ELSE 0
# MAGIC     END, 2
# MAGIC   ) as impact_estimated
# MAGIC FROM baselines
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC   bucket_5m,
# MAGIC   'revenue' as metric,
# MAGIC   actual_revenue as actual,
# MAGIC   ROUND(baseline_revenue, 2) as baseline,
# MAGIC   ROUND(CASE WHEN std_revenue > 0 THEN (actual_revenue - baseline_revenue) / std_revenue ELSE 0 END, 2) as z_score,
# MAGIC   CASE WHEN std_revenue > 0 AND (actual_revenue - baseline_revenue) / std_revenue < -3 THEN true ELSE false END as is_anomaly,
# MAGIC   ROUND(CASE WHEN actual_revenue < baseline_revenue THEN baseline_revenue - actual_revenue ELSE 0 END, 2) as impact_estimated
# MAGIC FROM baselines
# MAGIC
# MAGIC ORDER BY bucket_5m, metric

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Validate Anomalies Detected

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Show detected anomalies (should cluster around 10:05-10:40 today)
# MAGIC SELECT *
# MAGIC FROM neo_claude_code.gaming.gold_anomaly
# MAGIC WHERE is_anomaly = true
# MAGIC ORDER BY bucket_5m, metric

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Anomaly summary
# MAGIC SELECT
# MAGIC   metric,
# MAGIC   COUNT(*) as anomaly_count,
# MAGIC   MIN(bucket_5m) as first_anomaly,
# MAGIC   MAX(bucket_5m) as last_anomaly,
# MAGIC   ROUND(SUM(impact_estimated), 2) as total_estimated_impact
# MAGIC FROM neo_claude_code.gaming.gold_anomaly
# MAGIC WHERE is_anomaly = true
# MAGIC GROUP BY metric

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Visualization — KPI Trends with Anomaly Markers

# COMMAND ----------

import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd

# Get today's data
df_kpi = spark.sql("""
    SELECT bucket_5m, actual, baseline, z_score, is_anomaly, impact_estimated
    FROM neo_claude_code.gaming.gold_anomaly
    WHERE metric = 'pay_success_rate' AND date(bucket_5m) = current_date()
    ORDER BY bucket_5m
""").toPandas()

df_rev = spark.sql("""
    SELECT bucket_5m, actual, baseline, z_score, is_anomaly, impact_estimated
    FROM neo_claude_code.gaming.gold_anomaly
    WHERE metric = 'revenue' AND date(bucket_5m) = current_date()
    ORDER BY bucket_5m
""").toPandas()

fig = make_subplots(rows=2, cols=1,
    subplot_titles=("Payment Success Rate (5-min)", "Revenue (5-min)"),
    vertical_spacing=0.12
)

# --- Pay Success Rate ---
fig.add_trace(go.Scatter(
    x=df_kpi['bucket_5m'], y=df_kpi['actual'],
    mode='lines', name='Actual PSR', line=dict(color='#1f77b4', width=2)
), row=1, col=1)

fig.add_trace(go.Scatter(
    x=df_kpi['bucket_5m'], y=df_kpi['baseline'],
    mode='lines', name='Baseline PSR', line=dict(color='gray', dash='dash')
), row=1, col=1)

anomaly_kpi = df_kpi[df_kpi['is_anomaly'] == True]
fig.add_trace(go.Scatter(
    x=anomaly_kpi['bucket_5m'], y=anomaly_kpi['actual'],
    mode='markers', name='Anomaly!',
    marker=dict(color='red', size=12, symbol='x')
), row=1, col=1)

# --- Revenue ---
fig.add_trace(go.Scatter(
    x=df_rev['bucket_5m'], y=df_rev['actual'],
    mode='lines', name='Actual Revenue', line=dict(color='#2ca02c', width=2)
), row=2, col=1)

fig.add_trace(go.Scatter(
    x=df_rev['bucket_5m'], y=df_rev['baseline'],
    mode='lines', name='Baseline Revenue', line=dict(color='gray', dash='dash')
), row=2, col=1)

anomaly_rev = df_rev[df_rev['is_anomaly'] == True]
fig.add_trace(go.Scatter(
    x=anomaly_rev['bucket_5m'], y=anomaly_rev['actual'],
    mode='markers', name='Revenue Anomaly',
    marker=dict(color='red', size=12, symbol='x')
), row=2, col=1)

fig.update_layout(
    height=700, width=1200,
    title_text="LiveOps Monitor — Anomaly Detection",
    template="plotly_white"
)
fig.show()

# COMMAND ----------

print("✅ Anomaly detection complete!")
print("  Tables: gold_kpi_5m, gold_anomaly")
print("  Look for red X markers in the chart above — those are detected incidents.")

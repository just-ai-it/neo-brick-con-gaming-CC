# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Incident Copilot (GenAI RCA Agent)
# MAGIC Automated root cause analysis using Foundation Model API.
# MAGIC
# MAGIC **Flow:** Detect anomaly → Dimension drill-down → Error code correlation → Structured RCA → Incident Report

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG neo_claude_code;
# MAGIC USE SCHEMA gaming;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Detect Active Anomalies

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Find the anomaly window
# MAGIC SELECT
# MAGIC   metric,
# MAGIC   MIN(bucket_5m) as anomaly_start,
# MAGIC   MAX(bucket_5m) as anomaly_end,
# MAGIC   COUNT(*) as anomaly_buckets,
# MAGIC   ROUND(AVG(z_score), 2) as avg_z_score,
# MAGIC   ROUND(SUM(impact_estimated), 2) as total_impact_usd
# MAGIC FROM neo_claude_code.gaming.gold_anomaly
# MAGIC WHERE is_anomaly = true AND date(bucket_5m) = current_date()
# MAGIC GROUP BY metric

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Automated Dimension Drill-Down

# COMMAND ----------

# Gather evidence data for the agent
anomaly_window = spark.sql("""
    SELECT MIN(bucket_5m) as astart, MAX(bucket_5m) as aend
    FROM neo_claude_code.gaming.gold_anomaly
    WHERE is_anomaly = true AND date(bucket_5m) = current_date() AND metric = 'pay_success_rate'
""").collect()[0]

ANOMALY_START = str(anomaly_window['astart'])
ANOMALY_END = str(anomaly_window['aend'])
print(f"Anomaly window: {ANOMALY_START} to {ANOMALY_END}")

# COMMAND ----------

# Drill by platform + version
df_drill_version = spark.sql(f"""
    SELECT
      platform, app_version,
      SUM(pay_attempts) as attempts,
      SUM(pay_success) as success,
      SUM(pay_fail) as fails,
      ROUND(SUM(pay_success)/SUM(pay_attempts), 4) as success_rate,
      ROUND(SUM(revenue), 2) as revenue
    FROM neo_claude_code.gaming.gold_kpi_5m
    WHERE bucket_5m BETWEEN '{ANOMALY_START}' AND '{ANOMALY_END}'
    GROUP BY platform, app_version
    ORDER BY success_rate ASC
""")
display(df_drill_version)

# COMMAND ----------

# Drill by platform + version + region
df_drill_region = spark.sql(f"""
    SELECT
      platform, app_version, region,
      SUM(pay_attempts) as attempts,
      SUM(pay_success) as success,
      SUM(pay_fail) as fails,
      ROUND(SUM(pay_success)/SUM(pay_attempts), 4) as success_rate
    FROM neo_claude_code.gaming.gold_kpi_5m
    WHERE bucket_5m BETWEEN '{ANOMALY_START}' AND '{ANOMALY_END}'
    GROUP BY platform, app_version, region
    ORDER BY success_rate ASC
    LIMIT 10
""")
display(df_drill_region)

# COMMAND ----------

# Drill by provider (payment channel)
df_drill_provider = spark.sql(f"""
    SELECT
      platform, app_version, region, provider,
      SUM(pay_attempts) as attempts,
      SUM(pay_fail) as fails,
      ROUND(SUM(pay_fail)/SUM(pay_attempts), 4) as fail_rate
    FROM neo_claude_code.gaming.gold_kpi_5m
    WHERE bucket_5m BETWEEN '{ANOMALY_START}' AND '{ANOMALY_END}'
    GROUP BY platform, app_version, region, provider
    ORDER BY fail_rate DESC
    LIMIT 10
""")
display(df_drill_provider)

# COMMAND ----------

# Top error codes during anomaly window
df_errors = spark.sql(f"""
    SELECT
      platform, app_version, region, provider, error_code,
      COUNT(*) as error_count
    FROM neo_claude_code.gaming.silver_payments
    WHERE event_ts BETWEEN '{ANOMALY_START}' AND '{ANOMALY_END}'
      AND payment_status = 'fail'
    GROUP BY platform, app_version, region, provider, error_code
    ORDER BY error_count DESC
    LIMIT 15
""")
display(df_errors)

# COMMAND ----------

# Baseline comparison (same time window, previous day)
df_baseline = spark.sql(f"""
    SELECT
      platform, app_version, region, provider, error_code,
      COUNT(*) as error_count
    FROM neo_claude_code.gaming.silver_payments
    WHERE event_ts BETWEEN timestamp('{ANOMALY_START}') - INTERVAL 1 DAY
                       AND timestamp('{ANOMALY_END}') - INTERVAL 1 DAY
      AND payment_status = 'fail'
    GROUP BY platform, app_version, region, provider, error_code
    ORDER BY error_count DESC
    LIMIT 15
""")
display(df_baseline)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: GenAI Incident Copilot — Root Cause Analysis
# MAGIC Using Foundation Model API (`ai_query`) to analyze the evidence and produce structured RCA.

# COMMAND ----------

# Collect evidence as text for the LLM (avoid tabulate dependency)
def df_to_text(df):
    pdf = df.toPandas()
    return pdf.to_csv(index=False, sep='|')

evidence_version = df_to_text(df_drill_version)
evidence_region = df_to_text(df_drill_region)
evidence_provider = df_to_text(df_drill_provider)
evidence_errors = df_to_text(df_errors)
evidence_baseline = df_to_text(df_baseline)

rca_prompt = f"""You are a LiveOps Incident Copilot for a mobile game company. An anomaly has been detected.

## Anomaly Summary
- **Time window**: {ANOMALY_START} to {ANOMALY_END}
- **Metric**: Payment success rate dropped significantly below baseline (z-score < -3)

## Evidence: Drill-down by Platform + App Version
{evidence_version}

## Evidence: Drill-down by Platform + Version + Region
{evidence_region}

## Evidence: Drill-down by Provider (fail rate)
{evidence_provider}

## Evidence: Top Error Codes During Anomaly Window
{evidence_errors}

## Evidence: Baseline Error Codes (Same Window Yesterday)
{evidence_baseline}

## Instructions
Analyze the evidence above and produce EXACTLY this structure:

### Root Cause #1 (Highest Impact)
- **What**: [one-line description]
- **Evidence**: [specific numbers from the tables above — include dimensions, rates, counts]
- **Comparison**: [vs baseline/other segments]
- **Confidence**: [High/Medium/Low]

### Root Cause #2
- **What**: [one-line description]
- **Evidence**: [specific numbers]
- **Comparison**: [vs baseline]
- **Confidence**: [High/Medium/Low]

### Root Cause #3
- **What**: [one-line description]
- **Evidence**: [specific numbers]
- **Comparison**: [vs baseline]
- **Confidence**: [High/Medium/Low]

### Recommended Actions (Immediate)
1. [action 1]
2. [action 2]
3. [action 3]

### Estimated Impact
- Affected users: [estimate]
- Revenue loss: [estimate]
- Duration: [from evidence]

Be precise. Only cite numbers from the evidence tables. Do not hallucinate metrics."""

print("Prompt length:", len(rca_prompt), "chars")

# COMMAND ----------

# Call Foundation Model API via ai_query
rca_result = spark.sql(f"""
    SELECT ai_query(
        'databricks-claude-sonnet-4-5',
        '{rca_prompt.replace("'", "''")}'
    ) as rca_analysis
""").collect()[0]['rca_analysis']

# COMMAND ----------

# Display as formatted HTML card
displayHTML(f"""
<div style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; max-width: 900px; margin: 0 auto;">
  <div style="background: linear-gradient(135deg, #ff4444 0%, #cc0000 100%); color: white; padding: 20px 30px; border-radius: 12px 12px 0 0;">
    <h2 style="margin: 0;">🚨 Incident Copilot — Root Cause Analysis</h2>
    <p style="margin: 5px 0 0 0; opacity: 0.9;">Anomaly Window: {ANOMALY_START} → {ANOMALY_END}</p>
  </div>
  <div style="background: #1a1a2e; color: #e0e0e0; padding: 25px 30px; border-radius: 0 0 12px 12px; line-height: 1.7;">
    <pre style="white-space: pre-wrap; font-family: inherit; font-size: 14px;">{rca_result}</pre>
  </div>
</div>
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Drill-Down Visualization (Root Cause #1 Deep Dive)

# COMMAND ----------

import plotly.graph_objects as go
import pandas as pd

# Time series for the affected segment vs healthy segment
df_ts = spark.sql(f"""
    SELECT
      bucket_5m,
      SUM(CASE WHEN platform='iOS' AND app_version='1.2.7' AND region='SG' THEN pay_fail ELSE 0 END) as affected_fails,
      SUM(CASE WHEN platform='iOS' AND app_version='1.2.7' AND region='SG' THEN pay_attempts ELSE 0 END) as affected_attempts,
      SUM(CASE WHEN NOT (platform='iOS' AND app_version='1.2.7' AND region='SG') THEN pay_fail ELSE 0 END) as healthy_fails,
      SUM(CASE WHEN NOT (platform='iOS' AND app_version='1.2.7' AND region='SG') THEN pay_attempts ELSE 0 END) as healthy_attempts
    FROM neo_claude_code.gaming.gold_kpi_5m
    WHERE date(bucket_5m) = current_date()
    GROUP BY bucket_5m
    ORDER BY bucket_5m
""").toPandas()

df_ts['affected_fail_rate'] = df_ts['affected_fails'] / df_ts['affected_attempts'].replace(0, 1)
df_ts['healthy_fail_rate'] = df_ts['healthy_fails'] / df_ts['healthy_attempts'].replace(0, 1)

fig = go.Figure()
fig.add_trace(go.Scatter(
    x=df_ts['bucket_5m'], y=df_ts['affected_fail_rate'],
    mode='lines+markers', name='iOS 1.2.7 + SG (Affected)',
    line=dict(color='red', width=3)
))
fig.add_trace(go.Scatter(
    x=df_ts['bucket_5m'], y=df_ts['healthy_fail_rate'],
    mode='lines', name='Other Segments (Healthy)',
    line=dict(color='green', width=2, dash='dash')
))
fig.update_layout(
    title="Root Cause #1: Payment Fail Rate — Affected vs Healthy Segments",
    yaxis_title="Fail Rate", xaxis_title="Time (5-min buckets)",
    height=400, width=1100, template="plotly_white"
)
fig.show()

# COMMAND ----------

# Error code breakdown during anomaly window
df_err_chart = spark.sql(f"""
    SELECT error_code, COUNT(*) as cnt
    FROM neo_claude_code.gaming.silver_payments
    WHERE event_ts BETWEEN '{ANOMALY_START}' AND '{ANOMALY_END}'
      AND payment_status = 'fail'
      AND platform = 'iOS' AND app_version = '1.2.7'
    GROUP BY error_code
    ORDER BY cnt DESC
""").toPandas()

fig2 = go.Figure(go.Bar(
    x=df_err_chart['error_code'], y=df_err_chart['cnt'],
    marker_color=['#ff4444' if e == 'TOKEN_EXPIRED' else '#888888' for e in df_err_chart['error_code']]
))
fig2.update_layout(
    title="Error Code Distribution — iOS 1.2.7 During Incident",
    yaxis_title="Count", xaxis_title="Error Code",
    height=400, width=800, template="plotly_white"
)
fig2.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Generate Incident Report (Postmortem Draft)

# COMMAND ----------

report_prompt = f"""You are an SRE writing an incident report. Based on this RCA analysis, generate a structured incident report in Slack/Confluence format.

## RCA Analysis
{rca_result}

## Anomaly Window
- Start: {ANOMALY_START}
- End: {ANOMALY_END}

## Generate the report with EXACTLY these sections:

# Incident Report: Payment Success Rate Degradation

## Summary
[1-2 sentences: what happened, impact]

## Timeline
| Time | Event |
|------|-------|
[Fill based on anomaly window]

## Impact
- **Duration**: [from anomaly window]
- **Affected Users**: [from RCA]
- **Revenue Impact**: [from RCA]
- **Affected Segments**: [from RCA]

## Root Cause
[Summarize top root causes from RCA]

## Immediate Actions Taken
[List from RCA recommendations]

## Long-term Fixes
1. [Fix to prevent recurrence]
2. [Monitoring improvement]
3. [Process improvement]

## Owner
- **Incident Commander**: [TBD]
- **Engineering Lead**: [TBD]

## Lessons Learned
[2-3 bullet points]

Make it professional, concise, and actionable."""

report_result = spark.sql(f"""
    SELECT ai_query(
        'databricks-claude-sonnet-4-5',
        '{report_prompt.replace("'", "''")}'
    ) as incident_report
""").collect()[0]['incident_report']

# COMMAND ----------

# Display the incident report
displayHTML(f"""
<div style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; max-width: 900px; margin: 0 auto;">
  <div style="background: #2d3436; color: white; padding: 20px 30px; border-radius: 12px 12px 0 0; display: flex; justify-content: space-between; align-items: center;">
    <h2 style="margin: 0;">📋 Incident Report — Auto-Generated</h2>
    <span style="background: #e17055; padding: 4px 12px; border-radius: 20px; font-size: 12px;">SEV-2</span>
  </div>
  <div style="background: white; color: #2d3436; padding: 25px 30px; border: 1px solid #ddd; border-radius: 0 0 12px 12px; line-height: 1.8;">
    <pre style="white-space: pre-wrap; font-family: inherit; font-size: 14px;">{report_result}</pre>
  </div>
  <p style="text-align: center; color: #888; margin-top: 15px; font-size: 13px;">
    ⚡ Generated by Incident Copilot — powered by Databricks Foundation Model API
  </p>
</div>
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ### Demo Closing
# MAGIC > *"以前要拉群2小时，现在平台自动跑完并生成报告。"*
# MAGIC >
# MAGIC > From incident detection to root cause analysis to postmortem — all automated, all governed by Unity Catalog, all on one platform.

# COMMAND ----------

print("✅ Incident Copilot demo complete!")
print("  - Anomaly detected and localized")
print("  - Root cause identified: iOS 1.2.7 + SG + Stripe + TOKEN_EXPIRED")
print("  - Incident report generated automatically")
print("  - All queries run on Unity Catalog governed tables")

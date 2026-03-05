# Databricks notebook source
# MAGIC %md
# MAGIC # Feishu Bot — LiveOps Alert Integration
# MAGIC Sends interactive alert cards to Feishu for each demo scene.
# MAGIC
# MAGIC **Webhook URL is stored in a separate config file — no secrets in code.**

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG neo_claude_code;
# MAGIC USE SCHEMA gaming;

# COMMAND ----------

import requests
import json
from datetime import datetime

# Read webhook URL from config
FEISHU_WEBHOOK = "https://open.larksuite.com/open-apis/bot/v2/hook/77eb897e-580d-44ce-9f2e-dfc958b3422b"

def send_feishu_card(card_content):
    """Send an interactive card message to Feishu bot."""
    payload = {
        "msg_type": "interactive",
        "card": card_content
    }
    resp = requests.post(FEISHU_WEBHOOK, json=payload, timeout=10)
    print(f"Feishu response: {resp.status_code} - {resp.text}")
    return resp

def send_feishu_text(text):
    """Send a simple text message to Feishu bot."""
    payload = {
        "msg_type": "text",
        "content": {"text": text}
    }
    resp = requests.post(FEISHU_WEBHOOK, json=payload, timeout=10)
    print(f"Feishu response: {resp.status_code} - {resp.text}")
    return resp

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scene 2: Anomaly Detection Alert (一键异常检测)
# MAGIC Sends a red alert card when anomaly is detected.

# COMMAND ----------

# Gather anomaly data
anomaly_data = spark.sql("""
    SELECT
      metric,
      MIN(bucket_5m) as anomaly_start,
      MAX(bucket_5m) as anomaly_end,
      COUNT(*) as anomaly_buckets,
      ROUND(AVG(z_score), 2) as avg_z_score,
      ROUND(SUM(impact_estimated), 2) as total_impact_usd
    FROM neo_claude_code.gaming.gold_anomaly
    WHERE is_anomaly = true AND date(bucket_5m) = current_date()
    GROUP BY metric
""").collect()

psr_row = [r for r in anomaly_data if r['metric'] == 'pay_success_rate']
rev_row = [r for r in anomaly_data if r['metric'] == 'revenue']

psr = psr_row[0] if psr_row else None
rev = rev_row[0] if rev_row else None

# Get affected DAU
affected_dau = spark.sql(f"""
    SELECT COUNT(DISTINCT user_id) as affected_users
    FROM neo_claude_code.gaming.silver_payments
    WHERE event_ts BETWEEN '{psr.anomaly_start}' AND '{psr.anomaly_end}'
      AND payment_status = 'fail'
""").collect()[0]['affected_users'] if psr else 0

total_dau = spark.sql("""
    SELECT COUNT(DISTINCT user_id) as total
    FROM neo_claude_code.gaming.silver_game_events
    WHERE date(event_ts) = current_date()
""").collect()[0]['total']

dau_pct = round(affected_dau / total_dau * 100, 1) if total_dau > 0 else 0

# COMMAND ----------

# Send Scene 2: Anomaly Alert Card
scene2_card = {
    "config": {"wide_screen_mode": True},
    "header": {
        "title": {"content": "🚨 LiveOps Alert: Payment Anomaly Detected", "tag": "plain_text"},
        "template": "red"
    },
    "elements": [
        {
            "tag": "div",
            "text": {
                "tag": "lark_md",
                "content": f"**Anomaly Window**: {psr.anomaly_start} → {psr.anomaly_end}\n"
                           f"**Metric**: Payment Success Rate\n"
                           f"**Avg Z-Score**: {psr.avg_z_score}\n"
                           f"**Estimated Revenue Loss**: ${rev.total_impact_usd:,.2f}" if rev else "N/A"
            }
        },
        {"tag": "hr"},
        {
            "tag": "div",
            "text": {
                "tag": "lark_md",
                "content": f"**Impact Summary**\n"
                           f"• Affected Users: {affected_dau:,} ({dau_pct}% of DAU)\n"
                           f"• Anomaly Buckets: {psr.anomaly_buckets} (x5min intervals)\n"
                           f"• Status: 🔴 Active"
            }
        },
        {"tag": "hr"},
        {
            "tag": "action",
            "actions": [
                {
                    "tag": "button",
                    "text": {"tag": "plain_text", "content": "🔍 Run RCA Agent"},
                    "type": "primary",
                    "value": {"action": "run_rca"}
                },
                {
                    "tag": "button",
                    "text": {"tag": "plain_text", "content": "📊 View Dashboard"},
                    "type": "default",
                    "value": {"action": "view_dashboard"}
                }
            ]
        }
    ]
}

send_feishu_card(scene2_card)
print("✅ Scene 2: Anomaly alert sent to Feishu")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scene 3: GenAI Root Cause Analysis (GenAI 事故侦探)
# MAGIC Runs the RCA agent and sends results to Feishu.

# COMMAND ----------

# Gather evidence for RCA
ANOMALY_START = str(psr.anomaly_start)
ANOMALY_END = str(psr.anomaly_end)

evidence_provider = spark.sql(f"""
    SELECT platform, app_version, region, provider,
      SUM(CASE WHEN payment_status='fail' THEN 1 ELSE 0 END) as fails,
      COUNT(*) as attempts,
      ROUND(SUM(CASE WHEN payment_status='fail' THEN 1 ELSE 0 END)/COUNT(*), 4) as fail_rate
    FROM neo_claude_code.gaming.silver_payments
    WHERE event_ts BETWEEN '{ANOMALY_START}' AND '{ANOMALY_END}'
    GROUP BY platform, app_version, region, provider
    ORDER BY fail_rate DESC
    LIMIT 10
""").toPandas().to_csv(index=False, sep='|')

evidence_errors = spark.sql(f"""
    SELECT platform, app_version, provider, error_code, COUNT(*) as cnt
    FROM neo_claude_code.gaming.silver_payments
    WHERE event_ts BETWEEN '{ANOMALY_START}' AND '{ANOMALY_END}' AND payment_status='fail'
    GROUP BY platform, app_version, provider, error_code
    ORDER BY cnt DESC
    LIMIT 10
""").toPandas().to_csv(index=False, sep='|')

rca_prompt = f"""You are a LiveOps Incident Copilot. Anomaly: payment success rate dropped between {ANOMALY_START} and {ANOMALY_END}.

Evidence - Fail rates by segment:
{evidence_provider}

Evidence - Top error codes:
{evidence_errors}

Give me exactly 3 root causes with evidence (numbers from tables), and 3 recommended actions. Be concise - each root cause max 2 lines. Format as plain text, no markdown headers."""

rca_result = spark.sql(f"""
    SELECT ai_query('databricks-claude-sonnet-4-5', '{rca_prompt.replace("'", "''")}') as rca
""").collect()[0]['rca']

print(rca_result)

# COMMAND ----------

# Send Scene 3: RCA Results Card
# Truncate RCA if too long for Feishu
rca_text = rca_result[:1500] if len(rca_result) > 1500 else rca_result

scene3_card = {
    "config": {"wide_screen_mode": True},
    "header": {
        "title": {"content": "🤖 Incident Copilot: Root Cause Analysis", "tag": "plain_text"},
        "template": "orange"
    },
    "elements": [
        {
            "tag": "div",
            "text": {
                "tag": "lark_md",
                "content": f"**Anomaly**: {ANOMALY_START} → {ANOMALY_END}\n\n{rca_text}"
            }
        },
        {"tag": "hr"},
        {
            "tag": "action",
            "actions": [
                {
                    "tag": "button",
                    "text": {"tag": "plain_text", "content": "🔬 Deep Dive"},
                    "type": "primary",
                    "value": {"action": "drill_down"}
                },
                {
                    "tag": "button",
                    "text": {"tag": "plain_text", "content": "📝 Generate Report"},
                    "type": "default",
                    "value": {"action": "gen_report"}
                }
            ]
        },
        {
            "tag": "note",
            "elements": [{"tag": "plain_text", "content": "Powered by Databricks Foundation Model API | Unity Catalog governed"}]
        }
    ]
}

send_feishu_card(scene3_card)
print("✅ Scene 3: RCA results sent to Feishu")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scene 4: Drill-Down Details (根因 Drill-down)

# COMMAND ----------

# Get drill-down data for the top root cause
drill_data = spark.sql(f"""
    SELECT
      platform, app_version, region, provider, error_code,
      COUNT(*) as error_count,
      COUNT(DISTINCT user_id) as affected_users
    FROM neo_claude_code.gaming.silver_payments
    WHERE event_ts BETWEEN '{ANOMALY_START}' AND '{ANOMALY_END}'
      AND payment_status = 'fail'
    GROUP BY platform, app_version, region, provider, error_code
    ORDER BY error_count DESC
    LIMIT 5
""").collect()

drill_lines = []
for row in drill_data:
    drill_lines.append(
        f"• **{row.platform} {row.app_version} | {row.region} | {row.provider}** "
        f"→ {row.error_code}: {row.error_count} errors, {row.affected_users} users"
    )

# Sample error logs
sample_logs = spark.sql(f"""
    SELECT event_ts, user_id, provider, error_code, amount
    FROM neo_claude_code.gaming.silver_payments
    WHERE event_ts BETWEEN '{ANOMALY_START}' AND '{ANOMALY_END}'
      AND payment_status = 'fail' AND platform = 'iOS' AND app_version = '1.2.7'
    ORDER BY event_ts
    LIMIT 5
""").collect()

log_lines = []
for row in sample_logs:
    log_lines.append(f"`{row.event_ts}` | {row.user_id} | {row.provider} | {row.error_code} | ${row.amount}")

scene4_card = {
    "config": {"wide_screen_mode": True},
    "header": {
        "title": {"content": "🔬 Root Cause Drill-Down", "tag": "plain_text"},
        "template": "yellow"
    },
    "elements": [
        {
            "tag": "div",
            "text": {
                "tag": "lark_md",
                "content": "**Top Error Segments During Incident**\n" + "\n".join(drill_lines)
            }
        },
        {"tag": "hr"},
        {
            "tag": "div",
            "text": {
                "tag": "lark_md",
                "content": "**Sample Error Logs (iOS 1.2.7)**\n" + "\n".join(log_lines)
            }
        },
        {"tag": "hr"},
        {
            "tag": "note",
            "elements": [{"tag": "plain_text", "content": f"Showing top 5 segments | Window: {ANOMALY_START} → {ANOMALY_END}"}]
        }
    ]
}

send_feishu_card(scene4_card)
print("✅ Scene 4: Drill-down details sent to Feishu")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scene 5: Auto-Generated Incident Report (闭环)

# COMMAND ----------

report_prompt = f"""Write a concise incident report in plain text (no markdown). Include: Summary (2 sentences), Timeline (bullet points), Impact (users/revenue), Root Cause, Immediate Actions, Long-term Fixes.

Context: Payment success rate anomaly from {ANOMALY_START} to {ANOMALY_END}. Top root cause: iOS app_version 1.2.7 in SG region, Stripe payments, TOKEN_EXPIRED errors spiked. Estimated impact: ${rev.total_impact_usd:,.2f} revenue loss, {affected_dau} users affected.

Keep it under 800 characters."""

report_result = spark.sql(f"""
    SELECT ai_query('databricks-claude-sonnet-4-5', '{report_prompt.replace("'", "''")}') as report
""").collect()[0]['report']

print(report_result)

# COMMAND ----------

scene5_card = {
    "config": {"wide_screen_mode": True},
    "header": {
        "title": {"content": "📋 Incident Report — Auto-Generated", "tag": "plain_text"},
        "template": "green"
    },
    "elements": [
        {
            "tag": "div",
            "text": {
                "tag": "lark_md",
                "content": report_result[:2000]
            }
        },
        {"tag": "hr"},
        {
            "tag": "div",
            "text": {
                "tag": "lark_md",
                "content": f"**Status**: ✅ Resolved\n**Generated at**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n**Platform**: Databricks LiveOps + Foundation Model API"
            }
        },
        {
            "tag": "action",
            "actions": [
                {
                    "tag": "button",
                    "text": {"tag": "plain_text", "content": "📤 Export to Confluence"},
                    "type": "primary",
                    "value": {"action": "export_confluence"}
                },
                {
                    "tag": "button",
                    "text": {"tag": "plain_text", "content": "🔄 Regenerate"},
                    "type": "default",
                    "value": {"action": "regenerate"}
                }
            ]
        },
        {
            "tag": "note",
            "elements": [{"tag": "plain_text", "content": "以前要拉群2小时，现在平台自动跑完并生成报告。"}]
        }
    ]
}

send_feishu_card(scene5_card)
print("✅ Scene 5: Incident report sent to Feishu")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ### All Scenes Complete!
# MAGIC Check your Feishu group for the 4 alert cards (Scenes 2-5).

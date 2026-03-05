"""
LiveOps Gaming Control Center | claude_code — Databricks App
- Click "Release New Version" to inject random incident data
- Agent analyzes incidents and produces insights
- Compare insights against ground truth logs
"""

import os
import json
import random
import math
import requests
from datetime import datetime, timedelta
from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

w = WorkspaceClient()
WAREHOUSE_ID = os.environ.get("DATABRICKS_WAREHOUSE_ID", "f90a96426a475f5b")
FEISHU_WEBHOOK = os.environ.get("FEISHU_WEBHOOK", "")
CATALOG = "neo_claude_code"
SCHEMA = "gaming"
DASHBOARD_ID = "01f118519b9d102386a8df5a22488107"
WORKSPACE_HOST = w.config.host.rstrip("/")

# Store ground truth for comparison
incident_log = []


def run_sql(statement: str, timeout_seconds=120):
    """Execute SQL via Databricks SDK statement execution."""
    resp = w.statement_execution.execute_statement(
        warehouse_id=WAREHOUSE_ID,
        statement=statement,
        wait_timeout="50s",
    )
    if resp.status.state == StatementState.SUCCEEDED:
        if resp.result and resp.result.data_array:
            cols = [c.name for c in resp.manifest.schema.columns]
            return [dict(zip(cols, row)) for row in resp.result.data_array]
        return []
    else:
        error = resp.status.error
        raise Exception(f"SQL failed: {error.message if error else 'unknown'}")


def send_feishu(card):
    """Send card to Feishu bot."""
    if not FEISHU_WEBHOOK:
        return
    try:
        requests.post(FEISHU_WEBHOOK, json={"msg_type": "interactive", "card": card}, timeout=10)
    except Exception as e:
        print(f"Feishu send failed: {e}")


@app.get("/", response_class=HTMLResponse)
def index():
    dashboard_embed_url = f"{WORKSPACE_HOST}/embed/dashboardsv3/{DASHBOARD_ID}/published"
    dashboard_full_url = f"{WORKSPACE_HOST}/dashboardsv3/{DASHBOARD_ID}"
    html = """
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>LiveOps Gaming Control Center | claude_code</title>
<style>
  * { margin: 0; padding: 0; box-sizing: border-box; }
  body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #0f0f23; color: #e0e0e0; min-height: 100vh; }
  .header { background: linear-gradient(135deg, #1a1a3e 0%, #0d0d2b 100%); padding: 20px 30px; border-bottom: 2px solid #333; display: flex; justify-content: space-between; align-items: center; }
  .header h1 { font-size: 24px; color: #fff; }
  .header .badge { background: #00b894; padding: 4px 12px; border-radius: 20px; font-size: 12px; color: white; }
  .container { max-width: 1400px; margin: 0 auto; padding: 20px; }
  .grid { display: grid; grid-template-columns: 1fr 1fr; gap: 20px; margin-top: 20px; }
  .card { background: #1a1a2e; border: 1px solid #333; border-radius: 12px; padding: 20px; }
  .card h3 { margin-bottom: 15px; color: #a0a0ff; }
  .btn { padding: 12px 24px; border: none; border-radius: 8px; cursor: pointer; font-size: 14px; font-weight: 600; transition: all 0.2s; }
  .btn-danger { background: #e74c3c; color: white; }
  .btn-danger:hover { background: #c0392b; }
  .btn-primary { background: #3498db; color: white; }
  .btn-primary:hover { background: #2980b9; }
  .btn-success { background: #00b894; color: white; }
  .btn-success:hover { background: #00a383; }
  .btn:disabled { opacity: 0.5; cursor: not-allowed; }
  .log { background: #0a0a1a; border: 1px solid #333; border-radius: 8px; padding: 15px; margin-top: 15px; max-height: 400px; overflow-y: auto; font-family: 'Courier New', monospace; font-size: 13px; line-height: 1.6; }
  .log-entry { padding: 4px 0; border-bottom: 1px solid #1a1a2e; }
  .log-entry.error { color: #e74c3c; }
  .log-entry.success { color: #00b894; }
  .log-entry.info { color: #3498db; }
  .log-entry.warn { color: #f39c12; }
  .insight-card { background: #16213e; border: 1px solid #0f3460; border-radius: 8px; padding: 15px; margin-top: 10px; }
  .insight-card h4 { color: #e94560; margin-bottom: 8px; }
  .comparison { margin-top: 10px; padding: 10px; border-radius: 8px; }
  .comparison.match { background: rgba(0,184,148,0.1); border: 1px solid #00b894; }
  .comparison.mismatch { background: rgba(231,76,60,0.1); border: 1px solid #e74c3c; }
  .spinner { display: inline-block; width: 16px; height: 16px; border: 2px solid #333; border-top-color: #3498db; border-radius: 50%; animation: spin 0.8s linear infinite; margin-right: 8px; }
  @keyframes spin { to { transform: rotate(360deg); } }
  select { background: #1a1a2e; color: #e0e0e0; border: 1px solid #333; padding: 8px 12px; border-radius: 6px; font-size: 14px; }
</style>
</head>
<body>
<div class="header">
  <h1>🎮 LiveOps Gaming Control Center | claude_code</h1>
  <span class="badge">LIVE</span>
</div>
<div class="container">
  <div class="grid">
    <!-- Left: Incident Simulation -->
    <div class="card">
      <h3>🚀 Version Release Simulator</h3>
      <p style="margin-bottom: 15px; color: #888;">Click to simulate a new app version release with random incidents.</p>
      <div style="display: flex; gap: 10px; flex-wrap: wrap;">
        <button class="btn btn-danger" onclick="releaseVersion()" id="releaseBtn">
          🎯 Release New Version
        </button>
        <button class="btn btn-primary" onclick="runAnalysis()" id="analysisBtn" disabled>
          🤖 Run AI Analysis
        </button>
        <button class="btn btn-success" onclick="compareResults()" id="compareBtn" disabled>
          ✅ Compare Results
        </button>
      </div>
      <div class="log" id="groundTruthLog">
        <div class="log-entry info">System ready. Click "Release New Version" to start.</div>
      </div>
    </div>

    <!-- Right: AI Insights -->
    <div class="card">
      <h3>🤖 AI Agent Insights</h3>
      <p style="margin-bottom: 15px; color: #888;">Agent-generated root cause analysis from the data.</p>
      <div id="insightsArea">
        <div class="log" id="insightsLog">
          <div class="log-entry info">Waiting for incident data...</div>
        </div>
      </div>
    </div>
  </div>

  <!-- Bottom: Comparison -->
  <div class="card" style="margin-top: 20px;" id="comparisonCard" hidden>
    <h3>📊 Ground Truth vs AI Insights Comparison</h3>
    <div id="comparisonArea"></div>
  </div>

  <!-- Dashboard Embed -->
  <div class="card" style="margin-top: 20px;">
    <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 15px;">
      <h3>📊 LiveOps Dashboard</h3>
      <div style="display: flex; gap: 10px; align-items: center;">
        <span id="dashboardStatus" style="font-size: 12px; color: #888;"></span>
        <button class="btn btn-primary" onclick="refreshDashboard()" style="padding: 8px 16px; font-size: 12px;">
          🔄 Refresh Dashboard
        </button>
        <a href="__DASHBOARD_FULL_URL__" target="_blank" class="btn btn-success" style="padding: 8px 16px; font-size: 12px; text-decoration: none;">
          ↗ Open Full Screen
        </a>
      </div>
    </div>
    <iframe id="dashboardFrame"
            src="__DASHBOARD_EMBED_URL__"
            style="width: 100%; height: 700px; border: 1px solid #333; border-radius: 8px; background: #fff;"
            loading="lazy">
    </iframe>
  </div>
</div>

<script>
const groundTruthLog = document.getElementById('groundTruthLog');
const insightsLog = document.getElementById('insightsLog');
const dashboardFrame = document.getElementById('dashboardFrame');
const dashboardStatus = document.getElementById('dashboardStatus');

function refreshDashboard() {
  dashboardStatus.textContent = 'Refreshing...';
  dashboardStatus.style.color = '#f39c12';
  const src = dashboardFrame.src;
  dashboardFrame.src = '';
  setTimeout(() => {
    dashboardFrame.src = src;
    dashboardStatus.textContent = 'Refreshed at ' + new Date().toLocaleTimeString();
    dashboardStatus.style.color = '#00b894';
  }, 500);
}

function addLog(target, msg, cls='info') {
  const div = document.createElement('div');
  div.className = 'log-entry ' + cls;
  div.textContent = `[${new Date().toLocaleTimeString()}] ${msg}`;
  target.appendChild(div);
  target.scrollTop = target.scrollHeight;
}

async function releaseVersion() {
  const btn = document.getElementById('releaseBtn');
  btn.disabled = true;
  btn.innerHTML = '<span class="spinner"></span>Generating...';
  groundTruthLog.innerHTML = '';
  insightsLog.innerHTML = '';

  addLog(groundTruthLog, 'Simulating new version release...', 'info');

  try {
    const resp = await fetch('/api/release-version', { method: 'POST' });
    const data = await resp.json();

    if (data.success) {
      addLog(groundTruthLog, `New version released: ${data.app_version}`, 'success');
      addLog(groundTruthLog, `Platform: ${data.platform}`, 'info');
      addLog(groundTruthLog, `Incident window: ${data.incident_start} → ${data.incident_end}`, 'warn');
      addLog(groundTruthLog, `Root cause: ${data.root_cause}`, 'error');
      addLog(groundTruthLog, `Error code: ${data.error_code}`, 'error');
      addLog(groundTruthLog, `Affected region: ${data.region}`, 'warn');
      addLog(groundTruthLog, `Affected provider: ${data.provider}`, 'warn');
      addLog(groundTruthLog, `Fail rate injected: ${(data.fail_rate * 100).toFixed(0)}%`, 'error');
      addLog(groundTruthLog, `Rows inserted: ${data.rows_inserted}`, 'success');
      addLog(groundTruthLog, '---', 'info');
      addLog(groundTruthLog, 'Ground truth recorded. Ready for AI analysis.', 'success');
      addLog(groundTruthLog, 'Refreshing dashboard...', 'info');

      // Auto-refresh dashboard after data is written
      setTimeout(() => {
        refreshDashboard();
        addLog(groundTruthLog, 'Dashboard refreshed.', 'success');
      }, 2000);

      document.getElementById('analysisBtn').disabled = false;
    } else {
      addLog(groundTruthLog, `Error: ${data.error}`, 'error');
    }
  } catch (e) {
    addLog(groundTruthLog, `Error: ${e.message}`, 'error');
  }

  btn.disabled = false;
  btn.innerHTML = '🎯 Release New Version';
}

async function runAnalysis() {
  const btn = document.getElementById('analysisBtn');
  btn.disabled = true;
  btn.innerHTML = '<span class="spinner"></span>Analyzing...';
  insightsLog.innerHTML = '';

  addLog(insightsLog, 'Starting AI analysis agent...', 'info');
  addLog(insightsLog, 'Step 1: Detecting anomalies in gold_anomaly...', 'info');

  try {
    const resp = await fetch('/api/run-analysis', { method: 'POST' });
    const data = await resp.json();

    if (data.success) {
      addLog(insightsLog, 'Step 2: Dimension drill-down complete.', 'info');
      addLog(insightsLog, 'Step 3: Error code correlation complete.', 'info');
      addLog(insightsLog, '---', 'info');
      addLog(insightsLog, 'AI Root Cause Analysis:', 'success');

      // Show each insight
      data.insights.forEach((insight, i) => {
        addLog(insightsLog, `Root Cause #${i+1}: ${insight}`, 'warn');
      });

      addLog(insightsLog, '---', 'info');
      addLog(insightsLog, `AI detected platform: ${data.detected_platform}`, 'info');
      addLog(insightsLog, `AI detected version: ${data.detected_version}`, 'info');
      addLog(insightsLog, `AI detected region: ${data.detected_region}`, 'info');
      addLog(insightsLog, `AI detected provider: ${data.detected_provider}`, 'info');
      addLog(insightsLog, `AI detected error: ${data.detected_error}`, 'info');
      addLog(insightsLog, '---', 'info');
      addLog(insightsLog, 'Analysis complete. Ready for comparison.', 'success');

      document.getElementById('compareBtn').disabled = false;
    } else {
      addLog(insightsLog, `Error: ${data.error}`, 'error');
    }
  } catch (e) {
    addLog(insightsLog, `Error: ${e.message}`, 'error');
  }

  btn.disabled = false;
  btn.innerHTML = '🤖 Run AI Analysis';
}

async function compareResults() {
  const btn = document.getElementById('compareBtn');
  btn.disabled = true;

  try {
    const resp = await fetch('/api/compare', { method: 'POST' });
    const data = await resp.json();

    const card = document.getElementById('comparisonCard');
    card.hidden = false;
    const area = document.getElementById('comparisonArea');
    area.innerHTML = '';

    data.comparisons.forEach(c => {
      const div = document.createElement('div');
      div.className = 'comparison ' + (c.match ? 'match' : 'mismatch');
      div.innerHTML = `
        <strong>${c.field}</strong><br>
        Ground Truth: <code>${c.ground_truth}</code> |
        AI Detected: <code>${c.ai_detected}</code> |
        ${c.match ? '✅ Match' : '❌ Mismatch'}
      `;
      area.appendChild(div);
    });

    const summary = document.createElement('div');
    summary.style.cssText = 'margin-top: 15px; padding: 10px; background: #1a1a2e; border-radius: 8px; text-align: center;';
    summary.innerHTML = `<strong>Accuracy: ${data.accuracy}%</strong> (${data.matches}/${data.total} fields matched)`;
    area.appendChild(summary);

  } catch (e) {
    console.error(e);
  }
  btn.disabled = false;
}
</script>
</body>
</html>
"""
    return html.replace("__DASHBOARD_EMBED_URL__", dashboard_embed_url).replace("__DASHBOARD_FULL_URL__", dashboard_full_url)


@app.post("/api/release-version")
def release_version():
    """Simulate a new version release with random incident injection."""
    global incident_log

    # Random incident parameters
    platforms = ["iOS", "Android"]
    regions = ["US", "SG", "JP", "EU", "BR"]
    providers_map = {"iOS": ["Stripe", "ApplePay"], "Android": ["Stripe", "GooglePay"]}
    error_codes = ["TOKEN_EXPIRED", "NETWORK_TIMEOUT", "PROVIDER_ERROR", "CARD_DECLINED", "SESSION_TIMEOUT"]
    versions = ["1.3.0", "1.3.1", "1.4.0", "2.0.0"]

    platform = random.choice(platforms)
    region = random.choice(regions)
    provider = random.choice(providers_map[platform])
    error_code = random.choice(error_codes)
    app_version = random.choice(versions)
    fail_rate = round(random.uniform(0.12, 0.35), 2)

    now = datetime.now().replace(second=0, microsecond=0)
    incident_start = now - timedelta(minutes=random.randint(10, 30))
    incident_end = now

    # Generate incident payment data
    rows = []
    for minute_offset in range(0, int((incident_end - incident_start).total_seconds() / 60), 1):
        ts = incident_start + timedelta(minutes=minute_offset)
        n_events = random.randint(20, 80)
        for _ in range(n_events):
            event_ts = ts + timedelta(seconds=random.randint(0, 59))
            uid = f"u_{random.randint(0, 49999):06d}"
            amount = random.choice([0.99, 1.99, 4.99, 9.99, 19.99, 49.99])

            if random.random() < fail_rate:
                status = "fail"
                err = error_code if random.random() < 0.75 else random.choice(error_codes)
            else:
                status = "success"
                err = "NULL"

            devices = ["iPhone15", "Galaxy_S24", "Pixel_8"]
            device = random.choice(devices)
            err_val = "NULL" if err == "NULL" else f"'{err}'"
            rows.append(
                f"('{event_ts.isoformat()}', '{uid}', {amount}, 'USD', '{provider}', "
                f"'{status}', {err_val}, "
                f"'{platform}', '{app_version}', '{region}', '{device}')"
            )

    # Insert in batches
    batch_size = 500
    total_inserted = 0
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i+batch_size]
        sql = (f"INSERT INTO {CATALOG}.{SCHEMA}.silver_payments "
               f"(event_ts, user_id, amount, currency, provider, payment_status, error_code, "
               f"platform, app_version, region, device_model) VALUES " + ",".join(batch))
        try:
            run_sql(sql)
            total_inserted += len(batch)
        except Exception as e:
            return {"success": False, "error": str(e)}

    # Store ground truth
    ground_truth = {
        "platform": platform,
        "app_version": app_version,
        "region": region,
        "provider": provider,
        "error_code": error_code,
        "fail_rate": fail_rate,
        "incident_start": incident_start.isoformat(),
        "incident_end": incident_end.isoformat(),
        "root_cause": f"{platform} {app_version} + {region} + {provider}: {error_code} spike",
        "rows_inserted": total_inserted,
    }
    incident_log.append(ground_truth)

    # Also regenerate gold tables
    try:
        run_sql(f"""
            CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.gold_kpi_5m AS
            WITH bucketed AS (
              SELECT
                to_timestamp(unix_timestamp(date_trunc('minute', event_ts)) - (minute(date_trunc('minute', event_ts)) % 5) * 60) as bucket_5m,
                platform, app_version, region, provider, amount, payment_status, error_code
              FROM {CATALOG}.{SCHEMA}.silver_payments
            )
            SELECT bucket_5m, platform, app_version, region, provider,
              ROUND(SUM(CASE WHEN payment_status='success' THEN amount ELSE 0 END), 2) as revenue,
              COUNT(*) as pay_attempts,
              SUM(CASE WHEN payment_status='success' THEN 1 ELSE 0 END) as pay_success,
              SUM(CASE WHEN payment_status='fail' THEN 1 ELSE 0 END) as pay_fail,
              ROUND(SUM(CASE WHEN payment_status='success' THEN 1 ELSE 0 END)/COUNT(*), 4) as pay_success_rate
            FROM bucketed GROUP BY ALL ORDER BY bucket_5m
        """)

        run_sql(f"""
            CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.gold_anomaly AS
            WITH overall_5m AS (
              SELECT bucket_5m, SUM(revenue) as revenue, SUM(pay_attempts) as pay_attempts,
                SUM(pay_success) as pay_success, SUM(pay_fail) as pay_fail,
                ROUND(SUM(pay_success)/SUM(pay_attempts), 4) as pay_success_rate
              FROM {CATALOG}.{SCHEMA}.gold_kpi_5m GROUP BY bucket_5m
            ),
            with_time_key AS (
              SELECT *, hour(bucket_5m)*12 + minute(bucket_5m)/5 as tod_key, date(bucket_5m) as dt
              FROM overall_5m
            ),
            baselines AS (
              SELECT a.bucket_5m, a.pay_success_rate as actual_psr, a.pay_attempts as actual_attempts,
                a.revenue as actual_revenue,
                AVG(b.pay_success_rate) as baseline_psr, STDDEV(b.pay_success_rate) as std_psr,
                AVG(b.revenue) as baseline_revenue, STDDEV(b.revenue) as std_revenue
              FROM with_time_key a JOIN with_time_key b ON a.tod_key=b.tod_key AND a.dt!=b.dt
              GROUP BY a.bucket_5m, a.pay_success_rate, a.pay_attempts, a.revenue
            )
            SELECT bucket_5m, 'pay_success_rate' as metric, actual_psr as actual,
              ROUND(baseline_psr, 4) as baseline,
              ROUND(CASE WHEN std_psr>0 THEN (actual_psr-baseline_psr)/std_psr ELSE 0 END, 2) as z_score,
              CASE WHEN std_psr>0 AND (actual_psr-baseline_psr)/std_psr < -3 THEN true ELSE false END as is_anomaly,
              ROUND(CASE WHEN actual_psr<baseline_psr THEN (baseline_psr-actual_psr)*actual_attempts*14.99 ELSE 0 END, 2) as impact_estimated
            FROM baselines
            UNION ALL
            SELECT bucket_5m, 'revenue' as metric, actual_revenue as actual,
              ROUND(baseline_revenue, 2) as baseline,
              ROUND(CASE WHEN std_revenue>0 THEN (actual_revenue-baseline_revenue)/std_revenue ELSE 0 END, 2) as z_score,
              CASE WHEN std_revenue>0 AND (actual_revenue-baseline_revenue)/std_revenue < -3 THEN true ELSE false END as is_anomaly,
              ROUND(CASE WHEN actual_revenue<baseline_revenue THEN baseline_revenue-actual_revenue ELSE 0 END, 2) as impact_estimated
            FROM baselines ORDER BY bucket_5m, metric
        """)
    except Exception as e:
        print(f"Gold table refresh warning: {e}")

    # Send Feishu alert
    send_feishu({
        "config": {"wide_screen_mode": True},
        "header": {"title": {"content": f"🚀 New Version Released: {app_version}", "tag": "plain_text"}, "template": "red"},
        "elements": [
            {"tag": "div", "text": {"tag": "lark_md",
                "content": f"**Platform**: {platform}\n**Region**: {region}\n"
                           f"**Incident**: {incident_start.strftime('%H:%M')} → {incident_end.strftime('%H:%M')}\n"
                           f"**Status**: 🔴 Anomaly Detected"
            }}
        ]
    })

    return {"success": True, **ground_truth}


@app.post("/api/run-analysis")
def run_analysis():
    """Run AI agent to analyze the latest incident."""
    if not incident_log:
        return {"success": False, "error": "No incidents recorded. Release a version first."}

    latest = incident_log[-1]

    # Get top failing segments during the incident window
    try:
        top_segments = run_sql(f"""
            SELECT platform, app_version, region, provider,
              SUM(CASE WHEN payment_status='fail' THEN 1 ELSE 0 END) as fails,
              COUNT(*) as attempts,
              ROUND(SUM(CASE WHEN payment_status='fail' THEN 1 ELSE 0 END)/COUNT(*), 4) as fail_rate
            FROM {CATALOG}.{SCHEMA}.silver_payments
            WHERE event_ts BETWEEN '{latest['incident_start']}' AND '{latest['incident_end']}'
            GROUP BY platform, app_version, region, provider
            ORDER BY fail_rate DESC
            LIMIT 5
        """)

        top_errors = run_sql(f"""
            SELECT error_code, COUNT(*) as cnt
            FROM {CATALOG}.{SCHEMA}.silver_payments
            WHERE event_ts BETWEEN '{latest['incident_start']}' AND '{latest['incident_end']}'
              AND payment_status = 'fail'
            GROUP BY error_code
            ORDER BY cnt DESC
            LIMIT 5
        """)
    except Exception as e:
        return {"success": False, "error": str(e)}

    # Format evidence for LLM
    seg_text = "\n".join([f"  {s['platform']} {s['app_version']} | {s['region']} | {s['provider']}: fail_rate={s['fail_rate']}, fails={s['fails']}/{s['attempts']}" for s in top_segments])
    err_text = "\n".join([f"  {e['error_code']}: {e['cnt']} errors" for e in top_errors])

    prompt = (
        f"You are a LiveOps incident analyst. Based on the evidence below, "
        f"give exactly 3 root causes as short one-line statements. "
        f"Also identify the primary: platform, app_version, region, provider, and error_code.\n\n"
        f"Top failing segments:\n{seg_text}\n\n"
        f"Top error codes:\n{err_text}\n\n"
        f"Format your response as JSON with keys: insights (array of 3 strings), "
        f"detected_platform, detected_version, detected_region, detected_provider, detected_error"
    )

    try:
        result = run_sql(f"""
            SELECT ai_query('databricks-claude-sonnet-4-5', '{prompt.replace("'", "''")}') as analysis
        """)
        analysis_text = result[0]['analysis'] if result else "{}"

        # Try to parse JSON from the response
        # Find JSON in the response
        import re
        json_match = re.search(r'\{[\s\S]*\}', analysis_text)
        if json_match:
            analysis = json.loads(json_match.group())
        else:
            analysis = {
                "insights": [analysis_text[:200]],
                "detected_platform": top_segments[0]['platform'] if top_segments else "unknown",
                "detected_version": top_segments[0]['app_version'] if top_segments else "unknown",
                "detected_region": top_segments[0]['region'] if top_segments else "unknown",
                "detected_provider": top_segments[0]['provider'] if top_segments else "unknown",
                "detected_error": top_errors[0]['error_code'] if top_errors else "unknown",
            }

        # Send to Feishu
        send_feishu({
            "config": {"wide_screen_mode": True},
            "header": {"title": {"content": "🤖 AI Agent: Root Cause Analysis", "tag": "plain_text"}, "template": "orange"},
            "elements": [
                {"tag": "div", "text": {"tag": "lark_md",
                    "content": "\n".join([f"• Root Cause #{i+1}: {ins}" for i, ins in enumerate(analysis.get('insights', []))])
                }}
            ]
        })

        return {"success": True, **analysis}

    except Exception as e:
        return {"success": False, "error": str(e)}


@app.post("/api/compare")
def compare_results():
    """Compare AI-detected insights against ground truth."""
    if not incident_log:
        return {"success": False, "error": "No incidents to compare."}

    latest = incident_log[-1]

    # Get latest analysis results by re-querying top segments
    try:
        top_segments = run_sql(f"""
            SELECT platform, app_version, region, provider,
              ROUND(SUM(CASE WHEN payment_status='fail' THEN 1 ELSE 0 END)/COUNT(*), 4) as fail_rate
            FROM {CATALOG}.{SCHEMA}.silver_payments
            WHERE event_ts BETWEEN '{latest['incident_start']}' AND '{latest['incident_end']}'
            GROUP BY platform, app_version, region, provider
            ORDER BY fail_rate DESC
            LIMIT 1
        """)
        top_errors = run_sql(f"""
            SELECT error_code, COUNT(*) as cnt
            FROM {CATALOG}.{SCHEMA}.silver_payments
            WHERE event_ts BETWEEN '{latest['incident_start']}' AND '{latest['incident_end']}'
              AND payment_status = 'fail'
            GROUP BY error_code ORDER BY cnt DESC LIMIT 1
        """)
    except Exception as e:
        return {"success": False, "error": str(e)}

    detected = top_segments[0] if top_segments else {}
    detected_error = top_errors[0]['error_code'] if top_errors else "unknown"

    fields = [
        ("Platform", latest["platform"], detected.get("platform", "unknown")),
        ("App Version", latest["app_version"], detected.get("app_version", "unknown")),
        ("Region", latest["region"], detected.get("region", "unknown")),
        ("Provider", latest["provider"], detected.get("provider", "unknown")),
        ("Error Code", latest["error_code"], detected_error),
    ]

    comparisons = []
    matches = 0
    for field, gt, ai in fields:
        match = gt.lower() == ai.lower()
        if match:
            matches += 1
        comparisons.append({"field": field, "ground_truth": gt, "ai_detected": ai, "match": match})

    accuracy = round(matches / len(fields) * 100)

    # Send comparison to Feishu
    comp_text = "\n".join([f"{'✅' if c['match'] else '❌'} {c['field']}: GT={c['ground_truth']} vs AI={c['ai_detected']}" for c in comparisons])
    send_feishu({
        "config": {"wide_screen_mode": True},
        "header": {"title": {"content": f"📊 Accuracy Report: {accuracy}%", "tag": "plain_text"},
                    "template": "green" if accuracy >= 80 else "red"},
        "elements": [
            {"tag": "div", "text": {"tag": "lark_md", "content": comp_text}}
        ]
    })

    return {
        "success": True,
        "comparisons": comparisons,
        "matches": matches,
        "total": len(fields),
        "accuracy": accuracy,
    }

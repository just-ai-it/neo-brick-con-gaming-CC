# LiveOps Gaming Demo | Databricks + Claude Code

> **"Version release causes revenue drop -> auto-locate root cause in 5 minutes + actionable recommendations + incident report"**

A complete LiveOps monitoring demo for gaming companies, built entirely with **Claude Code** on **Databricks**. The demo showcases how Databricks' unified platform (Unity Catalog + Foundation Model API + Lakeview + Apps) can automate incident detection, root cause analysis, and reporting -- reducing a 2-hour war room to a 5-minute automated pipeline.

## Architecture

```
silver_game_events  ──┐
                      ├──> gold_kpi_5m ──> gold_anomaly ──> Incident Copilot (GenAI)
silver_payments     ──┘                                           │
                                                                  ├── Feishu/Lark Bot Alerts
                                                                  ├── Lakeview Dashboard
                                                                  └── Databricks App (Simulator)
```

## Demo Scenes (12-15 min)

| Scene | Title | Description |
|-------|-------|-------------|
| 1 | Dashboard Overview | Lakeview dashboard showing real-time KPIs: revenue trend, payment success rate, error codes, fail rate by platform |
| 2 | Anomaly Detection | Z-score based anomaly detection on 5-min KPI buckets, auto-alert to Feishu |
| 3 | GenAI RCA Agent | Foundation Model API (`ai_query`) analyzes evidence, produces top 3 root causes with specific numbers |
| 4 | Root Cause Drill-Down | Dimension drill-down: platform + version + region + provider + error code |
| 5 | Incident Report | Auto-generated postmortem with timeline, impact, actions, and lessons learned |

## Databricks App

The **LiveOps Gaming Control Center** is a Databricks App that lets you simulate version releases, inject random incidents, run AI-powered analysis, and compare results against ground truth.

![App Full View](images/app_full_view.png)

| Version Release Simulator | AI Agent Insights |
|:---:|:---:|
| ![Simulator](images/app_version_simulator.png) | ![AI Insights](images/app_ai_insights.png) |

**Flow:** Release New Version -> Run AI Analysis -> Compare Results (Ground Truth vs AI)

## Feishu/Lark Bot Integration

Automated alerts sent to Feishu group chat at each stage of the incident lifecycle:

| Alert | RCA Analysis | Accuracy Report |
|:---:|:---:|:---:|
| ![Alert](images/feishu_alert_version_release.png) | ![RCA](images/feishu_rca_analysis.png) | ![Accuracy](images/feishu_accuracy_report.png) |

## Repository Structure

```
.
├── 01_data_generation.py      # Synthetic data: 50K users, 3 days, injected incident
├── 02_anomaly_detection.py    # KPI aggregation + z-score anomaly detection
├── 03_incident_copilot.py     # GenAI RCA agent + incident report generation
├── feishu_bot_alerts.py       # Feishu interactive card alerts (Scenes 2-5)
├── liveops-app/               # Databricks App
│   ├── app.py                 # FastAPI backend + HTML frontend
│   ├── app.yaml               # App config (warehouse, webhook)
│   └── requirements.txt       # Dependencies
└── images/                    # Screenshots
```

## Key Tables (Unity Catalog)

| Table | Description |
|-------|-------------|
| `neo_claude_code.gaming.silver_game_events` | Raw game events (login, level_start, purchase_attempt, etc.) |
| `neo_claude_code.gaming.silver_payments` | Payment transactions with status, error codes, provider |
| `neo_claude_code.gaming.gold_kpi_5m` | 5-minute aggregated KPIs by platform/version/region/provider |
| `neo_claude_code.gaming.gold_anomaly` | Anomaly detection results with z-scores and impact estimates |

## Tech Stack

- **Databricks Unity Catalog** -- governed data layer
- **Databricks Foundation Model API** -- `ai_query('databricks-claude-sonnet-4-5', ...)` for RCA and report generation
- **Databricks Lakeview** -- real-time monitoring dashboard
- **Databricks Apps** -- interactive incident simulation and AI analysis
- **Feishu/Lark Webhook** -- automated alert cards to team chat
- **Built with Claude Code** -- entire demo generated via AI pair programming

## Getting Started

1. **Create catalog and schema:**
   ```sql
   CREATE CATALOG IF NOT EXISTS neo_claude_code;
   CREATE SCHEMA IF NOT EXISTS neo_claude_code.gaming;
   ```

2. **Run notebooks in order:**
   - `01_data_generation.py` -- generates synthetic data with injected incident
   - `02_anomaly_detection.py` -- builds KPI tables and detects anomalies
   - `03_incident_copilot.py` -- runs GenAI RCA and generates incident report

3. **Deploy the app:**
   ```bash
   databricks apps create liveops-gaming-claude-code --profile <your-profile>
   databricks apps deploy liveops-gaming-claude-code --source-code-path liveops-app/ --profile <your-profile>
   ```

4. **Run Feishu alerts (optional):**
   - Update the webhook URL in `feishu_bot_alerts.py`
   - Run the notebook after anomaly detection completes

## License

See [LICENSE](LICENSE) for details.

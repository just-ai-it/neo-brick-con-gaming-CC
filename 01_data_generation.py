# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - LiveOps Data Generation
# MAGIC Generate 3 days of synthetic gaming events & payment data with an injected incident at 10:05 today.

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG neo_claude_code;
# MAGIC USE SCHEMA gaming;

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime, timedelta
import random
import math

# Config
CATALOG = "neo_claude_code"
SCHEMA = "gaming"
NUM_USERS = 50000
DAYS_BACK = 3

# Dimensions
REGIONS = ["US", "SG", "JP", "EU", "BR"]
PLATFORMS = ["iOS", "Android"]
APP_VERSIONS = {"iOS": ["1.2.6", "1.2.7"], "Android": ["1.2.6", "1.2.7"]}
DEVICE_MODELS = {
    "iOS": ["iPhone15", "iPhone14", "iPhone13"],
    "Android": ["Galaxy_S24", "Pixel_8", "OnePlus_12"]
}
ERROR_CODES = ["TOKEN_EXPIRED", "NETWORK_TIMEOUT", "PROVIDER_ERROR", "INSUFFICIENT_FUNDS", "CARD_DECLINED"]

now = datetime.now().replace(second=0, microsecond=0)
start_ts = (now - timedelta(days=DAYS_BACK)).replace(hour=0, minute=0)

# Incident window
INCIDENT_DATE = now.replace(hour=10, minute=5, second=0)
INCIDENT_END = now.replace(hour=10, minute=40, second=0)

print(f"Generating data from {start_ts} to {now}")
print(f"Incident window: {INCIDENT_DATE} to {INCIDENT_END}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate User Base

# COMMAND ----------

user_rows = []
for i in range(NUM_USERS):
    uid = f"u_{i:06d}"
    platform = random.choice(PLATFORMS)
    region = random.choices(REGIONS, weights=[40, 15, 15, 20, 10])[0]
    version = random.choice(APP_VERSIONS[platform])
    device = random.choice(DEVICE_MODELS[platform])
    user_rows.append((uid, platform, region, version, device))

print(f"Users generated: {len(user_rows)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Game Events + Payments (5-min granularity)

# COMMAND ----------

def is_in_incident_window(ts):
    return INCIDENT_DATE <= ts <= INCIDENT_END

def base_fail_rate(platform, version, region, provider, ts):
    """Normal fail rate ~2%, incident bumps iOS+1.2.7 broadly so revenue visibly dips"""
    if is_in_incident_window(ts) and platform == "iOS" and version == "1.2.7" and region == "SG" and provider == "Stripe":
        return 0.55
    if is_in_incident_window(ts) and platform == "iOS" and version == "1.2.7" and region == "SG":
        return 0.35
    if is_in_incident_window(ts) and platform == "iOS" and version == "1.2.7":
        return 0.20
    if is_in_incident_window(ts) and platform == "iOS":
        return 0.06
    return 0.02

def pick_error_code(platform, version, region, provider, ts):
    if is_in_incident_window(ts) and platform == "iOS" and version == "1.2.7" and provider == "Stripe":
        return random.choices(ERROR_CODES, weights=[70, 15, 10, 3, 2])[0]
    if is_in_incident_window(ts) and platform == "iOS" and version == "1.2.7":
        return random.choices(ERROR_CODES, weights=[40, 30, 15, 10, 5])[0]
    return random.choices(ERROR_CODES, weights=[10, 20, 25, 25, 20])[0]

def activity_multiplier(hour):
    return 0.3 + 0.7 * (math.exp(-((hour - 12)**2) / 18) + math.exp(-((hour - 20)**2) / 18))

game_events = []
payment_events = []

total_minutes = DAYS_BACK * 24 * 60 + int((now - now.replace(hour=0, minute=0)).total_seconds() / 60)
print(f"Generating {total_minutes} minutes of data...")

for minute_offset in range(0, total_minutes, 5):
    ts = start_ts + timedelta(minutes=minute_offset)
    hour = ts.hour
    mult = activity_multiplier(hour)

    n_active = int(NUM_USERS * 0.02 * mult)
    active_users = random.sample(user_rows, min(n_active, len(user_rows)))

    for uid, platform, region, version, device in active_users:
        event_ts = ts + timedelta(seconds=random.randint(0, 299))

        game_events.append((event_ts.isoformat(), uid, region, platform, device, version, "login"))

        if random.random() < 0.15:
            purchase_ts = event_ts + timedelta(seconds=random.randint(10, 120))
            game_events.append((purchase_ts.isoformat(), uid, region, platform, device, version, "purchase_attempt"))

            provider = random.choice(["Stripe", "ApplePay"] if platform == "iOS" else ["Stripe", "GooglePay"])
            amount = round(random.choice([0.99, 1.99, 4.99, 9.99, 19.99, 49.99]), 2)

            fail_rate = base_fail_rate(platform, version, region, provider, ts)

            if random.random() < fail_rate:
                error = pick_error_code(platform, version, region, provider, ts)
                # Include dimension columns in payments for drill-down
                payment_events.append((purchase_ts.isoformat(), uid, amount, "USD", provider, "fail", error, platform, version, region, device))
                game_events.append((purchase_ts.isoformat(), uid, region, platform, device, version, "purchase_fail"))
            else:
                payment_events.append((purchase_ts.isoformat(), uid, amount, "USD", provider, "success", None, platform, version, region, device))
                game_events.append((purchase_ts.isoformat(), uid, region, platform, device, version, "purchase_success"))

print(f"Game events: {len(game_events)}, Payment events: {len(payment_events)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Delta Tables

# COMMAND ----------

# silver_game_events
ge_schema = StructType([
    StructField("event_ts", StringType()),
    StructField("user_id", StringType()),
    StructField("region", StringType()),
    StructField("platform", StringType()),
    StructField("device_model", StringType()),
    StructField("app_version", StringType()),
    StructField("event_name", StringType()),
])

df_events = (spark.createDataFrame(game_events, schema=ge_schema)
    .withColumn("event_ts", F.to_timestamp("event_ts"))
)

df_events.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.silver_game_events")
print(f"silver_game_events: {spark.table(f'{CATALOG}.{SCHEMA}.silver_game_events').count()} rows")

# COMMAND ----------

# silver_payments (includes dimension columns for drill-down)
pay_schema = StructType([
    StructField("event_ts", StringType()),
    StructField("user_id", StringType()),
    StructField("amount", DoubleType()),
    StructField("currency", StringType()),
    StructField("provider", StringType()),
    StructField("payment_status", StringType()),
    StructField("error_code", StringType()),
    StructField("platform", StringType()),
    StructField("app_version", StringType()),
    StructField("region", StringType()),
    StructField("device_model", StringType()),
])

df_payments = (spark.createDataFrame(payment_events, schema=pay_schema)
    .withColumn("event_ts", F.to_timestamp("event_ts"))
)

df_payments.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.silver_payments")
print(f"silver_payments: {spark.table(f'{CATALOG}.{SCHEMA}.silver_payments').count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quick Validation

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check incident window: iOS 1.2.7 SG should show high fail rate
# MAGIC SELECT
# MAGIC   platform, app_version, region, provider, payment_status,
# MAGIC   COUNT(*) as cnt
# MAGIC FROM neo_claude_code.gaming.silver_payments
# MAGIC WHERE event_ts >= current_date()
# MAGIC   AND hour(event_ts) BETWEEN 9 AND 11
# MAGIC   AND platform = 'iOS' AND app_version = '1.2.7' AND region = 'SG'
# MAGIC GROUP BY ALL
# MAGIC ORDER BY provider, payment_status

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Overall payment success rate by hour today
# MAGIC SELECT
# MAGIC   hour(event_ts) as hr,
# MAGIC   COUNT(*) as total,
# MAGIC   SUM(CASE WHEN payment_status='success' THEN 1 ELSE 0 END) as success,
# MAGIC   ROUND(SUM(CASE WHEN payment_status='success' THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) as success_rate_pct
# MAGIC FROM neo_claude_code.gaming.silver_payments
# MAGIC WHERE event_ts >= current_date()
# MAGIC GROUP BY 1
# MAGIC ORDER BY 1

# COMMAND ----------

print("Data generation complete!")
print(f"  Catalog: {CATALOG}")
print(f"  Schema: {SCHEMA}")
print(f"  Tables: silver_game_events, silver_payments")
print(f"  Incident: iOS 1.2.7 + SG + Stripe, 10:05-10:40 today, TOKEN_EXPIRED spike")

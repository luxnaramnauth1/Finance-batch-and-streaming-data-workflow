# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Reporting and analytics
# MAGIC Creates SQL views on top of the gold tables. Point a Databricks **AI/BI dashboard**, a SQL query, or a BI tool at these views.
# MAGIC
# MAGIC Run notebooks 01 to 03 first (02 and 03 are optional: views that depend on missing tables are skipped).

# COMMAND ----------

# MAGIC %run ./00_common

# COMMAND ----------

def create_view(name: str, query: str):
    try:
        spark.sql(f"CREATE OR REPLACE VIEW {t(name)} AS {query}")
        print(f"created view {name}")
    except Exception as e:
        print(f"skipped {name}: {str(e).splitlines()[0][:140]}")


create_view("vw_daily_kpis", f"""
    SELECT txn_date, txn_count, total_usd, avg_usd, fraud_count, fraud_rate
    FROM {t('gold_daily_kpis')}
""")

create_view("vw_spend_by_category", f"""
    SELECT merchant_category,
           SUM(txn_count) AS txn_count,
           ROUND(SUM(total_usd), 2) AS total_usd
    FROM {t('gold_category_spend')}
    GROUP BY merchant_category
""")

create_view("vw_channel_risk", f"""
    SELECT channel,
           COUNT(*) AS txn_count,
           SUM(is_fraud) AS fraud_count,
           ROUND(SUM(is_fraud) / COUNT(*), 4) AS fraud_rate
    FROM {t('silver_transactions')}
    GROUP BY channel
""")

create_view("vw_top_accounts", f"""
    SELECT account_id, txn_count, total_usd, max_usd, countries_used, last_txn_ts
    FROM {t('gold_account_summary')}
    ORDER BY total_usd DESC
    LIMIT 100
""")

create_view("vw_flagged_transactions", f"""
    SELECT s.transaction_id, s.txn_ts, s.account_id, s.merchant_category, s.channel, s.country,
           s.amount_usd, f.fraud_probability, f.actual_fraud
    FROM {t('gold_fraud_scores')} f
    JOIN {t('silver_transactions')} s USING (transaction_id)
    WHERE f.flagged AND f.split = 'test'
""")

create_view("vw_live_window_kpis", f"""
    SELECT window_start, window_end, merchant_category, txn_count, total_usd, fraud_count
    FROM {t('gold_stream_window_kpis')}
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Daily KPIs

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {t('vw_daily_kpis')} ORDER BY txn_date"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Spend by category

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {t('vw_spend_by_category')} ORDER BY total_usd DESC"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fraud rate by channel

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {t('vw_channel_risk')} ORDER BY fraud_rate DESC"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Highest-risk flagged transactions (held-out period)

# COMMAND ----------

try:
    display(spark.sql(f"SELECT * FROM {t('vw_flagged_transactions')} ORDER BY fraud_probability DESC LIMIT 25"))
except Exception as e:
    print("Run notebook 03 first:", str(e).splitlines()[0][:120])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Live 5-minute windows from the streaming pipeline

# COMMAND ----------

try:
    display(spark.sql(f"SELECT * FROM {t('vw_live_window_kpis')} ORDER BY window_start DESC LIMIT 50"))
except Exception as e:
    print("Run notebook 02 first:", str(e).splitlines()[0][:120])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Build a dashboard
# MAGIC In the sidebar choose **Dashboards → Create dashboard**, add a dataset from one of the `vw_*` views,
# MAGIC then add a line chart on `vw_daily_kpis` (x = `txn_date`, y = `total_usd`), a bar chart on `vw_spend_by_category`,
# MAGIC and a table on `vw_flagged_transactions`.

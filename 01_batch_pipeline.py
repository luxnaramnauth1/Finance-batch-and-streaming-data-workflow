# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Batch pipeline (medallion architecture)
# MAGIC **CSV → bronze → silver → gold**, all as Delta tables.
# MAGIC
# MAGIC | Layer | Table | Purpose |
# MAGIC |---|---|---|
# MAGIC | Bronze | `bronze_transactions` | Raw data exactly as received, plus lineage columns |
# MAGIC | Silver | `silver_transactions`, `silver_quarantine` | Typed, validated, converted to USD, deduplicated. Bad rows go to quarantine with a reason |
# MAGIC | Gold | `gold_daily_kpis`, `gold_category_spend`, `gold_account_summary` | Business-ready tables for reporting and analytics |
# MAGIC
# MAGIC Re-running is safe: silver uses `MERGE`, so it never creates duplicates.

# COMMAND ----------

# MAGIC %run ./00_common

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Source data
# MAGIC Uses your own CSV if you set the `own_csv_path` widget, otherwise generates 30 days of synthetic transactions.

# COMMAND ----------

OWN = dbutils.widgets.get("own_csv_path").strip()

if OWN:
    source_path = OWN
else:
    have_csv = any(f.name.endswith(".csv") for f in dbutils.fs.ls(BATCH_DIR))
    if not have_csv:
        end = datetime.now().replace(microsecond=0)
        pdf = make_transactions(60_000, end - timedelta(days=30), end, seed=7, id_prefix="TXN", dirty=True)
        pdf.to_csv(f"{BATCH_DIR}/transactions_history.csv", index=False)
        print(f"Generated {len(pdf):,} synthetic rows")
    source_path = BATCH_DIR

print("Reading from:", source_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Bronze: land the raw data
# MAGIC Overwritten on each batch run for the demo. In production use append, or the Auto Loader in notebook 02.

# COMMAND ----------

bronze = read_csv_standardised(source_path).withColumn("_ingested_at", F.current_timestamp())

(bronze.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(t("bronze_transactions")))

bronze_count = spark.table(t("bronze_transactions")).count()
print(f"Bronze rows: {bronze_count:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Silver: clean, validate, convert to USD, upsert

# COMMAND ----------

good, rejected = clean_transactions(spark.table(t("bronze_transactions")))
good = good.dropDuplicates(["transaction_id"])

upsert_delta(good, t("silver_transactions"), "transaction_id")

(rejected.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(t("silver_quarantine")))

silver_count = spark.table(t("silver_transactions")).count()
rejected_count = spark.table(t("silver_quarantine")).count()
print(f"Silver rows: {silver_count:,} | quarantined: {rejected_count:,} "
      f"({rejected_count / max(bronze_count, 1):.2%} of bronze) | duplicates removed: "
      f"{bronze_count - rejected_count - silver_count:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data-quality report: why rows were rejected

# COMMAND ----------

display(spark.table(t("silver_quarantine")).groupBy("reject_reason").count().orderBy(F.desc("count")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Gold: business-ready tables

# COMMAND ----------

silver = spark.table(t("silver_transactions"))

daily_kpis = (
    silver.groupBy("txn_date")
    .agg(
        F.count("*").alias("txn_count"),
        F.round(F.sum("amount_usd"), 2).alias("total_usd"),
        F.round(F.avg("amount_usd"), 2).alias("avg_usd"),
        F.sum("is_fraud").alias("fraud_count"),
    )
    .withColumn("fraud_rate", F.round(F.col("fraud_count") / F.col("txn_count"), 4))
)

category_spend = (
    silver.groupBy("txn_date", "merchant_category", "channel")
    .agg(F.count("*").alias("txn_count"), F.round(F.sum("amount_usd"), 2).alias("total_usd"))
)

account_summary = (
    silver.groupBy("account_id")
    .agg(
        F.count("*").alias("txn_count"),
        F.round(F.sum("amount_usd"), 2).alias("total_usd"),
        F.round(F.avg("amount_usd"), 2).alias("avg_usd"),
        F.round(F.max("amount_usd"), 2).alias("max_usd"),
        F.countDistinct("country").alias("countries_used"),
        F.max("txn_ts").alias("last_txn_ts"),
    )
)

for name, frame in [("gold_daily_kpis", daily_kpis),
                    ("gold_category_spend", category_spend),
                    ("gold_account_summary", account_summary)]:
    frame.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(t(name))
    print(f"{name}: {spark.table(t(name)).count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Maintenance and a first look at the results

# COMMAND ----------

try:
    spark.sql(f"OPTIMIZE {t('silver_transactions')}")
except Exception as e:  # not available on every compute type
    print("OPTIMIZE skipped:", str(e)[:120])

display(spark.table(t("gold_daily_kpis")).orderBy("txn_date"))

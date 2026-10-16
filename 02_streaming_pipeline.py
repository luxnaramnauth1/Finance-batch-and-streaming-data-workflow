# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Streaming pipeline (Auto Loader + Structured Streaming)
# MAGIC New CSV files landing in a folder are picked up incrementally:
# MAGIC
# MAGIC **landing folder → bronze (Auto Loader) → silver (validate + dedupe) → gold (5-minute window KPIs)**
# MAGIC
# MAGIC - Checkpoints guarantee each file is processed once, even after restarts.
# MAGIC - `trigger(availableNow=True)` processes everything that has arrived and then stops. It works on serverless
# MAGIC   compute and is how many production jobs run streaming on a schedule.
# MAGIC - For an always-on stream on a classic cluster, replace it with `trigger(processingTime="10 seconds")`.

# COMMAND ----------

# MAGIC %run ./00_common

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stream definitions

# COMMAND ----------

def run_bronze_stream():
    """Auto Loader: incrementally ingest new CSV files into a bronze Delta table."""
    stream = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .schema(TXN_SCHEMA)
        .load(STREAM_DIR)
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_ingested_at", F.current_timestamp())
    )
    q = (
        stream.writeStream
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/bronze_stream")
        .trigger(availableNow=True)
        .toTable(t("bronze_stream"))
    )
    q.awaitTermination()


def process_silver_batch(batch_df, batch_id):
    """foreachBatch: validate, deduplicate, and insert-only MERGE into silver; rejects go to quarantine."""
    good, rejected = clean_transactions(batch_df)
    good = good.dropDuplicates(["transaction_id"])
    upsert_delta(good, t("silver_stream"), "transaction_id", update=False)  # insert-only keeps silver streamable
    (rejected.withColumn("batch_id", F.lit(batch_id))
        .write.format("delta").mode("append").option("mergeSchema", "true")
        .saveAsTable(t("silver_stream_quarantine")))


def run_silver_stream():
    q = (
        spark.readStream.table(t("bronze_stream"))
        .writeStream.foreachBatch(process_silver_batch)
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/silver_stream")
        .trigger(availableNow=True)
        .start()
    )
    q.awaitTermination()


def upsert_window_kpis(batch_df, batch_id):
    """foreachBatch: replace the aggregate for each updated window (idempotent)."""
    (DeltaTable.forName(spark, t("gold_stream_window_kpis")).alias("t")
        .merge(batch_df.alias("s"), "t.window_start = s.window_start AND t.merchant_category = s.merchant_category")
        .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())


def run_gold_stream():
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {t('gold_stream_window_kpis')} (
            window_start TIMESTAMP, window_end TIMESTAMP, merchant_category STRING,
            txn_count BIGINT, total_usd DOUBLE, fraud_count BIGINT)
    """)
    windows = (
        spark.readStream.option("skipChangeCommits", "true").table(t("silver_stream"))
        .withWatermark("txn_ts", "10 minutes")
        .groupBy(F.window("txn_ts", "5 minutes"), "merchant_category")
        .agg(
            F.count("*").alias("txn_count"),
            F.round(F.sum("amount_usd"), 2).alias("total_usd"),
            F.sum("is_fraud").alias("fraud_count"),
        )
        .select(F.col("window.start").alias("window_start"), F.col("window.end").alias("window_end"),
                "merchant_category", "txn_count", "total_usd", "fraud_count")
    )
    q = (
        windows.writeStream.outputMode("update")
        .foreachBatch(upsert_window_kpis)
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/gold_stream")
        .trigger(availableNow=True)
        .start()
    )
    q.awaitTermination()


def run_all_streams():
    run_bronze_stream()
    run_silver_stream()
    run_gold_stream()


def counts():
    out = {}
    for name in ["bronze_stream", "silver_stream", "silver_stream_quarantine", "gold_stream_window_kpis"]:
        out[name] = spark.table(t(name)).count() if spark.catalog.tableExists(t(name)) else 0
    return out


# COMMAND ----------

# MAGIC %md
# MAGIC ## Run 1: first files arrive

# COMMAND ----------

drop_stream_files(n_files=3, rows_per_file=500, seed=100)
run_all_streams()
first = counts()
print("After run 1:", first)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run 2: more files arrive
# MAGIC Only the new files are processed. The checkpoint remembers what was already ingested.

# COMMAND ----------

drop_stream_files(n_files=2, rows_per_file=500, seed=200)
run_all_streams()
second = counts()
print("After run 2:", second)
print("New bronze rows from run 2 only:", second["bronze_stream"] - first["bronze_stream"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Results

# COMMAND ----------

display(spark.table(t("gold_stream_window_kpis")).orderBy(F.desc("window_start"), "merchant_category"))

# COMMAND ----------

display(spark.table(t("silver_stream_quarantine")).groupBy("reject_reason").count())

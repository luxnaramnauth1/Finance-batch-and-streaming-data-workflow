# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Common setup
# MAGIC Shared configuration, schema, cleaning logic and a synthetic finance-data generator.
# MAGIC The other notebooks load this one with `%run ./00_common`, so keep all notebooks in the same folder.
# MAGIC
# MAGIC You can run this notebook on its own to create the schema and volume.

# COMMAND ----------

import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from pyspark.sql import functions as F, types as T, Window

try:
    from delta.tables import DeltaTable
except ImportError:  # only happens when testing outside Databricks
    DeltaTable = None

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters
# MAGIC Change the catalog if yours is not `workspace` (older workspaces often use `main`).
# MAGIC To use your own finance CSV, paste its path into `own_csv_path` (see `HOW_TO_RUN.md`).

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("schema", "finance_demo", "Schema")
dbutils.widgets.text("own_csv_path", "", "Optional: path to your own finance CSV")

CATALOG = dbutils.widgets.get("catalog").strip()
SCHEMA = dbutils.widgets.get("schema").strip()

BASE = f"/Volumes/{CATALOG}/{SCHEMA}/raw_data"
BATCH_DIR = f"{BASE}/batch"
STREAM_DIR = f"{BASE}/stream_landing"
CHECKPOINT_DIR = f"{BASE}/_checkpoints"

HOME_COUNTRY = "MU"  # used for the "foreign transaction" feature


def t(name: str) -> str:
    """Fully qualified table name."""
    return f"{CATALOG}.{SCHEMA}.{name}"


# If your CSV uses different column names, map them here: {"your column": "standard column"}
# Standard columns: transaction_id, account_id, transaction_ts, amount, currency,
#                   merchant_category, merchant_name, channel, country, is_fraud
COLUMN_MAP = {
    # "Date": "transaction_ts",
    # "Amount": "amount",
    # "Account": "account_id",
    # "Category": "merchant_category",
}
COLUMN_DEFAULTS = {  # used when your CSV has no such column
    "account_id": "UNKNOWN",
    "currency": "USD",
    "merchant_category": "Other",
    "merchant_name": "Unknown",
    "channel": "UNKNOWN",
    "country": "UNKNOWN",
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create schema and volume

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.raw_data")
for _p in (BATCH_DIR, STREAM_DIR, CHECKPOINT_DIR):
    dbutils.fs.mkdirs(_p)
print(f"Using {CATALOG}.{SCHEMA}  |  files in {BASE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema and reference data
# MAGIC Raw CSVs are read as strings first, so one bad value is quarantined instead of crashing the job.

# COMMAND ----------

TXN_SCHEMA = T.StructType([
    T.StructField("transaction_id", T.StringType()),
    T.StructField("account_id", T.StringType()),
    T.StructField("transaction_ts", T.StringType()),
    T.StructField("amount", T.StringType()),
    T.StructField("currency", T.StringType()),
    T.StructField("merchant_category", T.StringType()),
    T.StructField("merchant_name", T.StringType()),
    T.StructField("channel", T.StringType()),
    T.StructField("country", T.StringType()),
    T.StructField("is_fraud", T.StringType()),
])

# Static demo exchange rates to USD. In production this would be a maintained reference table.
FX_RATES = {"USD": 1.0, "EUR": 1.08, "GBP": 1.27, "MUR": 0.0217, "ZAR": 0.055}


def fx_df():
    return spark.createDataFrame([(k, float(v)) for k, v in FX_RATES.items()], "currency string, usd_rate double")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Reading a CSV into the standard bronze layout

# COMMAND ----------

def read_csv_standardised(path: str):
    """Read any finance CSV (file or folder) and return it in the standard all-string layout."""
    raw = spark.read.option("header", True).csv(path)
    raw = raw.withColumn("_source_file", F.col("_metadata.file_path"))
    for src, dst in COLUMN_MAP.items():
        if src in raw.columns:
            raw = raw.withColumnRenamed(src, dst)
    std_cols = TXN_SCHEMA.fieldNames()
    if "transaction_id" not in raw.columns:  # build a stable id from the row content
        data_cols = [c for c in raw.columns if c != "_source_file"]
        raw = raw.withColumn("transaction_id", F.sha2(F.concat_ws("|", *[F.col(c).cast("string") for c in data_cols]), 256))
    for c in std_cols:
        if c not in raw.columns:
            default = COLUMN_DEFAULTS.get(c)
            raw = raw.withColumn(c, F.lit(default).cast("string"))
    return raw.select(*[F.col(c).cast("string").alias(c) for c in std_cols], "_source_file")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleaning logic (shared by the batch and streaming pipelines)
# MAGIC Returns two DataFrames: valid rows (typed, converted to USD, with features) and rejected rows with a reason.

# COMMAND ----------

def clean_transactions(bronze_df):
    df = (
        bronze_df
        .withColumn("amount_raw", F.col("amount"))
        .withColumn("transaction_id", F.trim("transaction_id"))
        .withColumn("account_id", F.trim("account_id"))
        .withColumn("txn_ts", F.expr("try_to_timestamp(transaction_ts, 'yyyy-MM-dd HH:mm:ss')"))
        .withColumn("amount", F.expr("try_cast(amount as double)"))
        .withColumn("currency", F.upper(F.trim("currency")))
        .withColumn("merchant_category", F.initcap(F.trim("merchant_category")))
        .withColumn("channel", F.upper(F.trim("channel")))
        .withColumn("country", F.upper(F.trim("country")))
        .withColumn("is_fraud", F.coalesce(F.expr("try_cast(is_fraud as int)"), F.lit(0)))
    )
    reason = (
        F.when(F.col("transaction_id").isNull() | (F.col("transaction_id") == ""), "missing_transaction_id")
        .when(F.col("txn_ts").isNull(), "invalid_timestamp")
        .when(F.col("amount").isNull(), "invalid_amount")
        .when(F.col("amount") <= 0, "non_positive_amount")
        .when(F.col("currency").isNull() | ~F.col("currency").isin(*FX_RATES.keys()), "unknown_currency")
    )
    flagged = df.withColumn("reject_reason", reason)

    rejected = (
        flagged.filter(F.col("reject_reason").isNotNull())
        .select("transaction_id", "account_id", "transaction_ts", "amount_raw", "currency",
                "merchant_category", "reject_reason")
        .withColumn("rejected_at", F.current_timestamp())
    )
    good = (
        flagged.filter(F.col("reject_reason").isNull())
        .join(F.broadcast(fx_df()), "currency", "left")
        .withColumn("amount_usd", F.round(F.col("amount") * F.col("usd_rate"), 2))
        .withColumn("txn_date", F.to_date("txn_ts"))
        .withColumn("txn_hour", F.hour("txn_ts"))
        .withColumn("is_weekend", F.dayofweek("txn_ts").isin(1, 7))
        .select("transaction_id", "account_id", "txn_ts", "txn_date", "txn_hour", "is_weekend",
                "amount", "currency", "amount_usd", "merchant_category", "merchant_name",
                "channel", "country", "is_fraud")
    )
    return good, rejected


def upsert_delta(df, table: str, key, update: bool = True):
    """Idempotent write: MERGE on the key so re-running never creates duplicates."""
    keys = [key] if isinstance(key, str) else list(key)
    if not spark.catalog.tableExists(table):
        df.write.format("delta").saveAsTable(table)
        return
    cond = " AND ".join(f"t.{k} = s.{k}" for k in keys)
    merge = DeltaTable.forName(spark, table).alias("t").merge(df.alias("s"), cond)
    if update:
        merge = merge.whenMatchedUpdateAll()
    merge.whenNotMatchedInsertAll().execute()


# COMMAND ----------

# MAGIC %md
# MAGIC ## Synthetic finance data generator
# MAGIC Produces realistic card transactions with a small fraction of fraud and, optionally, deliberately dirty rows
# MAGIC (missing amounts, negative amounts, bad dates, unknown currencies, duplicates) so the data-quality logic has work to do.
# MAGIC The data is synthetic, so model results on it demonstrate the workflow, not real-world performance.

# COMMAND ----------

CATEGORIES = ["Groceries", "Dining", "Travel", "Utilities", "Electronics", "Fuel",
              "Healthcare", "Entertainment", "Gift Cards", "Crypto"]
CATEGORY_P = [0.24, 0.18, 0.08, 0.10, 0.07, 0.10, 0.06, 0.09, 0.04, 0.04]
RISKY_CATEGORIES = ["Gift Cards", "Crypto", "Electronics"]
COUNTRY_CCY = {"MU": "MUR", "ZA": "ZAR", "FR": "EUR", "GB": "GBP", "US": "USD", "AE": "USD", "IN": "USD"}
COUNTRY_P = [0.70, 0.08, 0.06, 0.05, 0.04, 0.04, 0.03]


def make_transactions(n, start, end, seed=1, id_prefix="TXN", dirty=False):
    rng = np.random.default_rng(seed)
    span = (end - start).total_seconds()
    ts = pd.to_datetime(start) + pd.to_timedelta(rng.random(n) * span, unit="s")
    country = rng.choice(list(COUNTRY_CCY), n, p=COUNTRY_P)
    currency = pd.Series(country).map(COUNTRY_CCY).to_numpy()
    category = rng.choice(CATEGORIES, n, p=CATEGORY_P)
    channel = rng.choice(["POS", "ONLINE", "ATM", "MOBILE"], n, p=[0.40, 0.30, 0.10, 0.20])
    hour = np.asarray(ts.hour)

    usd = rng.lognormal(3.4, 0.9, n)
    p_fraud = (0.004
               + 0.020 * np.isin(category, RISKY_CATEGORIES)
               + 0.020 * (channel == "ONLINE")
               + 0.015 * (country != HOME_COUNTRY)
               + 0.020 * (hour < 5))
    fraud = rng.random(n) < p_fraud
    usd = np.where(fraud, usd * rng.uniform(3, 12, n), usd)
    rate = pd.Series(currency).map(FX_RATES).to_numpy()
    local_amount = np.round(usd / rate, 2)

    df = pd.DataFrame({
        "transaction_id": [f"{id_prefix}{i:08d}" for i in range(n)],
        "account_id": [f"ACC{a:05d}" for a in rng.integers(1, 2001, n)],
        "transaction_ts": ts.strftime("%Y-%m-%d %H:%M:%S"),
        "amount": [f"{x:.2f}" for x in local_amount],
        "currency": currency,
        "merchant_category": category,
        "merchant_name": [f"{c[:3].upper()}-{m:02d}" for c, m in zip(category, rng.integers(1, 60, n))],
        "channel": channel,
        "country": country,
        "is_fraud": fraud.astype(int),
    })

    if dirty:
        k = max(1, int(n * 0.005))
        idx = rng.choice(n, 5 * k, replace=False)
        df.loc[idx[: 2 * k], "amount"] = ""                 # missing amount
        df.loc[idx[2 * k: 3 * k], "amount"] = "-50.00"      # negative amount
        df.loc[idx[3 * k: 4 * k], "transaction_ts"] = "not-a-date"
        df.loc[idx[4 * k:], "currency"] = "XXX"             # unknown currency
        df = pd.concat([df, df.sample(k, random_state=seed)], ignore_index=True)  # duplicates
    return df


def drop_stream_files(n_files=3, rows_per_file=500, seed=100):
    """Simulate new CSV files arriving in the streaming landing folder."""
    now = datetime.now().replace(microsecond=0)
    paths = []
    for i in range(n_files):
        end = now - timedelta(seconds=45 * (n_files - i - 1))
        start = end - timedelta(minutes=3)
        prefix = f"S{now:%m%d%H%M%S}{i}_"
        df = make_transactions(rows_per_file, start, end, seed=seed + i, id_prefix=prefix, dirty=True)
        path = f"{STREAM_DIR}/txns_{now:%Y%m%d_%H%M%S}_{i}.csv"
        df.to_csv(path, index=False)
        paths.append(path)
    print(f"Dropped {len(paths)} files ({rows_per_file} rows each) into {STREAM_DIR}")
    return paths

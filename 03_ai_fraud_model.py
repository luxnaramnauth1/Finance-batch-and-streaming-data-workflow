# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · AI use case: transaction fraud scoring
# MAGIC Builds features from the silver table, trains a gradient-boosting model, tracks it with **MLflow**,
# MAGIC and writes a scored table (`gold_fraud_scores`) that reporting can read.
# MAGIC
# MAGIC - The split is **by time** (train on the earliest 80%, test on the latest 20%), the way the model would be used in production.
# MAGIC - The review budget metric answers a business question: if analysts can only check the top 1% riskiest transactions, how many frauds do they catch?
# MAGIC - Run notebook 01 first. The data is synthetic, so the numbers show the workflow, not real-world performance.

# COMMAND ----------

# MAGIC %run ./00_common

# COMMAND ----------

import mlflow
import mlflow.sklearn
from mlflow.models import infer_signature
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.metrics import average_precision_score, roc_auc_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder

NUMERIC = ["log_amount_usd", "txn_hour", "is_weekend", "acct_txn_count_1h", "amount_vs_acct_avg", "is_foreign"]
CATEGORICAL = ["merchant_category", "channel", "country"]
TARGET = "is_fraud"
REVIEW_BUDGET = 0.01  # analysts review the riskiest 1% of transactions

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Feature engineering with Spark window functions
# MAGIC Behavioural features use only *earlier* transactions of the same account, so there is no leakage from the future.

# COMMAND ----------

w_last_hour = Window.partitionBy("account_id").orderBy("ts_unix").rangeBetween(-3600, -1)
w_history = Window.partitionBy("account_id").orderBy("ts_unix").rowsBetween(-20, -1)

features = (
    spark.table(t("silver_transactions"))
    .withColumn("ts_unix", F.col("txn_ts").cast("long"))
    .withColumn("acct_txn_count_1h", F.count("*").over(w_last_hour))
    .withColumn("acct_avg_usd_prev20", F.avg("amount_usd").over(w_history))
    .withColumn("amount_vs_acct_avg",
                F.col("amount_usd") / F.coalesce(F.col("acct_avg_usd_prev20"), F.col("amount_usd")))
    .withColumn("is_foreign", (F.col("country") != HOME_COUNTRY).cast("int"))
    .withColumn("is_weekend", F.col("is_weekend").cast("int"))
    .withColumn("log_amount_usd", F.log1p("amount_usd"))
    .select("transaction_id", "txn_ts", *NUMERIC, *CATEGORICAL, TARGET)
)

pdf = features.toPandas().sort_values("txn_ts").reset_index(drop=True)
print(f"{len(pdf):,} rows | fraud rate {pdf[TARGET].mean():.2%}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Train, evaluate and log with MLflow

# COMMAND ----------

cutoff = pdf["txn_ts"].quantile(0.8)
train = pdf[pdf["txn_ts"] <= cutoff]
test = pdf[pdf["txn_ts"] > cutoff]
X_train, y_train = train[NUMERIC + CATEGORICAL], train[TARGET]
X_test, y_test = test[NUMERIC + CATEGORICAL], test[TARGET]

model = Pipeline([
    ("prep", ColumnTransformer(
        [("cat", OneHotEncoder(handle_unknown="ignore", sparse_output=False), CATEGORICAL)],
        remainder="passthrough")),
    ("clf", HistGradientBoostingClassifier(max_iter=200, learning_rate=0.08, class_weight="balanced", random_state=42)),
])

with mlflow.start_run(run_name="fraud_hist_gradient_boosting") as run:
    model.fit(X_train, y_train)

    proba_test = model.predict_proba(X_test)[:, 1]
    threshold = float(np.quantile(proba_test, 1 - REVIEW_BUDGET))
    flagged = proba_test >= threshold
    y_arr = y_test.to_numpy()
    precision_at_budget = float(y_arr[flagged].mean()) if flagged.any() else 0.0
    recall_at_budget = float(y_arr[flagged].sum() / max(y_arr.sum(), 1))

    metrics = {
        "roc_auc": float(roc_auc_score(y_test, proba_test)),
        "pr_auc": float(average_precision_score(y_test, proba_test)),
        "precision_at_review_budget": precision_at_budget,
        "recall_at_review_budget": recall_at_budget,
    }
    mlflow.log_params({"model": "HistGradientBoosting", "review_budget": REVIEW_BUDGET,
                       "threshold": round(threshold, 4), "train_rows": len(train), "test_rows": len(test)})
    mlflow.log_metrics(metrics)

    signature = infer_signature(X_train, model.predict_proba(X_train)[:, 1])
    try:
        mlflow.sklearn.log_model(sk_model=model, artifact_path="model", signature=signature,
                                 input_example=X_train.head(5))
    except TypeError:  # newer MLflow versions renamed artifact_path to name
        mlflow.sklearn.log_model(sk_model=model, name="model", signature=signature, input_example=X_train.head(5))

    try:  # registering needs permission on the schema; skip quietly if not allowed
        mlflow.set_registry_uri("databricks-uc")
        mlflow.register_model(f"runs:/{run.info.run_id}/model", t("fraud_detector"))
    except Exception as e:
        print("Model registration skipped:", str(e)[:150])

for k, v in metrics.items():
    print(f"{k:30s} {v:.3f}")
print(f"\nReviewing the top {REVIEW_BUDGET:.0%} riskiest test transactions catches "
      f"{recall_at_budget:.0%} of all fraud, with {precision_at_budget:.0%} precision.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Batch scoring: write the scores table
# MAGIC `split` shows whether a row was in the training period or the held-out period, so reports only quote honest numbers from `test`.
# MAGIC For very large tables, score with `mlflow.pyfunc.spark_udf` instead of pandas.

# COMMAND ----------

pdf["fraud_probability"] = model.predict_proba(pdf[NUMERIC + CATEGORICAL])[:, 1]
pdf["flagged"] = pdf["fraud_probability"] >= threshold
pdf["split"] = np.where(pdf["txn_ts"] <= cutoff, "train", "test")

scores = spark.createDataFrame(
    pdf[["transaction_id", "txn_ts", "fraud_probability", "flagged", "split", TARGET]]
    .rename(columns={TARGET: "actual_fraud"})
).withColumn("scored_at", F.current_timestamp())

scores.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(t("gold_fraud_scores"))
print(f"gold_fraud_scores: {spark.table(t('gold_fraud_scores')).count():,} rows")

# COMMAND ----------

display(
    spark.table(t("gold_fraud_scores")).filter("split = 'test' AND flagged")
    .orderBy(F.desc("fraud_probability")).limit(20)
)

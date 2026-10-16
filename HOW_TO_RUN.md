# Databricks finance workflows: how to run

## 1. Import the notebooks
In Databricks: **Workspace → your folder → Import**, and upload all five `.py` files into the **same folder**
(`00_common`, `01_batch_pipeline`, `02_streaming_pipeline`, `03_ai_fraud_model`, `04_reporting`).
They import as notebooks because of the `# Databricks notebook source` header.

## 2. Run them in order
Attach **serverless** compute (or any cluster), then run:

1. `01_batch_pipeline` - creates the schema and volume, generates a finance CSV, builds bronze, silver and gold Delta tables
2. `02_streaming_pipeline` - Auto Loader ingests new CSV files, then silver and 5-minute window KPIs
3. `03_ai_fraud_model` - trains a fraud model, logs it to MLflow, writes `gold_fraud_scores`
4. `04_reporting` - creates SQL views for dashboards

If your catalog is not `workspace`, change the **Catalog** widget at the top (older workspaces use `main`).

## 3. Use your own finance CSV
Upload it to a Volume (Catalog → your schema → Volumes → Upload), copy its path, and paste it into the
**own_csv_path** widget. If its column names differ, fill `COLUMN_MAP` in `00_common`, for example
`{"Date": "transaction_ts", "Amount": "amount"}`. At minimum it needs a timestamp and an amount column.
Timestamps must look like `2026-01-31 14:05:09`; change the format string in `clean_transactions` if yours differs.

## 4. Run as a scheduled workflow (optional)
`databricks_job.json` defines a job with task dependencies (batch and streaming in parallel, then the model, then reporting).
Replace `<your-email>` with your Databricks user, then create the job from **Workflows → Create job → JSON** or the Jobs API.

## Troubleshooting
- **Permission denied creating schema or volume:** create them manually, or set Catalog and Schema widgets to ones you own.
- **`/Volumes/...` not found:** check the widget values and that `CREATE VOLUME` succeeded in `00_common`.
- **Streaming never ends:** the notebooks use `trigger(availableNow=True)`, which processes the files that exist and stops.

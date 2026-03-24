"""
╔══════════════════════════════════════════════════════════════════╗
║         DATA QUALITY CHECKER — Pipeline Monitoring System        ║
║         Author: Portfolio Project | Data Quality Dashboard       ║
╚══════════════════════════════════════════════════════════════════╝

Usage:
    python data_quality_checker.py

Outputs:
    outputs/data_quality_report.csv
    outputs/schema_issues.csv
    outputs/anomaly_flags.csv
"""

import pandas as pd
import numpy as np
import os
import glob
import logging
from datetime import datetime

# ─────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────
DATA_DIR      = "data"
OUTPUT_DIR    = "outputs"
LOG_FILE      = "outputs/pipeline.log"

# Detection thresholds
NULL_SPIKE_THRESHOLD        = 0.20   # 20% increase in null % triggers alert
MEAN_DRIFT_THRESHOLD        = 0.30   # 30% change in mean triggers alert
DUP_SPIKE_THRESHOLD         = 0.05   # >5% duplicate rate triggers alert
STD_DRIFT_THRESHOLD         = 0.40   # 40% change in std triggers alert

# Expected baseline schema (Day 1)
BASELINE_SCHEMA = {
    "order_id":     "object",
    "customer_id":  "object",
    "order_amount": "float64",
    "order_date":   "object",
    "region":       "object",
}

os.makedirs(OUTPUT_DIR, exist_ok=True)

# ─────────────────────────────────────────────────────────────
# LOGGING SETUP
# ─────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────
# STEP 1: LOAD CSV FILES DYNAMICALLY
# ─────────────────────────────────────────────────────────────
def load_datasets(data_dir: str) -> dict:
    """Dynamically loads all CSV files from the data directory."""
    files = sorted(glob.glob(os.path.join(data_dir, "*.csv")))
    datasets = {}
    for f in files:
        name = os.path.basename(f).replace(".csv", "")
        try:
            df = pd.read_csv(f, dtype=str)   # load all as str to catch mixed types
            datasets[name] = df
            logger.info(f"✅ Loaded: {name} → {df.shape[0]} rows × {df.shape[1]} cols")
        except Exception as e:
            logger.error(f"❌ Failed to load {f}: {e}")
    return datasets


# ─────────────────────────────────────────────────────────────
# STEP 2: COMPUTE PER-DATASET METRICS
# ─────────────────────────────────────────────────────────────
def compute_metrics(name: str, df: pd.DataFrame) -> pd.DataFrame:
    """
    For a single dataset, compute column-level quality metrics.
    Returns a long-format DataFrame.
    """
    records = []
    n_rows = len(df)

    # ── Dedup count (whole-row duplicates) ──────────────────
    dup_count = df.duplicated().sum()
    dup_pct   = round(dup_count / n_rows * 100, 2) if n_rows > 0 else 0.0

    for col in df.columns:
        series = df[col]

        # Null stats
        null_count = series.isna().sum()
        null_pct   = round(null_count / n_rows * 100, 2) if n_rows > 0 else 0.0

        # Infer dtype
        dtype_raw = str(series.dtype)

        # Try numeric conversion for stats
        numeric = pd.to_numeric(series, errors="coerce")
        is_numeric = numeric.notna().sum() > (n_rows * 0.5)   # majority parseable → numeric

        if is_numeric:
            mean_val   = round(numeric.mean(), 4)
            median_val = round(numeric.median(), 4)
            std_val    = round(numeric.std(), 4)
            min_val    = round(numeric.min(), 4)
            max_val    = round(numeric.max(), 4)
            inferred_dtype = "float64"
        else:
            mean_val = median_val = std_val = min_val = max_val = None
            inferred_dtype = "object"

        # Unique count
        unique_count = series.nunique(dropna=True)

        records.append({
            "dataset_name":   name,
            "dataset_date":   name.replace("orders_day_", "Day "),
            "column_name":    col,
            "row_count":      n_rows,
            "column_count":   len(df.columns),
            "null_count":     int(null_count),
            "null_percentage": null_pct,
            "unique_count":   int(unique_count),
            "duplicate_count": int(dup_count),
            "duplicate_pct":  dup_pct,
            "dtype_raw":      dtype_raw,
            "inferred_dtype": inferred_dtype,
            "mean":           mean_val,
            "median":         median_val,
            "std":            std_val,
            "min":            min_val,
            "max":            max_val,
        })

    return pd.DataFrame(records)


def build_quality_report(datasets: dict) -> pd.DataFrame:
    """Build quality report across all datasets."""
    frames = []
    for name, df in datasets.items():
        logger.info(f"📊 Computing metrics for: {name}")
        metrics = compute_metrics(name, df)
        frames.append(metrics)
    return pd.concat(frames, ignore_index=True)


# ─────────────────────────────────────────────────────────────
# STEP 3A: SCHEMA DRIFT DETECTION
# ─────────────────────────────────────────────────────────────
def detect_schema_drift(datasets: dict) -> pd.DataFrame:
    """
    Compare each dataset's schema against baseline (Day 1).
    Detects: new columns, missing columns, dtype changes.
    """
    schema_issues = []
    names = list(datasets.keys())

    if len(names) < 2:
        return pd.DataFrame()

    baseline_name = names[0]
    baseline_df   = datasets[baseline_name]

    # Build baseline schema map
    baseline_cols = set(baseline_df.columns)
    baseline_dtypes = {
        col: ("float64" if pd.to_numeric(baseline_df[col], errors="coerce").notna().sum()
              > len(baseline_df) * 0.5 else "object")
        for col in baseline_df.columns
    }

    for name in names[1:]:
        df        = datasets[name]
        curr_cols = set(df.columns)

        # New columns (added vs baseline)
        for col in curr_cols - baseline_cols:
            schema_issues.append({
                "dataset_name": name,
                "column_name":  col,
                "issue_type":   "NEW_COLUMN",
                "detail":       f"Column '{col}' does not exist in baseline ({baseline_name})",
                "severity":     "MEDIUM",
            })
            logger.warning(f"⚠️  SCHEMA DRIFT [{name}] → NEW COLUMN: '{col}'")

        # Missing columns (dropped vs baseline)
        for col in baseline_cols - curr_cols:
            schema_issues.append({
                "dataset_name": name,
                "column_name":  col,
                "issue_type":   "MISSING_COLUMN",
                "detail":       f"Column '{col}' from baseline is missing in {name}",
                "severity":     "HIGH",
            })
            logger.warning(f"🚨 SCHEMA DRIFT [{name}] → MISSING COLUMN: '{col}'")

        # Dtype changes (for common columns)
        for col in curr_cols & baseline_cols:
            curr_dtype = ("float64" if pd.to_numeric(df[col], errors="coerce").notna().sum()
                          > len(df) * 0.5 else "object")
            base_dtype = baseline_dtypes[col]
            if curr_dtype != base_dtype:
                schema_issues.append({
                    "dataset_name": name,
                    "column_name":  col,
                    "issue_type":   "DTYPE_CHANGE",
                    "detail":       f"Dtype changed from '{base_dtype}' (baseline) to '{curr_dtype}' in {name}",
                    "severity":     "HIGH",
                })
                logger.warning(f"🚨 SCHEMA DRIFT [{name}] → DTYPE CHANGE in '{col}': {base_dtype} → {curr_dtype}")

    return pd.DataFrame(schema_issues) if schema_issues else pd.DataFrame(
        columns=["dataset_name", "column_name", "issue_type", "detail", "severity"]
    )


# ─────────────────────────────────────────────────────────────
# STEP 3B: ANOMALY FLAG DETECTION
# ─────────────────────────────────────────────────────────────
def detect_anomalies(quality_report: pd.DataFrame) -> pd.DataFrame:
    """
    Detects null spikes, distribution drifts, and duplicate spikes
    by comparing consecutive day metrics.
    """
    anomalies = []
    datasets_ordered = quality_report["dataset_name"].unique().tolist()

    for i in range(1, len(datasets_ordered)):
        prev_name = datasets_ordered[i - 1]
        curr_name = datasets_ordered[i]

        prev = quality_report[quality_report["dataset_name"] == prev_name]
        curr = quality_report[quality_report["dataset_name"] == curr_name]

        # ── NULL SPIKE per column ────────────────────────────
        for _, curr_row in curr.iterrows():
            col = curr_row["column_name"]
            prev_row = prev[prev["column_name"] == col]
            if prev_row.empty:
                continue

            curr_null = curr_row["null_percentage"]
            prev_null = prev_row.iloc[0]["null_percentage"]
            delta_null = curr_null - prev_null

            if delta_null > NULL_SPIKE_THRESHOLD * 100:
                anomalies.append({
                    "dataset_name":  curr_name,
                    "column_name":   col,
                    "anomaly_type":  "NULL_SPIKE",
                    "prev_value":    prev_null,
                    "curr_value":    curr_null,
                    "delta":         round(delta_null, 2),
                    "threshold":     NULL_SPIKE_THRESHOLD * 100,
                    "severity":      "HIGH" if delta_null > 40 else "MEDIUM",
                    "message": (
                        f"NULL spike in '{col}': {prev_null}% → {curr_null}% "
                        f"(+{round(delta_null,1)}%) in {curr_name}"
                    ),
                })
                logger.warning(f"🚨 NULL SPIKE [{curr_name}] '{col}': {prev_null}% → {curr_null}% (+{round(delta_null,1)}%)")

        # ── DISTRIBUTION DRIFT (mean) ────────────────────────
        for _, curr_row in curr.iterrows():
            if curr_row["mean"] is None or pd.isna(curr_row["mean"]):
                continue
            col      = curr_row["column_name"]
            prev_row = prev[prev["column_name"] == col]
            if prev_row.empty or prev_row.iloc[0]["mean"] is None or pd.isna(prev_row.iloc[0]["mean"]):
                continue

            curr_mean = curr_row["mean"]
            prev_mean = prev_row.iloc[0]["mean"]

            if prev_mean == 0:
                continue

            mean_change = abs(curr_mean - prev_mean) / abs(prev_mean)

            if mean_change > MEAN_DRIFT_THRESHOLD:
                anomalies.append({
                    "dataset_name":  curr_name,
                    "column_name":   col,
                    "anomaly_type":  "DISTRIBUTION_DRIFT_MEAN",
                    "prev_value":    prev_mean,
                    "curr_value":    curr_mean,
                    "delta":         round(mean_change * 100, 2),
                    "threshold":     MEAN_DRIFT_THRESHOLD * 100,
                    "severity":      "HIGH" if mean_change > 0.5 else "MEDIUM",
                    "message": (
                        f"Mean drift in '{col}': {prev_mean} → {curr_mean} "
                        f"({round(mean_change*100,1)}% change) in {curr_name}"
                    ),
                })
                logger.warning(f"📈 DIST DRIFT [{curr_name}] '{col}' mean: {prev_mean} → {curr_mean} ({round(mean_change*100,1)}%)")

        # ── DUPLICATE SPIKE ──────────────────────────────────
        prev_dup_pct = prev.iloc[0]["duplicate_pct"] if not prev.empty else 0
        curr_dup_pct = curr.iloc[0]["duplicate_pct"] if not curr.empty else 0

        if curr_dup_pct > DUP_SPIKE_THRESHOLD * 100 and curr_dup_pct > prev_dup_pct:
            anomalies.append({
                "dataset_name":  curr_name,
                "column_name":   "ALL",
                "anomaly_type":  "DUPLICATE_SPIKE",
                "prev_value":    prev_dup_pct,
                "curr_value":    curr_dup_pct,
                "delta":         round(curr_dup_pct - prev_dup_pct, 2),
                "threshold":     DUP_SPIKE_THRESHOLD * 100,
                "severity":      "HIGH",
                "message": (
                    f"Duplicate spike: {prev_dup_pct}% → {curr_dup_pct}% in {curr_name}"
                ),
            })
            logger.warning(f"🔁 DUP SPIKE [{curr_name}]: {prev_dup_pct}% → {curr_dup_pct}%")

    return pd.DataFrame(anomalies) if anomalies else pd.DataFrame(
        columns=["dataset_name", "column_name", "anomaly_type",
                 "prev_value", "curr_value", "delta", "threshold", "severity", "message"]
    )


# ─────────────────────────────────────────────────────────────
# STEP 4: DATA HEALTH SCORE
# ─────────────────────────────────────────────────────────────
def compute_health_scores(quality_report: pd.DataFrame, anomaly_flags: pd.DataFrame,
                           schema_issues: pd.DataFrame) -> pd.DataFrame:
    """
    Health Score (0–100) per dataset:
      - Base score: 100
      - Deduct for avg null %:    null_avg_pct * 1.5
      - Deduct for dup %:         dup_pct * 2
      - Deduct for schema issues: 10 per HIGH, 5 per MEDIUM
      - Deduct for anomalies:     8 per HIGH,  4 per MEDIUM
    """
    scores = []
    for dataset in quality_report["dataset_name"].unique():
        sub     = quality_report[quality_report["dataset_name"] == dataset]
        avg_null = sub["null_percentage"].mean()
        dup_pct  = sub["duplicate_pct"].iloc[0]

        schema_sub = schema_issues[schema_issues["dataset_name"] == dataset] if not schema_issues.empty else pd.DataFrame()
        anomaly_sub = anomaly_flags[anomaly_flags["dataset_name"] == dataset] if not anomaly_flags.empty else pd.DataFrame()

        schema_penalty  = (schema_sub["severity"].eq("HIGH").sum() * 10 +
                           schema_sub["severity"].eq("MEDIUM").sum() * 5)
        anomaly_penalty = (anomaly_sub["severity"].eq("HIGH").sum() * 8 +
                           anomaly_sub["severity"].eq("MEDIUM").sum() * 4)

        raw_score = 100 - (avg_null * 1.5) - (dup_pct * 2) - schema_penalty - anomaly_penalty
        final_score = max(0, round(raw_score, 1))

        grade = ("A" if final_score >= 90 else "B" if final_score >= 75 else
                 "C" if final_score >= 60 else "D" if final_score >= 40 else "F")

        scores.append({
            "dataset_name":     dataset,
            "avg_null_pct":     round(avg_null, 2),
            "duplicate_pct":    round(dup_pct, 2),
            "schema_penalties": schema_penalty,
            "anomaly_penalties": anomaly_penalty,
            "health_score":     final_score,
            "grade":            grade,
        })

        logger.info(f"💯 Health Score [{dataset}]: {final_score}/100 (Grade: {grade})")

    return pd.DataFrame(scores)


# ─────────────────────────────────────────────────────────────
# STEP 5: PRINT ALERT SUMMARY
# ─────────────────────────────────────────────────────────────
def print_alert_summary(health_scores: pd.DataFrame, anomaly_flags: pd.DataFrame,
                         schema_issues: pd.DataFrame):
    """Prints a structured alert summary to stdout."""
    border = "═" * 65
    print(f"\n{border}")
    print("  🚨  DATA QUALITY PIPELINE — ALERT SUMMARY")
    print(f"{border}")

    print("\n📋 DATASET HEALTH SCORES:")
    print(f"  {'Dataset':<22} {'Score':>6}  {'Grade':>5}  {'Avg Null%':>9}  {'Dup%':>6}")
    print(f"  {'─'*22} {'─'*6}  {'─'*5}  {'─'*9}  {'─'*6}")
    for _, row in health_scores.iterrows():
        icon = "✅" if row["grade"] in ("A", "B") else "⚠️ " if row["grade"] == "C" else "❌"
        print(f"  {icon} {row['dataset_name']:<20} {row['health_score']:>6}  "
              f"{row['grade']:>5}  {row['avg_null_pct']:>8.1f}%  {row['duplicate_pct']:>5.1f}%")

    if not schema_issues.empty:
        print(f"\n🔀 SCHEMA DRIFT ISSUES ({len(schema_issues)} total):")
        for _, row in schema_issues.iterrows():
            icon = "🚨" if row["severity"] == "HIGH" else "⚠️ "
            print(f"  {icon} [{row['dataset_name']}] {row['issue_type']}: {row['detail']}")

    if not anomaly_flags.empty:
        print(f"\n📣 ANOMALY FLAGS ({len(anomaly_flags)} total):")
        for _, row in anomaly_flags.iterrows():
            icon = "🚨" if row["severity"] == "HIGH" else "⚠️ "
            print(f"  {icon} {row['message']}")

    print(f"\n{border}\n")


# ─────────────────────────────────────────────────────────────
# MAIN PIPELINE
# ─────────────────────────────────────────────────────────────
def run_pipeline():
    logger.info("=" * 65)
    logger.info("  DATA QUALITY PIPELINE STARTED")
    logger.info(f"  Run time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 65)

    # 1. Load
    datasets = load_datasets(DATA_DIR)
    if not datasets:
        logger.error("No datasets found! Check DATA_DIR path.")
        return

    # 2. Metrics
    quality_report = build_quality_report(datasets)
    quality_report.to_csv(f"{OUTPUT_DIR}/data_quality_report.csv", index=False)
    logger.info(f"✅ Saved: {OUTPUT_DIR}/data_quality_report.csv ({len(quality_report)} rows)")

    # 3. Schema Drift
    schema_issues = detect_schema_drift(datasets)
    schema_issues.to_csv(f"{OUTPUT_DIR}/schema_issues.csv", index=False)
    logger.info(f"✅ Saved: {OUTPUT_DIR}/schema_issues.csv ({len(schema_issues)} issues)")

    # 4. Anomaly Flags
    anomaly_flags = detect_anomalies(quality_report)
    anomaly_flags.to_csv(f"{OUTPUT_DIR}/anomaly_flags.csv", index=False)
    logger.info(f"✅ Saved: {OUTPUT_DIR}/anomaly_flags.csv ({len(anomaly_flags)} flags)")

    # 5. Health Scores
    health_scores = compute_health_scores(quality_report, anomaly_flags, schema_issues)
    health_scores.to_csv(f"{OUTPUT_DIR}/health_scores.csv", index=False)
    logger.info(f"✅ Saved: {OUTPUT_DIR}/health_scores.csv")

    # 6. Alert Summary
    print_alert_summary(health_scores, anomaly_flags, schema_issues)

    logger.info("  PIPELINE COMPLETE")
    logger.info("=" * 65)


if __name__ == "__main__":
    run_pipeline()

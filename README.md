# 📊 Data Quality & Pipeline Monitoring Dashboard

> **Portfolio Project** · Data Engineering + Data Analysis · End-to-End

[![Python](https://img.shields.io/badge/Python-3.9%2B-blue?logo=python)](https://www.python.org/)
[![Pandas](https://img.shields.io/badge/Pandas-2.x-150458?logo=pandas)](https://pandas.pydata.org/)
[![SQL](https://img.shields.io/badge/SQL-PostgreSQL-336791?logo=postgresql)](https://www.postgresql.org/)
[![Power BI](https://img.shields.io/badge/Dashboard-Power%20BI-F2C811?logo=powerbi)](https://powerbi.microsoft.com/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

---

## 📌 Problem Statement

Modern data pipelines ingest data from multiple sources daily. Without automated monitoring, data quality issues silently corrupt downstream reports, dashboards, and ML models. This project builds a complete, production-inspired system that:

- Detects **null value spikes** before they reach analysts
- Catches **schema drift** (added/removed/changed columns) between runs
- Flags **statistical distribution shifts** in numeric fields
- Identifies **duplicate record spikes** during ingestion
- Computes a **health score** per dataset for at-a-glance monitoring

---

## 🎯 Solution Approach

```
Raw CSV Files (3 days)
        │
        ▼
 generate_datasets.py        ← Simulates real-world pipeline issues
        │
        ▼
data_quality_checker.py      ← Computes metrics, detects anomalies
        │
        ├── data_quality_report.csv
        ├── schema_issues.csv
        ├── anomaly_flags.csv
        └── health_scores.csv
                │
                ▼
         SQL Tables (PostgreSQL)
                │
                ▼
         Power BI Dashboard (5 pages)
```

---

## 🛠️ Tech Stack

| Layer | Tool | Purpose |
|-------|------|---------|
| Data Generation | Python (Pandas, NumPy) | Simulate pipeline datasets with issues |
| Data Quality Engine | Python (Pandas) | Compute metrics, detect anomalies |
| Storage | CSV → PostgreSQL | Persist quality reports |
| SQL Analysis | PostgreSQL | Trend queries, issue ranking, KPIs |
| Visualization | Power BI Desktop | Interactive 5-page dashboard |
| Logging | Python `logging` | Audit trail of all pipeline runs |

---

## 📁 Project Structure

```
data_quality_project/
│
├── data/                          ← Raw pipeline datasets
│   ├── orders_day_1.csv           ← Clean baseline (120 rows)
│   ├── orders_day_2.csv           ← Null spikes + schema drift (120 rows)
│   └── orders_day_3.csv           ← Distribution shift + duplicates (135 rows)
│
├── outputs/                       ← Generated quality reports
│   ├── data_quality_report.csv    ← Column-level metrics per dataset
│   ├── schema_issues.csv          ← Schema drift log
│   ├── anomaly_flags.csv          ← Detected anomalies
│   ├── health_scores.csv          ← Dataset health scores + grades
│   └── pipeline.log               ← Run audit log
│
├── sql/
│   └── data_quality_sql.sql       ← CREATE TABLE + LOAD + 10 queries
│
├── generate_datasets.py           ← Dataset generator
├── data_quality_checker.py        ← Main pipeline script
└── README.md
```

---

## 🔍 Issues Introduced Per Dataset

| Issue | Day 1 | Day 2 | Day 3 |
|-------|:-----:|:-----:|:-----:|
| Missing values | ❌ | ✅ ~28% in customer_id, order_amount | ✅ ~22% in region |
| Schema drift (new column) | ❌ | ✅ `discount_code` added | ✅ `discount_code` |
| Schema drift (missing column) | ❌ | ✅ `region` removed | ❌ |
| Datatype inconsistency | ❌ | ✅ Mixed strings in order_amount | ❌ |
| Duplicate records | ❌ | ❌ | ✅ ~15 duplicate rows |
| Distribution shift | ❌ | ❌ | ✅ mean: 154 → 322 (+109%) |

---

## 🚨 Key Detections (Sample Output)

```
═══════════════════════════════════════════════════════════════════
  🚨  DATA QUALITY PIPELINE — ALERT SUMMARY
═══════════════════════════════════════════════════════════════════

📋 DATASET HEALTH SCORES:
  Dataset                 Score  Grade  Avg Null%    Dup%
  ────────────────────── ──────  ─────  ─────────  ──────
  ✅ orders_day_1          100.0      A       0.0%    0.0%
  ❌ orders_day_2           49.2      D      18.5%    0.0%
  ❌ orders_day_3           56.1      D      15.7%    3.7%

🔀 SCHEMA DRIFT ISSUES (3 total):
  ⚠️  [orders_day_2] NEW_COLUMN: 'discount_code' added
  🚨 [orders_day_2] MISSING_COLUMN: 'region' dropped
  ⚠️  [orders_day_3] NEW_COLUMN: 'discount_code' still present

📣 ANOMALY FLAGS (3 total):
  ⚠️  NULL spike in 'customer_id': 0.0% → 27.5% (+27.5%)
  ⚠️  NULL spike in 'order_amount': 0.0% → 27.5% (+27.5%)
  🚨 Mean drift in 'order_amount': 153.9 → 322.5 (+109.5%)
```

---

## 💯 Health Score Formula

```
Health Score = 100
    - (avg_null_pct × 1.5)
    - (duplicate_pct × 2)
    - (HIGH schema issues × 10)
    - (MEDIUM schema issues × 5)
    - (HIGH anomaly flags × 8)
    - (MEDIUM anomaly flags × 4)

Grade: A (≥90) | B (≥75) | C (≥60) | D (≥40) | F (<40)
```

---

## ▶️ How to Run

### Prerequisites
```bash
pip install pandas numpy
```

### 1. Generate Datasets
```bash
python generate_datasets.py
# → Creates data/orders_day_1.csv, orders_day_2.csv, orders_day_3.csv
```

### 2. Run Quality Pipeline
```bash
python data_quality_checker.py
# → Creates outputs/data_quality_report.csv
# → Creates outputs/schema_issues.csv
# → Creates outputs/anomaly_flags.csv
# → Creates outputs/health_scores.csv
# → Creates outputs/pipeline.log
```

### 3. Load into SQL (optional)
```bash
# Open PostgreSQL and run:
psql -U your_user -d your_db -f sql/data_quality_sql.sql
# Then load CSVs using \COPY commands in the SQL file
```

### 4. Build Power BI Dashboard
```
Follow: POWERBI_GUIDE.md
Load 4 CSVs from outputs/ into Power BI Desktop
Build 5 pages following the layout guide
```

---

## 📊 Sample Outputs

**data_quality_report.csv** (sample rows):
```
dataset_name,   column_name,  null_percentage, mean,     std,   inferred_dtype
orders_day_1,   order_amount, 0.0,             153.91,   39.88, float64
orders_day_2,   order_amount, 27.5,            NULL,     NULL,  object
orders_day_3,   order_amount, 0.0,             322.46,   89.55, float64
```

**anomaly_flags.csv** (sample rows):
```
dataset_name,  column_name,  anomaly_type,           delta, severity, message
orders_day_2,  customer_id,  NULL_SPIKE,             27.5,  MEDIUM,   NULL spike: 0% → 27.5%
orders_day_3,  order_amount, DISTRIBUTION_DRIFT_MEAN, 109.5, HIGH,   Mean drift: 153.91 → 322.46
```

---

## 🧠 Key Insights from the Data

1. **Day 2 had the worst schema stability** — a column was dropped (`region`) and one added (`discount_code`), combined with >27% nulls in critical fields
2. **Day 3 showed a severe distribution shift** — order amounts nearly doubled in mean value (153 → 322), indicating either a data source change or pricing model shift
3. **Duplicate records appeared on Day 3** — 15 duplicated rows (3.7% of dataset), suggesting an upstream deduplication step failed
4. **`order_amount` is the highest-risk column** — it suffered null spikes, datatype corruption, and distribution drift across 2 of 3 days

---

## 🔮 Extension Ideas

- [ ] Add email/Slack alerting via `smtplib` or `requests` webhook
- [ ] Schedule with Apache Airflow or cron
- [ ] Add Great Expectations integration for richer validation
- [ ] Connect to real S3 / BigQuery / Snowflake sources
- [ ] Add row-count trend monitoring
- [ ] Build a Streamlit web UI as an alternative to Power BI

---

## 📜 License

MIT License — free to use, modify, and share for portfolio or commercial purposes.

---

*Built as a portfolio project demonstrating end-to-end data quality engineering skills.*
*Stack: Python · Pandas · PostgreSQL · Power BI*

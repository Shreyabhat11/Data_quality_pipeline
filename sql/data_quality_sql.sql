-- ╔══════════════════════════════════════════════════════════════╗
-- ║      DATA QUALITY DASHBOARD — SQL INTEGRATION               ║
-- ║      Dialect: PostgreSQL (compatible with MySQL w/ minor edits)
-- ╚══════════════════════════════════════════════════════════════╝


-- ══════════════════════════════════════════════════════════════
-- SECTION 1: CREATE TABLES
-- ══════════════════════════════════════════════════════════════

-- Drop tables if re-running (dev convenience)
DROP TABLE IF EXISTS anomaly_flags;
DROP TABLE IF EXISTS schema_issues;
DROP TABLE IF EXISTS data_quality_metrics;
DROP TABLE IF EXISTS health_scores;

-- ── Table 1: data_quality_metrics ─────────────────────────────
CREATE TABLE data_quality_metrics (
    id                  SERIAL PRIMARY KEY,
    dataset_name        VARCHAR(100)   NOT NULL,
    dataset_date        VARCHAR(20)    NOT NULL,
    column_name         VARCHAR(100)   NOT NULL,
    row_count           INTEGER,
    column_count        INTEGER,
    null_count          INTEGER,
    null_percentage     NUMERIC(6,2),
    unique_count        INTEGER,
    duplicate_count     INTEGER,
    duplicate_pct       NUMERIC(6,2),
    dtype_raw           VARCHAR(50),
    inferred_dtype      VARCHAR(50),
    mean                NUMERIC(14,4),
    median              NUMERIC(14,4),
    std                 NUMERIC(14,4),
    min                 NUMERIC(14,4),
    max                 NUMERIC(14,4),
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ── Table 2: schema_issues ────────────────────────────────────
CREATE TABLE schema_issues (
    id              SERIAL PRIMARY KEY,
    dataset_name    VARCHAR(100)  NOT NULL,
    column_name     VARCHAR(100),
    issue_type      VARCHAR(50),   -- NEW_COLUMN | MISSING_COLUMN | DTYPE_CHANGE
    detail          TEXT,
    severity        VARCHAR(10),   -- HIGH | MEDIUM | LOW
    detected_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ── Table 3: anomaly_flags ────────────────────────────────────
CREATE TABLE anomaly_flags (
    id              SERIAL PRIMARY KEY,
    dataset_name    VARCHAR(100)  NOT NULL,
    column_name     VARCHAR(100),
    anomaly_type    VARCHAR(50),   -- NULL_SPIKE | DISTRIBUTION_DRIFT_MEAN | DUPLICATE_SPIKE
    prev_value      NUMERIC(14,4),
    curr_value      NUMERIC(14,4),
    delta           NUMERIC(14,4),
    threshold       NUMERIC(14,4),
    severity        VARCHAR(10),
    message         TEXT,
    detected_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ── Table 4: health_scores ────────────────────────────────────
CREATE TABLE health_scores (
    id                  SERIAL PRIMARY KEY,
    dataset_name        VARCHAR(100)  NOT NULL,
    avg_null_pct        NUMERIC(6,2),
    duplicate_pct       NUMERIC(6,2),
    schema_penalties    INTEGER,
    anomaly_penalties   INTEGER,
    health_score        NUMERIC(5,1),
    grade               CHAR(1),
    scored_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


-- ══════════════════════════════════════════════════════════════
-- SECTION 2: LOAD DATA FROM CSV (PostgreSQL COPY syntax)
-- ══════════════════════════════════════════════════════════════

-- NOTE: Update file paths to match your system.
-- Run these commands as a PostgreSQL superuser or with COPY privilege.

/*
\COPY data_quality_metrics (dataset_name, dataset_date, column_name, row_count,
    column_count, null_count, null_percentage, unique_count, duplicate_count,
    duplicate_pct, dtype_raw, inferred_dtype, mean, median, std, min, max)
FROM '/path/to/outputs/data_quality_report.csv'
WITH (FORMAT CSV, HEADER TRUE, NULL '');

\COPY schema_issues (dataset_name, column_name, issue_type, detail, severity)
FROM '/path/to/outputs/schema_issues.csv'
WITH (FORMAT CSV, HEADER TRUE, NULL '');

\COPY anomaly_flags (dataset_name, column_name, anomaly_type, prev_value,
    curr_value, delta, threshold, severity, message)
FROM '/path/to/outputs/anomaly_flags.csv'
WITH (FORMAT CSV, HEADER TRUE, NULL '');

\COPY health_scores (dataset_name, avg_null_pct, duplicate_pct,
    schema_penalties, anomaly_penalties, health_score, grade)
FROM '/path/to/outputs/health_scores.csv'
WITH (FORMAT CSV, HEADER TRUE, NULL '');
*/

-- ── SAMPLE INSERT (fallback if COPY not available) ─────────────
INSERT INTO data_quality_metrics
    (dataset_name, dataset_date, column_name, row_count, column_count,
     null_count, null_percentage, unique_count, duplicate_count,
     duplicate_pct, dtype_raw, inferred_dtype, mean, median, std, min, max)
VALUES
    ('orders_day_1', 'Day 1', 'order_amount', 120, 5, 0, 0.00, 118, 0, 0.00, 'object', 'float64', 153.91, 151.22, 39.88, 10.05, 498.77),
    ('orders_day_2', 'Day 2', 'order_amount', 120, 5, 33, 27.50, 85, 0, 0.00, 'object', 'object',  NULL,   NULL,   NULL,  NULL,  NULL),
    ('orders_day_3', 'Day 3', 'order_amount', 135, 6, 0, 0.00, 119, 5, 3.70, 'object', 'float64', 322.46, 318.12, 89.55, 52.30, 899.12);

INSERT INTO schema_issues
    (dataset_name, column_name, issue_type, detail, severity)
VALUES
    ('orders_day_2', 'discount_code', 'NEW_COLUMN',      'Column discount_code does not exist in baseline', 'MEDIUM'),
    ('orders_day_2', 'region',        'MISSING_COLUMN',  'Column region from baseline is missing',           'HIGH'),
    ('orders_day_3', 'discount_code', 'NEW_COLUMN',      'Column discount_code does not exist in baseline', 'MEDIUM');

INSERT INTO anomaly_flags
    (dataset_name, column_name, anomaly_type, prev_value, curr_value, delta, threshold, severity, message)
VALUES
    ('orders_day_2', 'customer_id',   'NULL_SPIKE',              0.00,   27.50, 27.50, 20.00, 'MEDIUM', 'NULL spike in customer_id: 0% → 27.5% (+27.5%)'),
    ('orders_day_2', 'order_amount',  'NULL_SPIKE',              0.00,   27.50, 27.50, 20.00, 'MEDIUM', 'NULL spike in order_amount: 0% → 27.5% (+27.5%)'),
    ('orders_day_3', 'order_amount',  'DISTRIBUTION_DRIFT_MEAN', 153.91, 322.46, 109.50, 30.00, 'HIGH', 'Mean drift in order_amount: 153.91 → 322.46 (109.5% change)');

INSERT INTO health_scores
    (dataset_name, avg_null_pct, duplicate_pct, schema_penalties, anomaly_penalties, health_score, grade)
VALUES
    ('orders_day_1', 0.00,  0.00, 0,  0, 100.0, 'A'),
    ('orders_day_2', 18.50, 0.00, 15, 8,  49.2, 'D'),
    ('orders_day_3', 15.70, 3.70, 5,  12, 56.1, 'D');


-- ══════════════════════════════════════════════════════════════
-- SECTION 3: ANALYTICAL QUERIES
-- ══════════════════════════════════════════════════════════════

-- ── Q1: NULL TRENDS — null % per column across all days ───────
SELECT
    dataset_date,
    column_name,
    null_percentage,
    null_count,
    row_count,
    ROUND(
        null_percentage - LAG(null_percentage) OVER (
            PARTITION BY column_name ORDER BY dataset_date
        ), 2
    ) AS null_pct_delta
FROM data_quality_metrics
WHERE null_count IS NOT NULL
ORDER BY column_name, dataset_date;


-- ── Q2: WORST COLUMNS BY AVERAGE NULL % ───────────────────────
SELECT
    column_name,
    ROUND(AVG(null_percentage), 2)   AS avg_null_pct,
    MAX(null_percentage)             AS peak_null_pct,
    COUNT(DISTINCT dataset_name)     AS days_with_nulls
FROM data_quality_metrics
WHERE null_percentage > 0
GROUP BY column_name
ORDER BY avg_null_pct DESC;


-- ── Q3: SCHEMA CHANGE HISTORY ─────────────────────────────────
SELECT
    dataset_name,
    issue_type,
    column_name,
    severity,
    detail,
    detected_at
FROM schema_issues
ORDER BY
    CASE severity WHEN 'HIGH' THEN 1 WHEN 'MEDIUM' THEN 2 ELSE 3 END,
    detected_at;


-- ── Q4: ALL ANOMALY FLAGS WITH CONTEXT ────────────────────────
SELECT
    af.dataset_name,
    af.column_name,
    af.anomaly_type,
    af.severity,
    af.prev_value,
    af.curr_value,
    af.delta,
    af.message,
    hs.health_score,
    hs.grade
FROM anomaly_flags af
LEFT JOIN health_scores hs ON af.dataset_name = hs.dataset_name
ORDER BY
    CASE af.severity WHEN 'HIGH' THEN 1 WHEN 'MEDIUM' THEN 2 ELSE 3 END,
    af.dataset_name;


-- ── Q5: HEALTH SCORE TREND ACROSS DAYS ───────────────────────
SELECT
    dataset_name,
    health_score,
    grade,
    avg_null_pct,
    duplicate_pct,
    schema_penalties   AS schema_score_deduction,
    anomaly_penalties  AS anomaly_score_deduction
FROM health_scores
ORDER BY dataset_name;


-- ── Q6: DISTRIBUTION DRIFT — MEAN AND STD OVER TIME ──────────
SELECT
    dataset_date,
    column_name,
    mean,
    std,
    ROUND(
        mean - LAG(mean) OVER (PARTITION BY column_name ORDER BY dataset_date),
    4) AS mean_delta,
    ROUND(
        (mean - LAG(mean) OVER (PARTITION BY column_name ORDER BY dataset_date))
        / NULLIF(ABS(LAG(mean) OVER (PARTITION BY column_name ORDER BY dataset_date)), 0) * 100,
    2) AS mean_pct_change
FROM data_quality_metrics
WHERE mean IS NOT NULL
ORDER BY column_name, dataset_date;


-- ── Q7: DUPLICATE SPIKE SUMMARY ──────────────────────────────
SELECT
    dataset_name,
    dataset_date,
    MAX(duplicate_count) AS total_duplicates,
    MAX(duplicate_pct)   AS duplicate_pct,
    MAX(row_count)        AS total_rows
FROM data_quality_metrics
GROUP BY dataset_name, dataset_date
ORDER BY dataset_date;


-- ── Q8: SUMMARY DASHBOARD KPIs ───────────────────────────────
SELECT
    COUNT(DISTINCT dataset_name)            AS total_datasets,
    ROUND(AVG(null_percentage), 2)          AS overall_avg_null_pct,
    ROUND(AVG(duplicate_pct), 2)            AS overall_avg_dup_pct,
    SUM(CASE WHEN null_percentage > 20 THEN 1 ELSE 0 END)  AS columns_with_high_nulls,
    (SELECT COUNT(*) FROM schema_issues)    AS total_schema_issues,
    (SELECT COUNT(*) FROM anomaly_flags)    AS total_anomalies,
    (SELECT ROUND(AVG(health_score),1) FROM health_scores) AS avg_health_score
FROM data_quality_metrics;


-- ── Q9: FRESHNESS CHECK — latest dataset load time ───────────
SELECT
    dataset_name,
    MAX(created_at)  AS last_loaded_at,
    CURRENT_TIMESTAMP - MAX(created_at) AS data_age
FROM data_quality_metrics
GROUP BY dataset_name
ORDER BY last_loaded_at DESC;


-- ── Q10: COLUMN-LEVEL ISSUES MATRIX ──────────────────────────
SELECT
    m.column_name,
    COUNT(DISTINCT m.dataset_name)                         AS datasets_tracked,
    ROUND(AVG(m.null_percentage), 2)                       AS avg_null_pct,
    COALESCE(si.schema_issue_count, 0)                     AS schema_issues,
    COALESCE(af.anomaly_count, 0)                          AS anomalies,
    (ROUND(AVG(m.null_percentage), 2) * 1.5
        + COALESCE(si.schema_issue_count, 0) * 10
        + COALESCE(af.anomaly_count, 0) * 8)               AS risk_score
FROM data_quality_metrics m
LEFT JOIN (
    SELECT column_name, COUNT(*) AS schema_issue_count
    FROM schema_issues
    GROUP BY column_name
) si ON m.column_name = si.column_name
LEFT JOIN (
    SELECT column_name, COUNT(*) AS anomaly_count
    FROM anomaly_flags
    GROUP BY column_name
) af ON m.column_name = af.column_name
GROUP BY m.column_name, si.schema_issue_count, af.anomaly_count
ORDER BY risk_score DESC;

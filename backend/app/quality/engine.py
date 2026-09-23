"""
Data quality engine.

This module is a refactor of the original `data_quality_checker.py` script
from the Data_quality_pipeline repository. The detection logic (null-spike,
schema-drift, distribution-drift, duplicate-spike, health scoring) is the
same logic and the same thresholds as the original script -- it has been
turned into pure, dependency-free functions that operate on in-memory
pandas DataFrames instead of scanning a folder of CSVs on disk. This makes
the engine callable from an API request and unit-testable.

Key behavioural change from the original script
-------------------------------------------------
The original script treated "the first file in the data/ folder" as the
baseline and compared each *subsequent* file to the *previous* file
(Day 1 -> Day 2 -> Day 3). That doesn't map onto a single-file upload flow.

Here, a validation run compares exactly two things, both explicit:
  - `current`  : the dataset the user just uploaded
  - `baseline` : an optional reference dataset the user chose to compare against

If no baseline is supplied, only checks that don't require a point of
comparison are run (null percentages, duplicate rate, and intra-file
datatype consistency). Schema-drift and anomaly-drift checks require a
baseline and are skipped (not faked) when one isn't provided.

New checks added beyond the original script
--------------------------------------------
- DATATYPE_INCONSISTENCY: flags a column that is neither cleanly numeric
  nor cleanly text (a mix of parseable and non-parseable values), which the
  original script could only see indirectly via a DTYPE_CHANGE against a
  baseline. This lets datatype problems surface even with no baseline.
- STD_DRIFT: the original script defined STD_DRIFT_THRESHOLD but never used
  it. It's implemented here as a genuine check (large change in a numeric
  column's standard deviation vs. baseline), since an unused constant
  suggested unfinished work rather than an intentional omission.
"""

from __future__ import annotations

import csv
import io
from dataclasses import dataclass, field
from io import BytesIO
from typing import Optional

import numpy as np
import pandas as pd

# ─────────────────────────────────────────────────────────────
# CONFIG (ported from the original script's module-level constants)
# ─────────────────────────────────────────────────────────────

NULL_SPIKE_THRESHOLD_PP = 20.0      # percentage-POINT increase in null % triggers alert
MEAN_DRIFT_THRESHOLD = 0.30         # 30% relative change in mean triggers alert
DUP_SPIKE_THRESHOLD_PCT = 5.0       # duplicate rate above this % (and rising) triggers alert
STD_DRIFT_THRESHOLD = 0.40          # 40% relative change in std triggers alert (new: now used)

# A column is flagged DATATYPE_INCONSISTENCY when the fraction of its
# non-null values that parse as numeric falls strictly between these two
# bounds -- i.e. it's neither "clearly numeric" nor "clearly text".
MIXED_TYPE_LOWER_BOUND = 0.05
MIXED_TYPE_UPPER_BOUND = 0.95

MAX_ROWS = 2_000_000  # sanity cap; a portfolio-scale guard, not a hard product limit


class QualityEngineError(ValueError):
    """Raised for problems with the input file itself (not server errors)."""


# ─────────────────────────────────────────────────────────────
# CSV LOADING
# ─────────────────────────────────────────────────────────────

def read_csv_bytes(raw: bytes, filename: str = "upload.csv") -> pd.DataFrame:
    """
    Parse uploaded CSV bytes into a DataFrame, loaded as strings (matching
    the original script's approach) so mixed-type columns are preserved
    for inspection rather than being silently coerced by pandas' own
    type inference.
    """
    if not raw or not raw.strip():
        raise QualityEngineError(f"'{filename}' is empty.")

    _validate_row_lengths(raw, filename)

    try:
        df = pd.read_csv(BytesIO(raw), dtype=str, keep_default_na=True, index_col=False)
    except pd.errors.EmptyDataError as exc:
        raise QualityEngineError(f"'{filename}' has no columns to parse.") from exc
    except pd.errors.ParserError as exc:
        raise QualityEngineError(f"'{filename}' is not a valid CSV: {exc}") from exc
    except UnicodeDecodeError as exc:
        raise QualityEngineError(f"'{filename}' is not valid UTF-8 text.") from exc

    if df.shape[1] == 0:
        raise QualityEngineError(f"'{filename}' has no columns.")
    if df.shape[0] == 0:
        raise QualityEngineError(f"'{filename}' has headers but no data rows.")
    if df.shape[0] > MAX_ROWS:
        raise QualityEngineError(
            f"'{filename}' has {df.shape[0]} rows, exceeding the {MAX_ROWS}-row limit."
        )
    return df


def _validate_row_lengths(raw: bytes, filename: str) -> None:
    """
    pandas silently tolerates ragged rows (either by mis-assigning an index
    column, or by dropping data with a warning), which would let a
    malformed file through disguised as clean data. Reject it explicitly
    instead: every data row must have the same number of fields as the
    header row.
    """
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise QualityEngineError(f"'{filename}' is not valid UTF-8 text.") from exc

    reader = csv.reader(io.StringIO(text))
    rows = list(reader)
    rows = [r for r in rows if r]  # drop fully blank lines
    if not rows:
        raise QualityEngineError(f"'{filename}' has no rows to parse.")

    header_len = len(rows[0])
    for i, row in enumerate(rows[1:], start=2):
        if len(row) != header_len:
            raise QualityEngineError(
                f"'{filename}' is malformed: row {i} has {len(row)} fields, "
                f"expected {header_len} (matching the header)."
            )


# ─────────────────────────────────────────────────────────────
# DATA STRUCTURES
# ─────────────────────────────────────────────────────────────

@dataclass
class ColumnMetric:
    column_name: str
    null_count: int
    null_percentage: float
    unique_count: int
    dtype_raw: str
    inferred_dtype: str
    numeric_ratio: float
    mean: Optional[float] = None
    median: Optional[float] = None
    std: Optional[float] = None
    min: Optional[float] = None
    max: Optional[float] = None

    def to_dict(self) -> dict:
        return {
            "column_name": self.column_name,
            "null_count": self.null_count,
            "null_percentage": self.null_percentage,
            "unique_count": self.unique_count,
            "dtype_raw": self.dtype_raw,
            "inferred_dtype": self.inferred_dtype,
            "numeric_ratio": self.numeric_ratio,
            "mean": self.mean,
            "median": self.median,
            "std": self.std,
            "min": self.min,
            "max": self.max,
        }


@dataclass
class Finding:
    kind: str            # "schema" | "anomaly" | "datatype"
    issue_type: str
    column_name: str
    severity: str         # "HIGH" | "MEDIUM"
    message: str
    prev_value: Optional[float] = None
    curr_value: Optional[float] = None
    delta: Optional[float] = None

    def to_dict(self) -> dict:
        return {
            "kind": self.kind,
            "issue_type": self.issue_type,
            "column_name": self.column_name,
            "severity": self.severity,
            "message": self.message,
            "prev_value": self.prev_value,
            "curr_value": self.curr_value,
            "delta": self.delta,
        }


@dataclass
class QualityResult:
    dataset_name: str
    row_count: int
    column_count: int
    duplicate_count: int
    duplicate_percentage: float
    has_baseline: bool
    baseline_name: Optional[str]
    column_metrics: list[ColumnMetric] = field(default_factory=list)
    findings: list[Finding] = field(default_factory=list)
    health_score: float = 0.0
    grade: str = "A"

    @property
    def schema_findings(self) -> list[Finding]:
        return [f for f in self.findings if f.kind == "schema"]

    @property
    def anomaly_findings(self) -> list[Finding]:
        return [f for f in self.findings if f.kind == "anomaly"]

    @property
    def datatype_findings(self) -> list[Finding]:
        return [f for f in self.findings if f.kind == "datatype"]


# ─────────────────────────────────────────────────────────────
# STEP 1: PER-COLUMN METRICS (ported from compute_metrics)
# ─────────────────────────────────────────────────────────────

def compute_column_metrics(df: pd.DataFrame) -> list[ColumnMetric]:
    n_rows = len(df)
    metrics: list[ColumnMetric] = []

    for col in df.columns:
        series = df[col]
        null_count = int(series.isna().sum())
        null_pct = round(null_count / n_rows * 100, 2) if n_rows else 0.0

        numeric = pd.to_numeric(series, errors="coerce")
        non_null = series.notna().sum()
        numeric_non_null = numeric.notna().sum()
        numeric_ratio = round(numeric_non_null / non_null, 4) if non_null else 0.0
        is_numeric = numeric_ratio > 0.5

        if is_numeric:
            mean_val = round(float(numeric.mean()), 4) if numeric.notna().any() else None
            median_val = round(float(numeric.median()), 4) if numeric.notna().any() else None
            std_val = round(float(numeric.std()), 4) if numeric.notna().sum() > 1 else None
            min_val = round(float(numeric.min()), 4) if numeric.notna().any() else None
            max_val = round(float(numeric.max()), 4) if numeric.notna().any() else None
            inferred_dtype = "float64"
        else:
            mean_val = median_val = std_val = min_val = max_val = None
            inferred_dtype = "object"

        metrics.append(
            ColumnMetric(
                column_name=col,
                null_count=null_count,
                null_percentage=null_pct,
                unique_count=int(series.nunique(dropna=True)),
                dtype_raw=str(series.dtype),
                inferred_dtype=inferred_dtype,
                numeric_ratio=numeric_ratio,
                mean=mean_val,
                median=median_val,
                std=std_val,
                min=min_val,
                max=max_val,
            )
        )
    return metrics


def compute_duplicate_stats(df: pd.DataFrame) -> tuple[int, float]:
    n_rows = len(df)
    dup_count = int(df.duplicated().sum())
    dup_pct = round(dup_count / n_rows * 100, 2) if n_rows else 0.0
    return dup_count, dup_pct


# ─────────────────────────────────────────────────────────────
# STEP 2: DATATYPE INCONSISTENCY (new; no baseline required)
# ─────────────────────────────────────────────────────────────

def detect_datatype_issues(metrics: list[ColumnMetric]) -> list[Finding]:
    findings = []
    for m in metrics:
        if MIXED_TYPE_LOWER_BOUND < m.numeric_ratio < MIXED_TYPE_UPPER_BOUND:
            severity = "HIGH" if 0.3 < m.numeric_ratio < 0.7 else "MEDIUM"
            findings.append(
                Finding(
                    kind="datatype",
                    issue_type="DATATYPE_INCONSISTENCY",
                    column_name=m.column_name,
                    severity=severity,
                    message=(
                        f"Column '{m.column_name}' has mixed types: "
                        f"{round(m.numeric_ratio * 100, 1)}% of non-null values parse as numeric, "
                        f"the rest do not."
                    ),
                )
            )
    return findings


# ─────────────────────────────────────────────────────────────
# STEP 3: SCHEMA DRIFT (ported from detect_schema_drift, generalized
# from "vs. first file in folder" to "current vs. explicit baseline")
# ─────────────────────────────────────────────────────────────

def detect_schema_drift(
    current_metrics: list[ColumnMetric], baseline_metrics: list[ColumnMetric]
) -> list[Finding]:
    findings = []
    curr_cols = {m.column_name: m for m in current_metrics}
    base_cols = {m.column_name: m for m in baseline_metrics}

    for col in set(curr_cols) - set(base_cols):
        findings.append(
            Finding(
                kind="schema",
                issue_type="NEW_COLUMN",
                column_name=col,
                severity="MEDIUM",
                message=f"Column '{col}' is present in this dataset but not in the baseline.",
            )
        )

    for col in set(base_cols) - set(curr_cols):
        findings.append(
            Finding(
                kind="schema",
                issue_type="MISSING_COLUMN",
                column_name=col,
                severity="HIGH",
                message=f"Column '{col}' is present in the baseline but missing from this dataset.",
            )
        )

    for col in set(curr_cols) & set(base_cols):
        curr_dtype = curr_cols[col].inferred_dtype
        base_dtype = base_cols[col].inferred_dtype
        if curr_dtype != base_dtype:
            findings.append(
                Finding(
                    kind="schema",
                    issue_type="DTYPE_CHANGE",
                    column_name=col,
                    severity="HIGH",
                    message=(
                        f"Column '{col}' changed inferred type from "
                        f"'{base_dtype}' (baseline) to '{curr_dtype}' (current)."
                    ),
                )
            )
    return findings


# ─────────────────────────────────────────────────────────────
# STEP 4: ANOMALY / DRIFT DETECTION (ported from detect_anomalies)
# ─────────────────────────────────────────────────────────────

def detect_anomalies(
    current_metrics: list[ColumnMetric],
    baseline_metrics: list[ColumnMetric],
    current_dup_pct: float,
    baseline_dup_pct: float,
) -> list[Finding]:
    findings: list[Finding] = []
    base_by_col = {m.column_name: m for m in baseline_metrics}

    for curr in current_metrics:
        base = base_by_col.get(curr.column_name)
        if base is None:
            continue

        # NULL SPIKE
        delta_null = curr.null_percentage - base.null_percentage
        if delta_null > NULL_SPIKE_THRESHOLD_PP:
            findings.append(
                Finding(
                    kind="anomaly",
                    issue_type="NULL_SPIKE",
                    column_name=curr.column_name,
                    severity="HIGH" if delta_null > 40 else "MEDIUM",
                    prev_value=base.null_percentage,
                    curr_value=curr.null_percentage,
                    delta=round(delta_null, 2),
                    message=(
                        f"NULL spike in '{curr.column_name}': {base.null_percentage}% -> "
                        f"{curr.null_percentage}% (+{round(delta_null, 1)} pp)."
                    ),
                )
            )

        # MEAN DRIFT
        if curr.mean is not None and base.mean is not None and base.mean != 0:
            mean_change = abs(curr.mean - base.mean) / abs(base.mean)
            if mean_change > MEAN_DRIFT_THRESHOLD:
                findings.append(
                    Finding(
                        kind="anomaly",
                        issue_type="DISTRIBUTION_DRIFT_MEAN",
                        column_name=curr.column_name,
                        severity="HIGH" if mean_change > 0.5 else "MEDIUM",
                        prev_value=base.mean,
                        curr_value=curr.mean,
                        delta=round(mean_change * 100, 2),
                        message=(
                            f"Mean drift in '{curr.column_name}': {base.mean} -> {curr.mean} "
                            f"({round(mean_change * 100, 1)}% change)."
                        ),
                    )
                )

        # STD DRIFT (new: the original script defined this threshold but never checked it)
        if curr.std is not None and base.std is not None and base.std != 0:
            std_change = abs(curr.std - base.std) / abs(base.std)
            if std_change > STD_DRIFT_THRESHOLD:
                findings.append(
                    Finding(
                        kind="anomaly",
                        issue_type="DISTRIBUTION_DRIFT_STD",
                        column_name=curr.column_name,
                        severity="HIGH" if std_change > 0.7 else "MEDIUM",
                        prev_value=base.std,
                        curr_value=curr.std,
                        delta=round(std_change * 100, 2),
                        message=(
                            f"Spread (std dev) drift in '{curr.column_name}': "
                            f"{base.std} -> {curr.std} ({round(std_change * 100, 1)}% change)."
                        ),
                    )
                )

    # DUPLICATE SPIKE (dataset-level, not per-column)
    if current_dup_pct > DUP_SPIKE_THRESHOLD_PCT and current_dup_pct > baseline_dup_pct:
        findings.append(
            Finding(
                kind="anomaly",
                issue_type="DUPLICATE_SPIKE",
                column_name="ALL",
                severity="HIGH",
                prev_value=baseline_dup_pct,
                curr_value=current_dup_pct,
                delta=round(current_dup_pct - baseline_dup_pct, 2),
                message=f"Duplicate rate rose from {baseline_dup_pct}% to {current_dup_pct}%.",
            )
        )
    return findings


# ─────────────────────────────────────────────────────────────
# STEP 5: HEALTH SCORE (ported from compute_health_scores, documented)
# ─────────────────────────────────────────────────────────────
#
# Health Score = 100
#     - (avg_null_pct * 1.5)
#     - (duplicate_pct * 2)
#     - schema penalty:   10 per HIGH  +  5 per MEDIUM  schema finding
#     - anomaly penalty:   8 per HIGH  +  4 per MEDIUM  anomaly finding
#     - datatype penalty:  6 per HIGH  +  3 per MEDIUM  datatype finding  (new)
#
# Grade:  A >= 90 | B >= 75 | C >= 60 | D >= 40 | F < 40
#
# This is a deterministic, reproducible weighted-penalty score -- not a
# learned/ML score -- so the same input always produces the same result
# and the reasoning behind any given score is fully inspectable.

SCHEMA_WEIGHTS = {"HIGH": 10, "MEDIUM": 5}
ANOMALY_WEIGHTS = {"HIGH": 8, "MEDIUM": 4}
DATATYPE_WEIGHTS = {"HIGH": 6, "MEDIUM": 3}


def compute_health_score(
    avg_null_pct: float,
    duplicate_pct: float,
    findings: list[Finding],
) -> tuple[float, str]:
    schema_penalty = sum(SCHEMA_WEIGHTS.get(f.severity, 0) for f in findings if f.kind == "schema")
    anomaly_penalty = sum(ANOMALY_WEIGHTS.get(f.severity, 0) for f in findings if f.kind == "anomaly")
    datatype_penalty = sum(DATATYPE_WEIGHTS.get(f.severity, 0) for f in findings if f.kind == "datatype")

    raw_score = (
        100
        - (avg_null_pct * 1.5)
        - (duplicate_pct * 2)
        - schema_penalty
        - anomaly_penalty
        - datatype_penalty
    )
    final_score = max(0.0, round(raw_score, 1))
    if final_score >= 90:
        grade = "A"
    elif final_score >= 75:
        grade = "B"
    elif final_score >= 60:
        grade = "C"
    elif final_score >= 40:
        grade = "D"
    else:
        grade = "F"
    return final_score, grade


# ─────────────────────────────────────────────────────────────
# ORCHESTRATION
# ─────────────────────────────────────────────────────────────

def analyze(
    current_df: pd.DataFrame,
    dataset_name: str,
    baseline_df: Optional[pd.DataFrame] = None,
    baseline_name: Optional[str] = None,
) -> QualityResult:
    """Run the full check suite on `current_df`, optionally against `baseline_df`."""
    row_count, column_count = current_df.shape
    dup_count, dup_pct = compute_duplicate_stats(current_df)
    current_metrics = compute_column_metrics(current_df)

    findings: list[Finding] = []
    findings.extend(detect_datatype_issues(current_metrics))

    has_baseline = baseline_df is not None
    if has_baseline:
        baseline_metrics = compute_column_metrics(baseline_df)
        _, baseline_dup_pct = compute_duplicate_stats(baseline_df)
        findings.extend(detect_schema_drift(current_metrics, baseline_metrics))
        findings.extend(
            detect_anomalies(current_metrics, baseline_metrics, dup_pct, baseline_dup_pct)
        )

    avg_null = float(np.mean([m.null_percentage for m in current_metrics])) if current_metrics else 0.0
    score, grade = compute_health_score(avg_null, dup_pct, findings)

    return QualityResult(
        dataset_name=dataset_name,
        row_count=row_count,
        column_count=column_count,
        duplicate_count=dup_count,
        duplicate_percentage=dup_pct,
        has_baseline=has_baseline,
        baseline_name=baseline_name if has_baseline else None,
        column_metrics=current_metrics,
        findings=findings,
        health_score=score,
        grade=grade,
    )

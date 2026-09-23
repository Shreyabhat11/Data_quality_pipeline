"""
Regenerates the four report CSVs (quality report, schema issues, anomaly
flags, health score summary) from a stored run's metrics/findings, on
demand. Nothing is kept from the original upload -- reports are rebuilt
purely from what's in the database, matching the "don't store large raw
CSVs" requirement.
"""
from __future__ import annotations

import json
import os

import pandas as pd

from app.core.config import settings
from app.db.models import Run

REPORT_KINDS = ("quality", "schema", "anomaly", "health")


def _run_dir(run_id: str) -> str:
    path = os.path.join(settings.REPORTS_DIR, run_id)
    os.makedirs(path, exist_ok=True)
    return path


def generate_report(run: Run, kind: str) -> str:
    """Write the requested report CSV to disk and return its path."""
    if kind not in REPORT_KINDS:
        raise ValueError(f"Unknown report kind '{kind}'. Expected one of {REPORT_KINDS}.")

    out_dir = _run_dir(run.id)
    path = os.path.join(out_dir, f"{kind}_report.csv")

    if kind == "quality":
        metrics = json.loads(run.column_metrics_json)
        pd.DataFrame(metrics).to_csv(path, index=False)

    elif kind == "schema":
        rows = [
            {
                "column_name": f.column_name,
                "issue_type": f.issue_type,
                "severity": f.severity,
                "message": f.message,
            }
            for f in run.findings
            if f.kind == "schema"
        ]
        pd.DataFrame(rows, columns=["column_name", "issue_type", "severity", "message"]).to_csv(
            path, index=False
        )

    elif kind == "anomaly":
        rows = [
            {
                "column_name": f.column_name,
                "issue_type": f.issue_type,
                "severity": f.severity,
                "prev_value": f.prev_value,
                "curr_value": f.curr_value,
                "delta": f.delta,
                "message": f.message,
            }
            for f in run.findings
            if f.kind in ("anomaly", "datatype")
        ]
        pd.DataFrame(
            rows,
            columns=["column_name", "issue_type", "severity", "prev_value", "curr_value", "delta", "message"],
        ).to_csv(path, index=False)

    elif kind == "health":
        pd.DataFrame(
            [
                {
                    "run_id": run.id,
                    "dataset_name": run.dataset_name,
                    "baseline_name": run.baseline_name,
                    "created_at": run.created_at,
                    "row_count": run.row_count,
                    "column_count": run.column_count,
                    "duplicate_percentage": run.duplicate_percentage,
                    "health_score": run.health_score,
                    "grade": run.grade,
                    "issue_count": run.issue_count,
                    "anomaly_count": run.anomaly_count,
                }
            ]
        ).to_csv(path, index=False)

    return path

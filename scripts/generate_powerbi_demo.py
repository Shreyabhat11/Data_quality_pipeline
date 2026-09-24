from __future__ import annotations

import csv
import json
import random
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
BACKEND = ROOT / "backend"
sys.path.insert(0, str(BACKEND))

from app.db.database import SessionLocal
from app.db.models import FindingRecord, Run
from app.quality.engine import analyze


random.seed(42)
np.random.seed(42)

POWERBI_DIR = ROOT / "powerbi"
POWERBI_DIR.mkdir(exist_ok=True)

N_ROWS = 180


def make_baseline() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "order_id": [f"ORD-{i:04d}" for i in range(1, N_ROWS + 1)],
            "customer_id": [f"CUST-{random.randint(100, 160)}" for _ in range(N_ROWS)],
            "order_amount": np.random.normal(2500, 450, N_ROWS).round(2),
            "quantity": np.random.randint(1, 8, N_ROWS),
            "region": np.random.choice(
                ["South", "North", "West", "East"], N_ROWS
            ),
            "discount_code": np.random.choice(
                ["SAVE10", "NEW20", "FESTIVE", None],
                N_ROWS,
                p=[0.25, 0.20, 0.15, 0.40],
            ),
        }
    ).astype(object)


BASELINE = make_baseline()


def build_dataset(index: int) -> tuple[pd.DataFrame, str]:
    df = BASELINE.copy(deep=True)

    scenario = index % 10

    if scenario == 0:
        label = "Healthy"

    elif scenario == 1:
        label = "Minor Missingness"
        rows = np.random.choice(df.index, 18, replace=False)
        df.loc[rows, "customer_id"] = None

    elif scenario == 2:
        label = "Null Spike"
        rows = np.random.choice(df.index, 70, replace=False)
        df.loc[rows, "customer_id"] = None

    elif scenario == 3:
        label = "Mean Drift"
        df["order_amount"] = (
            pd.to_numeric(df["order_amount"]) * 1.55
        ).round(2)

    elif scenario == 4:
        label = "Variance Drift"
        values = pd.to_numeric(df["order_amount"])
        mean = values.mean()
        df["order_amount"] = (
            mean + (values - mean) * 2.2
        ).round(2)

    elif scenario == 5:
        label = "Datatype Issue"
        rows = np.random.choice(df.index, 20, replace=False)
        df.loc[rows, "order_amount"] = "INVALID"

    elif scenario == 6:
        label = "Schema Drift"
        df = df.drop(columns=["quantity"])
        df["sales_channel"] = np.random.choice(
            ["Web", "Mobile", "Store"], len(df)
        )

    elif scenario == 7:
        label = "Duplicate Spike"
        duplicate_rows = df.iloc[:35].copy()
        df = pd.concat([df, duplicate_rows], ignore_index=True)

    elif scenario == 8:
        label = "Multiple Issues"

        missing_rows = np.random.choice(df.index, 80, replace=False)
        df.loc[missing_rows, "customer_id"] = None

        df["order_amount"] = (
            pd.to_numeric(df["order_amount"]) * 1.7
        ).round(2)

        df = df.drop(columns=["quantity"])

    else:
        label = "Severe Quality Drop"

        missing_rows = np.random.choice(df.index, 110, replace=False)
        df.loc[missing_rows, "customer_id"] = None

        discount_rows = np.random.choice(df.index, 120, replace=False)
        df.loc[discount_rows, "discount_code"] = None

        df["order_amount"] = (
            pd.to_numeric(df["order_amount"]) * 2.0
        ).round(2)

        df = df.drop(columns=["quantity"])

        duplicate_rows = df.iloc[:50].copy()
        df = pd.concat([df, duplicate_rows], ignore_index=True)

    return df, label


def persist_demo_history() -> None:
    db = SessionLocal()

    try:
        # Only clear existing demo data if the DB is currently empty or being
        # intentionally regenerated for the portfolio dashboard.
        db.query(FindingRecord).delete()
        db.query(Run).delete()
        db.commit()

        start_date = datetime.now(timezone.utc) - timedelta(days=58)

        for i in range(30):
            current_df, scenario = build_dataset(i)

            result = analyze(
                current_df=current_df,
                dataset_name=f"orders_batch_{i + 1:02d}.csv",
                baseline_df=BASELINE,
                baseline_name="orders_baseline.csv",
            )

            run = Run(
                dataset_name=result.dataset_name,
                baseline_name=result.baseline_name,
                has_baseline=result.has_baseline,
                created_at=start_date + timedelta(days=i * 2),
                row_count=result.row_count,
                column_count=result.column_count,
                duplicate_count=result.duplicate_count,
                duplicate_percentage=result.duplicate_percentage,
                health_score=result.health_score,
                grade=result.grade,
                score_breakdown_json=json.dumps(result.score_breakdown),
                issue_count=len(result.schema_findings),
                anomaly_count=len(result.anomaly_findings),
                schema_issue_count=len(result.schema_findings),
                datatype_issue_count=len(result.datatype_findings),
                status="COMPLETED",
                column_metrics_json=json.dumps(
                    [metric.to_dict() for metric in result.column_metrics]
                ),
            )

            # Useful only for our Power BI demo export.
            run.dataset_name = f"{result.dataset_name} | {scenario}"

            db.add(run)
            db.flush()

            for finding in result.findings:
                db.add(
                    FindingRecord(
                        run_id=run.id,
                        kind=finding.kind,
                        issue_type=finding.issue_type,
                        column_name=finding.column_name,
                        severity=finding.severity,
                        message=finding.message,
                        prev_value=finding.prev_value,
                        curr_value=finding.curr_value,
                        delta=finding.delta,
                    )
                )

            db.commit()

        print("Generated 30 demo validation runs.")

    finally:
        db.close()


def export_powerbi_tables() -> None:
    db = SessionLocal()

    try:
        runs = db.query(Run).order_by(Run.created_at).all()

        runs_rows = []
        columns_rows = []

        for run in runs:
            breakdown = json.loads(run.score_breakdown_json or "{}")

            runs_rows.append(
                {
                    "run_id": run.id,
                    "created_at": run.created_at.isoformat(),
                    "dataset_name": run.dataset_name,
                    "baseline_name": run.baseline_name,
                    "has_baseline": run.has_baseline,
                    "row_count": run.row_count,
                    "column_count": run.column_count,
                    "duplicate_count": run.duplicate_count,
                    "duplicate_percentage": run.duplicate_percentage,
                    "health_score": run.health_score,
                    "grade": run.grade,
                    "schema_issue_count": run.schema_issue_count,
                    "datatype_issue_count": run.datatype_issue_count,
                    "anomaly_count": run.anomaly_count,
                    "null_penalty": breakdown.get("null_penalty", 0),
                    "duplicate_penalty": breakdown.get("duplicate_penalty", 0),
                    "schema_penalty": breakdown.get("schema_penalty", 0),
                    "datatype_penalty": breakdown.get("datatype_penalty", 0),
                    "anomaly_penalty": breakdown.get("anomaly_penalty", 0),
                    "total_penalty": breakdown.get("total_penalty", 0),
                    "status": run.status,
                }
            )

            metrics = json.loads(run.column_metrics_json or "[]")

            for metric in metrics:
                columns_rows.append(
                    {
                        "run_id": run.id,
                        "created_at": run.created_at.isoformat(),
                        "dataset_name": run.dataset_name,
                        **metric,
                    }
                )

        findings = db.query(FindingRecord).all()

        finding_rows = [
            {
                "finding_id": finding.id,
                "run_id": finding.run_id,
                "kind": finding.kind,
                "issue_type": finding.issue_type,
                "column_name": finding.column_name,
                "severity": finding.severity,
                "message": finding.message,
                "prev_value": finding.prev_value,
                "curr_value": finding.curr_value,
                "delta": finding.delta,
            }
            for finding in findings
        ]

        pd.DataFrame(runs_rows).to_csv(
            POWERBI_DIR / "runs.csv", index=False
        )
        pd.DataFrame(finding_rows).to_csv(
            POWERBI_DIR / "findings.csv", index=False
        )
        pd.DataFrame(columns_rows).to_csv(
            POWERBI_DIR / "column_metrics.csv", index=False
        )

        print(f"Exported {len(runs_rows)} runs")
        print(f"Exported {len(finding_rows)} findings")
        print(f"Exported {len(columns_rows)} column metrics")
        print(f"Files written to: {POWERBI_DIR}")

    finally:
        db.close()


if __name__ == "__main__":
    persist_demo_history()
    export_powerbi_tables()
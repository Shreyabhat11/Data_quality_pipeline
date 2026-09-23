"""
Service layer sitting between the API routes and (a) the quality engine
and (b) the database. Keeps routes thin and keeps ORM <-> schema mapping
in one place.
"""
from __future__ import annotations

import json
from typing import Optional

import pandas as pd
from sqlalchemy.orm import Session

from app.core.logging_config import logger
from app.db.models import FindingRecord, Run
from app.quality.engine import QualityResult, analyze


def run_validation(
    db: Session,
    current_df: pd.DataFrame,
    dataset_name: str,
    baseline_df: Optional[pd.DataFrame],
    baseline_name: Optional[str],
) -> Run:
    logger.info(
        "validation started dataset=%s rows=%s cols=%s has_baseline=%s",
        dataset_name,
        current_df.shape[0],
        current_df.shape[1],
        baseline_df is not None,
    )

    result: QualityResult = analyze(
        current_df=current_df,
        dataset_name=dataset_name,
        baseline_df=baseline_df,
        baseline_name=baseline_name,
    )

    run = Run(
        dataset_name=result.dataset_name,
        baseline_name=result.baseline_name,
        has_baseline=result.has_baseline,
        row_count=result.row_count,
        column_count=result.column_count,
        duplicate_count=result.duplicate_count,
        duplicate_percentage=result.duplicate_percentage,
        health_score=result.health_score,
        grade=result.grade,
        issue_count=len(result.schema_findings),
        anomaly_count=len(result.anomaly_findings),
        schema_issue_count=len(result.schema_findings),
        datatype_issue_count=len(result.datatype_findings),
        status="COMPLETED",
        column_metrics_json=json.dumps([m.to_dict() for m in result.column_metrics]),
    )
    db.add(run)
    db.flush()  # assigns run.id

    for f in result.findings:
        db.add(
            FindingRecord(
                run_id=run.id,
                kind=f.kind,
                issue_type=f.issue_type,
                column_name=f.column_name,
                severity=f.severity,
                message=f.message,
                prev_value=f.prev_value,
                curr_value=f.curr_value,
                delta=f.delta,
            )
        )
    db.commit()
    db.refresh(run)

    logger.info(
        "validation completed dataset=%s run_id=%s status=%s score=%s issues=%s anomalies=%s",
        dataset_name,
        run.id,
        run.status,
        run.health_score,
        run.issue_count,
        run.anomaly_count,
    )
    return run


def run_to_validate_response(run: Run) -> dict:
    checks = json.loads(run.column_metrics_json)
    issues = [
        {
            "kind": f.kind,
            "issue_type": f.issue_type,
            "column_name": f.column_name,
            "severity": f.severity,
            "message": f.message,
            "prev_value": f.prev_value,
            "curr_value": f.curr_value,
            "delta": f.delta,
        }
        for f in run.findings
    ]
    return {
        "run_id": run.id,
        "dataset_name": run.dataset_name,
        "baseline_name": run.baseline_name,
        "has_baseline": run.has_baseline,
        "row_count": run.row_count,
        "column_count": run.column_count,
        "health_score": run.health_score,
        "grade": run.grade,
        "issue_count": run.issue_count,
        "anomaly_count": run.anomaly_count,
        "schema_issue_count": run.schema_issue_count,
        "datatype_issue_count": run.datatype_issue_count,
        "duplicate_count": run.duplicate_count,
        "duplicate_percentage": run.duplicate_percentage,
        "checks": checks,
        "issues": issues,
        "created_at": run.created_at,
        "status": run.status,
    }


def run_to_summary(run: Run) -> dict:
    return {
        "run_id": run.id,
        "dataset_name": run.dataset_name,
        "created_at": run.created_at,
        "health_score": run.health_score,
        "grade": run.grade,
        "issue_count": run.issue_count,
        "anomaly_count": run.anomaly_count,
        "status": run.status,
    }


def list_runs(db: Session, limit: int = 50, offset: int = 0) -> tuple[int, list[Run]]:
    total = db.query(Run).count()
    runs = (
        db.query(Run)
        .order_by(Run.created_at.desc())
        .offset(offset)
        .limit(limit)
        .all()
    )
    return total, runs


def get_run(db: Session, run_id: str) -> Optional[Run]:
    return db.query(Run).filter(Run.id == run_id).first()

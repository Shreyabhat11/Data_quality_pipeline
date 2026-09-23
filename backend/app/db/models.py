"""
Database models.

Design decision: raw uploaded CSVs are never persisted to the database or
kept on disk long-term (see spec: "Do not store unnecessarily large raw
CSV files in PostgreSQL"). Only run metadata, per-column metrics, and
findings (issues/anomalies) are stored -- everything needed to redraw the
dashboard, the run-history list, and to regenerate report CSVs on demand.
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone

from sqlalchemy import DateTime, Float, ForeignKey, Integer, String, Text, Boolean
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.database import Base


def _uuid() -> str:
    return str(uuid.uuid4())


def _now() -> datetime:
    return datetime.now(timezone.utc)


class Run(Base):
    __tablename__ = "runs"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    dataset_name: Mapped[str] = mapped_column(String(255))
    baseline_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    has_baseline: Mapped[bool] = mapped_column(Boolean, default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)

    row_count: Mapped[int] = mapped_column(Integer)
    column_count: Mapped[int] = mapped_column(Integer)
    duplicate_count: Mapped[int] = mapped_column(Integer)
    duplicate_percentage: Mapped[float] = mapped_column(Float)

    health_score: Mapped[float] = mapped_column(Float)
    grade: Mapped[str] = mapped_column(String(1))

    issue_count: Mapped[int] = mapped_column(Integer, default=0)          # schema findings
    anomaly_count: Mapped[int] = mapped_column(Integer, default=0)        # anomaly findings
    schema_issue_count: Mapped[int] = mapped_column(Integer, default=0)   # alias kept for API parity
    datatype_issue_count: Mapped[int] = mapped_column(Integer, default=0)

    status: Mapped[str] = mapped_column(String(20), default="COMPLETED")  # COMPLETED | FAILED

    # JSON-encoded list of per-column metrics (see schemas.validation.ColumnMetricOut)
    column_metrics_json: Mapped[str] = mapped_column(Text, default="[]")

    findings: Mapped[list["FindingRecord"]] = relationship(
        back_populates="run", cascade="all, delete-orphan"
    )


class FindingRecord(Base):
    __tablename__ = "findings"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[str] = mapped_column(ForeignKey("runs.id"))
    kind: Mapped[str] = mapped_column(String(20))          # schema | anomaly | datatype
    issue_type: Mapped[str] = mapped_column(String(50))
    column_name: Mapped[str] = mapped_column(String(255))
    severity: Mapped[str] = mapped_column(String(10))
    message: Mapped[str] = mapped_column(Text)
    prev_value: Mapped[float | None] = mapped_column(Float, nullable=True)
    curr_value: Mapped[float | None] = mapped_column(Float, nullable=True)
    delta: Mapped[float | None] = mapped_column(Float, nullable=True)

    run: Mapped["Run"] = relationship(back_populates="findings")

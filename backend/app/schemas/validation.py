"""Pydantic response/request schemas for the API."""
from __future__ import annotations

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, ConfigDict


class ColumnMetricOut(BaseModel):
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


class FindingOut(BaseModel):
    kind: str
    issue_type: str
    column_name: str
    severity: str
    message: str
    prev_value: Optional[float] = None
    curr_value: Optional[float] = None
    delta: Optional[float] = None


class ValidateResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    run_id: str
    dataset_name: str
    baseline_name: Optional[str] = None
    has_baseline: bool
    row_count: int
    column_count: int
    health_score: float
    grade: str
    issue_count: int
    anomaly_count: int
    schema_issue_count: int
    datatype_issue_count: int
    duplicate_count: int
    duplicate_percentage: float
    checks: list[ColumnMetricOut]
    issues: list[FindingOut]


class RunSummaryOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    run_id: str
    dataset_name: str
    created_at: datetime
    health_score: float
    grade: str
    issue_count: int
    anomaly_count: int
    status: str


class RunListOut(BaseModel):
    total: int
    runs: list[RunSummaryOut]


class RunDetailOut(ValidateResponse):
    created_at: datetime
    status: str


class ErrorResponse(BaseModel):
    detail: str

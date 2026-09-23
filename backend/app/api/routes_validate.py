from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends, File, HTTPException, UploadFile, status
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from app.core.config import settings
from app.core.logging_config import logger
from app.db.database import get_db
from app.quality.engine import QualityEngineError, read_csv_bytes
from app.schemas.validation import ValidateResponse
from app.services.validation_service import run_to_validate_response, run_validation

router = APIRouter()

ALLOWED_CONTENT_TYPES = {"text/csv", "application/vnd.ms-excel", "application/csv", "text/plain"}


async def _read_upload(upload: UploadFile) -> bytes:
    if not upload.filename or not upload.filename.lower().endswith(".csv"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"'{upload.filename}' must be a .csv file.",
        )
    raw = await upload.read()
    if len(raw) > settings.MAX_UPLOAD_SIZE:
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail=(
                f"'{upload.filename}' is {len(raw)} bytes, exceeding the "
                f"{settings.MAX_UPLOAD_SIZE}-byte upload limit."
            ),
        )
    return raw


@router.post("/validate", response_model=ValidateResponse)
async def validate_dataset(
    file: UploadFile = File(..., description="The CSV dataset to validate."),
    baseline: Optional[UploadFile] = File(
        None, description="Optional reference/baseline CSV to compare against."
    ),
    db: Session = Depends(get_db),
):
    raw = await _read_upload(file)
    baseline_raw = await _read_upload(baseline) if baseline is not None else None

    try:
        current_df = read_csv_bytes(raw, file.filename)
        baseline_df = (
            read_csv_bytes(baseline_raw, baseline.filename) if baseline_raw is not None else None
        )
    except QualityEngineError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

    try:
        run = run_validation(
            db=db,
            current_df=current_df,
            dataset_name=file.filename,
            baseline_df=baseline_df,
            baseline_name=baseline.filename if baseline is not None else None,
        )
    except SQLAlchemyError as exc:
        logger.exception("database error while saving validation run")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Database is unavailable. The dataset was not saved.",
        ) from exc
    except Exception as exc:  # noqa: BLE001 - convert any unexpected engine failure to a clean 500
        logger.exception("unexpected error during validation")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An unexpected error occurred while validating the dataset.",
        ) from exc

    return run_to_validate_response(run)

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query, status
from fastapi.responses import FileResponse
from sqlalchemy.orm import Session

from app.db.database import get_db
from app.services.report_service import REPORT_KINDS, generate_report
from app.services.validation_service import get_run

router = APIRouter()


@router.get("/reports/{run_id}")
def get_report(
    run_id: str,
    type: str = Query("quality", description=f"One of: {', '.join(REPORT_KINDS)}"),
    db: Session = Depends(get_db),
):
    run = get_run(db, run_id)
    if run is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Run '{run_id}' not found.")

    if type not in REPORT_KINDS:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Unknown report type '{type}'. Expected one of {REPORT_KINDS}.",
        )

    try:
        path = generate_report(run, type)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to generate the requested report.",
        ) from exc

    filename = f"{run.dataset_name.rsplit('.', 1)[0]}_{type}_report.csv"
    return FileResponse(path, media_type="text/csv", filename=filename)

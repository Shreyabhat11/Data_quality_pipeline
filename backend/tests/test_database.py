from app.db.models import FindingRecord, Run
from app.services.validation_service import get_run, list_runs, run_validation
from tests.conftest import GOOD_CSV
from app.quality.engine import read_csv_bytes


def test_run_validation_persists_run_and_findings(db_session):
    df = read_csv_bytes(GOOD_CSV.encode("utf-8"), "orders.csv")
    run = run_validation(
        db=db_session,
        current_df=df,
        dataset_name="orders.csv",
        baseline_df=None,
        baseline_name=None,
    )
    assert run.id is not None
    assert run.status == "COMPLETED"

    fetched = get_run(db_session, run.id)
    assert fetched is not None
    assert fetched.dataset_name == "orders.csv"


def test_list_runs_orders_by_most_recent(db_session):
    df = read_csv_bytes(GOOD_CSV.encode("utf-8"), "orders.csv")
    run1 = run_validation(db_session, df, "first.csv", None, None)
    run2 = run_validation(db_session, df, "second.csv", None, None)

    total, runs = list_runs(db_session)
    assert total == 2
    assert runs[0].id == run2.id  # most recent first


def test_findings_cascade_delete_with_run(db_session):
    bad_csv = "a,b\n1,x\n1,x\n2,y\n"  # duplicate row, but no baseline so no findings expected here
    df = read_csv_bytes(bad_csv.encode("utf-8"), "dup.csv")
    run = run_validation(db_session, df, "dup.csv", None, None)

    db_session.delete(run)
    db_session.commit()

    remaining = db_session.query(FindingRecord).filter(FindingRecord.run_id == run.id).all()
    assert remaining == []

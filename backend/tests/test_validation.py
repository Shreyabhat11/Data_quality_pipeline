from tests.conftest import BASELINE_CSV, DRIFTED_CSV, GOOD_CSV, make_csv_file


def test_validate_clean_dataset_no_baseline(client):
    resp = client.post("/validate", files={"file": make_csv_file(GOOD_CSV, "orders.csv")})
    assert resp.status_code == 200
    body = resp.json()
    assert body["dataset_name"] == "orders.csv"
    assert body["row_count"] == 4
    assert body["column_count"] == 4
    assert body["has_baseline"] is False
    assert body["health_score"] == 100.0
    assert body["grade"] == "A"
    assert body["issues"] == []


def test_validate_with_baseline_detects_drift(client):
    resp = client.post(
        "/validate",
        files={
            "file": make_csv_file(DRIFTED_CSV, "drifted.csv"),
            "baseline": make_csv_file(BASELINE_CSV, "baseline.csv"),
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["has_baseline"] is True
    assert body["schema_issue_count"] >= 1  # discount_code new, region missing
    assert body["anomaly_count"] >= 1       # null spike on customer_id
    assert body["health_score"] < 100.0
    issue_types = {i["issue_type"] for i in body["issues"]}
    assert "NEW_COLUMN" in issue_types
    assert "MISSING_COLUMN" in issue_types


def test_validate_rejects_non_csv_extension(client):
    resp = client.post("/validate", files={"file": ("data.txt", b"a,b\n1,2", "text/plain")})
    assert resp.status_code == 400


def test_validate_rejects_empty_file(client):
    resp = client.post("/validate", files={"file": ("empty.csv", b"", "text/csv")})
    assert resp.status_code == 400


def test_validate_rejects_malformed_csv(client):
    bad = b"a,b\n1,2,3\n4,5"
    resp = client.post("/validate", files={"file": ("bad.csv", bad, "text/csv")})
    assert resp.status_code == 400


def test_validate_rejects_oversized_upload(client, monkeypatch):
    from app.core import config

    monkeypatch.setattr(config.settings, "MAX_UPLOAD_SIZE", 10)
    resp = client.post("/validate", files={"file": make_csv_file(GOOD_CSV, "orders.csv")})
    assert resp.status_code == 413


def test_run_appears_in_history_after_validation(client):
    resp = client.post("/validate", files={"file": make_csv_file(GOOD_CSV, "orders.csv")})
    run_id = resp.json()["run_id"]

    runs_resp = client.get("/runs")
    assert runs_resp.status_code == 200
    ids = [r["run_id"] for r in runs_resp.json()["runs"]]
    assert run_id in ids


def test_get_run_detail(client):
    resp = client.post("/validate", files={"file": make_csv_file(GOOD_CSV, "orders.csv")})
    run_id = resp.json()["run_id"]

    detail_resp = client.get(f"/runs/{run_id}")
    assert detail_resp.status_code == 200
    assert detail_resp.json()["run_id"] == run_id


def test_get_run_detail_not_found(client):
    resp = client.get("/runs/does-not-exist")
    assert resp.status_code == 404


def test_report_download(client):
    resp = client.post("/validate", files={"file": make_csv_file(GOOD_CSV, "orders.csv")})
    run_id = resp.json()["run_id"]

    report_resp = client.get(f"/reports/{run_id}", params={"type": "quality"})
    assert report_resp.status_code == 200
    assert report_resp.headers["content-type"].startswith("text/csv")


def test_report_unknown_type(client):
    resp = client.post("/validate", files={"file": make_csv_file(GOOD_CSV, "orders.csv")})
    run_id = resp.json()["run_id"]

    report_resp = client.get(f"/reports/{run_id}", params={"type": "not_a_real_type"})
    assert report_resp.status_code == 400

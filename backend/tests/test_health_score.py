from app.quality.engine import Finding, compute_health_score


def test_perfect_dataset_scores_100_grade_a():
    score, grade, breakdown = compute_health_score(
        avg_null_pct=0.0,
        duplicate_pct=0.0,
        findings=[],
    )

    assert score == 100.0
    assert grade == "A"
    assert breakdown == {
        "null_penalty": 0.0,
        "duplicate_penalty": 0.0,
        "schema_penalty": 0.0,
        "anomaly_penalty": 0.0,
        "datatype_penalty": 0.0,
        "total_penalty": 0.0,
    }


def test_high_nulls_and_duplicates_lower_score():
    score, grade, breakdown = compute_health_score(
        avg_null_pct=20.0,
        duplicate_pct=10.0,
        findings=[],
    )

    # 100 - 20*1.5 - 10*2 = 50
    assert score == 50.0
    assert grade == "D"

    assert breakdown["null_penalty"] == 30.0
    assert breakdown["duplicate_penalty"] == 20.0
    assert breakdown["schema_penalty"] == 0.0
    assert breakdown["anomaly_penalty"] == 0.0
    assert breakdown["datatype_penalty"] == 0.0
    assert breakdown["total_penalty"] == 50.0


def test_schema_and_anomaly_findings_apply_penalties():
    findings = [
        Finding("schema", "MISSING_COLUMN", "x", "HIGH", "msg"),
        Finding("anomaly", "NULL_SPIKE", "y", "MEDIUM", "msg"),
    ]

    score, grade, breakdown = compute_health_score(
        avg_null_pct=0.0,
        duplicate_pct=0.0,
        findings=findings,
    )

    # 100 - 10 (schema HIGH) - 4 (anomaly MEDIUM) = 86
    assert score == 86.0
    assert grade == "B"

    assert breakdown["schema_penalty"] == 10.0
    assert breakdown["anomaly_penalty"] == 4.0
    assert breakdown["datatype_penalty"] == 0.0
    assert breakdown["total_penalty"] == 14.0


def test_score_never_goes_below_zero():
    findings = [
        Finding("schema", "MISSING_COLUMN", f"col{i}", "HIGH", "msg")
        for i in range(20)
    ]

    score, grade, breakdown = compute_health_score(
        avg_null_pct=100.0,
        duplicate_pct=100.0,
        findings=findings,
    )

    assert score == 0.0
    assert grade == "F"

    assert breakdown["null_penalty"] == 150.0
    assert breakdown["duplicate_penalty"] == 200.0
    assert breakdown["schema_penalty"] == 200.0
    assert breakdown["total_penalty"] == 550.0


def test_grade_boundaries():
    assert compute_health_score(0, 0, [])[1] == "A"      # 100
    assert compute_health_score(10, 0, [])[1] == "B"     # 85
    assert compute_health_score(20, 5, [])[1] == "C"     # 60
    assert compute_health_score(20, 10, [])[1] == "D"    # 50
    assert compute_health_score(50, 20, [])[1] == "F"    # clamped to 0
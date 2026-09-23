from app.quality.engine import Finding, compute_health_score


def test_perfect_dataset_scores_100_grade_a():
    score, grade = compute_health_score(avg_null_pct=0.0, duplicate_pct=0.0, findings=[])
    assert score == 100.0
    assert grade == "A"


def test_high_nulls_and_duplicates_lower_score():
    score, grade = compute_health_score(avg_null_pct=20.0, duplicate_pct=10.0, findings=[])
    # 100 - 20*1.5 - 10*2 = 100 - 30 - 20 = 50
    assert score == 50.0
    assert grade == "D"


def test_schema_and_anomaly_findings_apply_penalties():
    findings = [
        Finding("schema", "MISSING_COLUMN", "x", "HIGH", "msg"),
        Finding("anomaly", "NULL_SPIKE", "y", "MEDIUM", "msg"),
    ]
    score, grade = compute_health_score(avg_null_pct=0.0, duplicate_pct=0.0, findings=findings)
    # 100 - 10 (schema HIGH) - 4 (anomaly MEDIUM) = 86
    assert score == 86.0
    assert grade == "B"


def test_score_never_goes_below_zero():
    findings = [Finding("schema", "MISSING_COLUMN", f"col{i}", "HIGH", "msg") for i in range(20)]
    score, grade = compute_health_score(avg_null_pct=100.0, duplicate_pct=100.0, findings=findings)
    assert score == 0.0
    assert grade == "F"


def test_grade_boundaries():
    assert compute_health_score(0, 0, [])[1] == "A"      # 100
    assert compute_health_score(10, 0, [])[1] == "B"      # 100 - 15 = 85
    assert compute_health_score(20, 5, [])[1] == "C"      # 100 - 30 - 10 = 60
    assert compute_health_score(20, 10, [])[1] == "D"     # 100 - 30 - 20 = 50 -> D
    assert compute_health_score(50, 20, [])[1] == "F"     # 100 - 75 - 40 -> clamped to 0 -> F

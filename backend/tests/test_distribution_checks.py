import pandas as pd

from app.quality.engine import compute_column_metrics, detect_anomalies, detect_datatype_issues


def test_mean_drift_detected():
    baseline = compute_column_metrics(pd.DataFrame({"amount": ["100", "100", "100", "100"]}))
    current = compute_column_metrics(pd.DataFrame({"amount": ["200", "200", "200", "200"]}))
    findings = detect_anomalies(current, baseline, 0.0, 0.0)
    types = [f.issue_type for f in findings]
    assert "DISTRIBUTION_DRIFT_MEAN" in types


def test_mean_drift_not_flagged_for_small_change():
    baseline = compute_column_metrics(pd.DataFrame({"amount": ["100", "100", "100", "100"]}))
    current = compute_column_metrics(pd.DataFrame({"amount": ["105", "103", "104", "106"]}))
    findings = detect_anomalies(current, baseline, 0.0, 0.0)
    assert not [f for f in findings if f.issue_type == "DISTRIBUTION_DRIFT_MEAN"]


def test_std_drift_detected():
    baseline = compute_column_metrics(pd.DataFrame({"amount": ["10", "11", "9", "10"]}))
    current = compute_column_metrics(pd.DataFrame({"amount": ["1", "50", "5", "40"]}))
    findings = detect_anomalies(current, baseline, 0.0, 0.0)
    types = [f.issue_type for f in findings]
    assert "DISTRIBUTION_DRIFT_STD" in types


def test_datatype_inconsistency_flagged_for_mixed_column():
    df = pd.DataFrame({"amount": ["100", "abc", "150", "xyz", "120"]})
    metrics = compute_column_metrics(df)
    findings = detect_datatype_issues(metrics)
    assert len(findings) == 1
    assert findings[0].issue_type == "DATATYPE_INCONSISTENCY"


def test_datatype_inconsistency_not_flagged_for_clean_numeric_column():
    df = pd.DataFrame({"amount": ["100", "150", "120", "130"]})
    metrics = compute_column_metrics(df)
    findings = detect_datatype_issues(metrics)
    assert findings == []


def test_datatype_inconsistency_not_flagged_for_clean_text_column():
    df = pd.DataFrame({"region": ["North", "South", "East", "West"]})
    metrics = compute_column_metrics(df)
    findings = detect_datatype_issues(metrics)
    assert findings == []

import pandas as pd

from app.quality.engine import compute_column_metrics, detect_schema_drift


def test_new_column_detected():
    baseline = compute_column_metrics(pd.DataFrame({"a": ["1", "2"]}))
    current = compute_column_metrics(pd.DataFrame({"a": ["1", "2"], "b": ["x", "y"]}))
    findings = detect_schema_drift(current, baseline)
    types = [f.issue_type for f in findings]
    assert "NEW_COLUMN" in types


def test_missing_column_detected():
    baseline = compute_column_metrics(pd.DataFrame({"a": ["1", "2"], "b": ["x", "y"]}))
    current = compute_column_metrics(pd.DataFrame({"a": ["1", "2"]}))
    findings = detect_schema_drift(current, baseline)
    types = [f.issue_type for f in findings]
    assert "MISSING_COLUMN" in types
    missing = [f for f in findings if f.issue_type == "MISSING_COLUMN"][0]
    assert missing.severity == "HIGH"


def test_dtype_change_detected():
    baseline = compute_column_metrics(pd.DataFrame({"a": ["1", "2", "3", "4"]}))
    current = compute_column_metrics(pd.DataFrame({"a": ["x", "y", "z", "w"]}))
    findings = detect_schema_drift(current, baseline)
    types = [f.issue_type for f in findings]
    assert "DTYPE_CHANGE" in types


def test_no_schema_findings_when_identical():
    baseline = compute_column_metrics(pd.DataFrame({"a": ["1", "2"], "b": ["x", "y"]}))
    current = compute_column_metrics(pd.DataFrame({"a": ["3", "4"], "b": ["p", "q"]}))
    findings = detect_schema_drift(current, baseline)
    assert findings == []

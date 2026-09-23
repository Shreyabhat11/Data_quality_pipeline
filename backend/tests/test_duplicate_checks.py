import pandas as pd

from app.quality.engine import compute_duplicate_stats, detect_anomalies, compute_column_metrics


def test_duplicate_count_and_percentage():
    df = pd.DataFrame({"a": ["1", "1", "2", "3"], "b": ["x", "x", "y", "z"]})
    dup_count, dup_pct = compute_duplicate_stats(df)
    assert dup_count == 1
    assert dup_pct == 25.0


def test_no_duplicates():
    df = pd.DataFrame({"a": ["1", "2", "3"]})
    dup_count, dup_pct = compute_duplicate_stats(df)
    assert dup_count == 0
    assert dup_pct == 0.0


def test_duplicate_spike_flagged():
    metrics = compute_column_metrics(pd.DataFrame({"a": ["1", "2", "3"]}))
    findings = detect_anomalies(metrics, metrics, current_dup_pct=20.0, baseline_dup_pct=1.0)
    spikes = [f for f in findings if f.issue_type == "DUPLICATE_SPIKE"]
    assert len(spikes) == 1
    assert spikes[0].severity == "HIGH"


def test_duplicate_spike_not_flagged_when_below_threshold():
    metrics = compute_column_metrics(pd.DataFrame({"a": ["1", "2", "3"]}))
    findings = detect_anomalies(metrics, metrics, current_dup_pct=2.0, baseline_dup_pct=1.0)
    assert not [f for f in findings if f.issue_type == "DUPLICATE_SPIKE"]

import pandas as pd

from app.quality.engine import compute_column_metrics, detect_anomalies


def test_null_percentage_computed_correctly():
    df = pd.DataFrame({"a": ["1", None, "3", None]})
    metrics = compute_column_metrics(df)
    assert metrics[0].null_count == 2
    assert metrics[0].null_percentage == 50.0


def test_null_spike_detected_against_baseline():
    baseline_df = pd.DataFrame({"a": ["1", "2", "3", "4"]})
    current_df = pd.DataFrame({"a": [None, None, None, "4"]})

    baseline_metrics = compute_column_metrics(baseline_df)
    current_metrics = compute_column_metrics(current_df)

    findings = detect_anomalies(current_metrics, baseline_metrics, 0.0, 0.0)
    null_spikes = [f for f in findings if f.issue_type == "NULL_SPIKE"]
    assert len(null_spikes) == 1
    assert null_spikes[0].column_name == "a"
    assert null_spikes[0].curr_value == 75.0


def test_no_null_spike_below_threshold():
    baseline_df = pd.DataFrame({"a": ["1", "2", "3", "4", "5", "6", "7", "8", "9", "10"]})
    current_df = pd.DataFrame({"a": ["1", "2", "3", None, "5", "6", "7", "8", "9", "10"]})

    baseline_metrics = compute_column_metrics(baseline_df)
    current_metrics = compute_column_metrics(current_df)

    findings = detect_anomalies(current_metrics, baseline_metrics, 0.0, 0.0)
    assert not [f for f in findings if f.issue_type == "NULL_SPIKE"]

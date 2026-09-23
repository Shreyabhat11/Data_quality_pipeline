const GRADE_CLASS = { A: "grade-a", B: "grade-b", C: "grade-c", D: "grade-d", F: "grade-f" };

export default function HealthScoreCard({ result }) {
  if (!result) return null;
  return (
    <div className="card health-card">
      <div className={`grade-badge ${GRADE_CLASS[result.grade] || ""}`}>{result.grade}</div>
      <div>
        <h3>{result.health_score.toFixed(1)} / 100</h3>
        <p>{result.dataset_name}</p>
        <div className="stat-row">
          <span>{result.row_count} rows</span>
          <span>{result.column_count} columns</span>
          <span>{result.duplicate_percentage}% duplicates</span>
        </div>
        <div className="stat-row">
          <span>{result.schema_issue_count} schema issues</span>
          <span>{result.anomaly_count} anomalies</span>
          <span>{result.datatype_issue_count} datatype issues</span>
        </div>
        {result.has_baseline ? (
          <p className="baseline-note">Compared against baseline: {result.baseline_name}</p>
        ) : (
          <p className="baseline-note">No baseline supplied — drift checks skipped.</p>
        )}
      </div>
    </div>
  );
}

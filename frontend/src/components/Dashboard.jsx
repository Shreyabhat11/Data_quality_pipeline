export default function Dashboard({ runs }) {
  const latest = runs[0];
  const totalRuns = runs.length;
  const recentIssues = runs.slice(0, 5).reduce((sum, r) => sum + r.issue_count + r.anomaly_count, 0);

  return (
    <div className="dashboard-strip">
      <div className="card mini-stat">
        <span className="mini-label">Latest health score</span>
        <span className="mini-value">{latest ? latest.health_score.toFixed(1) : "—"}</span>
      </div>
      <div className="card mini-stat">
        <span className="mini-label">Total validation runs</span>
        <span className="mini-value">{totalRuns}</span>
      </div>
      <div className="card mini-stat">
        <span className="mini-label">Issues in last 5 runs</span>
        <span className="mini-value">{recentIssues}</span>
      </div>
    </div>
  );
}

export default function RunHistory({ runs, onOpenRun, onDownloadReport }) {
  return (
    <div className="card">
      <h3>Validation history</h3>
      {runs.length === 0 ? (
        <p className="hint">No validation runs yet.</p>
      ) : (
        <div className="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Dataset</th>
                <th>Date</th>
                <th>Score</th>
                <th>Grade</th>
                <th>Findings</th>
                <th>Status</th>
                <th></th>
              </tr>
            </thead>
            <tbody>
              {runs.map((r) => (
                <tr key={r.run_id}>
                  <td>{r.dataset_name}</td>
                  <td>{new Date(r.created_at).toLocaleString()}</td>
                  <td>{r.health_score.toFixed(1)}</td>
                  <td>{r.grade}</td>
                  <td>{r.issue_count + r.anomaly_count + (r.datatype_issue_count || 0)}</td>
                  <td>{r.status}</td>
                  <td className="row-actions">
                    <button onClick={() => onOpenRun(r.run_id)}>View</button>
                    <button onClick={() => onDownloadReport(r.run_id, "quality")}>
                      Report
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}

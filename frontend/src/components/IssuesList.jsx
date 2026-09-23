const SEVERITY_CLASS = { HIGH: "sev-high", MEDIUM: "sev-medium" };

function IssueRow({ issue }) {
  return (
    <li className={`issue-row ${SEVERITY_CLASS[issue.severity] || ""}`}>
      <div className="issue-header">
        <span className="issue-type">{issue.issue_type.replaceAll("_", " ")}</span>
        <span className="issue-column">{issue.column_name}</span>
        <span className="issue-severity">{issue.severity}</span>
      </div>
      <p>{issue.message}</p>
    </li>
  );
}

export default function IssuesList({ result }) {
  if (!result) return null;
  const { issues, checks } = result;

  return (
    <div className="card issues-card">
      <h3>Detected issues ({issues.length})</h3>
      {issues.length === 0 ? (
        <p className="hint">No issues detected.</p>
      ) : (
        <ul className="issue-list">
          {issues.map((issue, i) => (
            <IssueRow key={i} issue={issue} />
          ))}
        </ul>
      )}

      <h3>Per-column metrics</h3>
      <div className="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Column</th>
              <th>Type</th>
              <th>Null %</th>
              <th>Unique</th>
              <th>Mean</th>
              <th>Std</th>
            </tr>
          </thead>
          <tbody>
            {checks.map((c) => (
              <tr key={c.column_name}>
                <td>{c.column_name}</td>
                <td>{c.inferred_dtype}</td>
                <td>{c.null_percentage}%</td>
                <td>{c.unique_count}</td>
                <td>{c.mean ?? "—"}</td>
                <td>{c.std ?? "—"}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

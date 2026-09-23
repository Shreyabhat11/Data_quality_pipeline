import { useEffect, useState } from "react";
import Dashboard from "./components/Dashboard.jsx";
import UploadForm from "./components/UploadForm.jsx";
import HealthScoreCard from "./components/HealthScoreCard.jsx";
import IssuesList from "./components/IssuesList.jsx";
import RunHistory from "./components/RunHistory.jsx";
import { validateDataset, fetchRuns, fetchRunDetail, reportDownloadUrl } from "./api.js";

export default function App() {
  const [runs, setRuns] = useState([]);
  const [result, setResult] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [historyLoaded, setHistoryLoaded] = useState(false);

  async function loadHistory() {
    try {
      const data = await fetchRuns();
      setRuns(data.runs);
      setHistoryLoaded(true);
    } catch (err) {
      setError(err.message);
    }
  }

  useEffect(() => {
    loadHistory();
  }, []);

  async function handleValidate(file, baseline) {
    setLoading(true);
    setError(null);
    try {
      const data = await validateDataset(file, baseline);
      setResult(data);
      await loadHistory();
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  }

  async function handleOpenRun(runId) {
    setError(null);
    try {
      const data = await fetchRunDetail(runId);
      setResult(data);
      window.scrollTo({ top: 0, behavior: "smooth" });
    } catch (err) {
      setError(err.message);
    }
  }

  function handleDownloadReport(runId, type) {
    window.open(reportDownloadUrl(runId, type), "_blank");
  }

  return (
    <div className="app">
      <header>
        <h1>Data Quality Monitoring Platform</h1>
        <p>Upload a CSV, get a health score, catch schema drift and anomalies.</p>
      </header>

      {historyLoaded && <Dashboard runs={runs} />}

      {error && <div className="card error-banner">{error}</div>}

      <div className="grid">
        <UploadForm onValidate={handleValidate} loading={loading} />
        <HealthScoreCard result={result} />
      </div>

      <IssuesList result={result} />

      <RunHistory runs={runs} onOpenRun={handleOpenRun} onDownloadReport={handleDownloadReport} />
    </div>
  );
}

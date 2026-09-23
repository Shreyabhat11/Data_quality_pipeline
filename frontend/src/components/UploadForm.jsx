import { useState } from "react";

export default function UploadForm({ onValidate, loading }) {
  const [file, setFile] = useState(null);
  const [baseline, setBaseline] = useState(null);

  function handleSubmit(e) {
    e.preventDefault();
    if (!file) return;
    onValidate(file, baseline);
  }

  return (
    <form className="card upload-form" onSubmit={handleSubmit}>
      <h2>Run a validation</h2>

      <label className="field">
        <span>Dataset (required)</span>
        <input
          type="file"
          accept=".csv"
          onChange={(e) => setFile(e.target.files[0] || null)}
        />
      </label>

      <label className="field">
        <span>Baseline / reference dataset (optional)</span>
        <input
          type="file"
          accept=".csv"
          onChange={(e) => setBaseline(e.target.files[0] || null)}
        />
      </label>
      <p className="hint">
        Without a baseline, only null-rate, duplicate-rate, and datatype checks run.
        With a baseline, schema drift and distribution-drift checks run too.
      </p>

      <button type="submit" disabled={!file || loading}>
        {loading ? "Validating…" : "Run validation"}
      </button>
    </form>
  );
}

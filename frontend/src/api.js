const BASE_URL = import.meta.env.VITE_API_BASE_URL || "http://localhost:8000";

async function handleResponse(resp) {
  if (!resp.ok) {
    let detail = `Request failed with status ${resp.status}`;
    try {
      const body = await resp.json();
      if (body.detail) detail = body.detail;
    } catch {
      // response wasn't JSON; keep the generic message
    }
    throw new Error(detail);
  }
  return resp.json();
}

export async function checkHealth() {
  const resp = await fetch(`${BASE_URL}/health`);
  return handleResponse(resp);
}

export async function validateDataset(file, baselineFile) {
  const formData = new FormData();
  formData.append("file", file);
  if (baselineFile) formData.append("baseline", baselineFile);

  const resp = await fetch(`${BASE_URL}/validate`, {
    method: "POST",
    body: formData,
  });
  return handleResponse(resp);
}

export async function fetchRuns(limit = 50) {
  const resp = await fetch(`${BASE_URL}/runs?limit=${limit}`);
  return handleResponse(resp);
}

export async function fetchRunDetail(runId) {
  const resp = await fetch(`${BASE_URL}/runs/${runId}`);
  return handleResponse(resp);
}

export function reportDownloadUrl(runId, type) {
  return `${BASE_URL}/reports/${runId}?type=${type}`;
}

# Data Quality Monitoring Platform

A deployable, full-stack evolution of the original
[`Data_quality_pipeline`](https://github.com/Shreyabhat11/Data_quality_pipeline)
script. The same detection logic (null spikes, schema drift, duplicate
detection, distribution drift, health scoring) now runs behind a REST API
with persistent history, served to a React dashboard, instead of a CLI
script that writes CSVs to a local `outputs/` folder.

## 1. What this project does

Upload a CSV. The platform runs a suite of data-quality checks against it
(optionally comparing it to a baseline/reference dataset), computes a
0–100 health score with a letter grade, stores the result, and shows you:

- the health score and grade
- every detected issue (schema drift, null spikes, distribution drift,
  duplicate spikes, datatype inconsistencies), with severity
- per-column metrics (null %, unique count, mean/median/std, inferred type)
- the full history of past validation runs
- downloadable CSV reports per run

## 2. Architecture

```
                    ┌──────────────────────┐
                    │      Web Browser      │
                    └──────────┬────────────┘
                               │
                               ▼
                    ┌──────────────────────┐
                    │  React / Vite Frontend│
                    │  Upload · Score ·     │
                    │  Issues · History     │
                    └──────────┬────────────┘
                               │ REST (JSON)
                               ▼
                    ┌──────────────────────┐
                    │       FastAPI         │
                    │ /health   /validate   │
                    │ /runs     /reports    │
                    └──────────┬────────────┘
                               │
                    ┌──────────┴────────────┐
                    ▼                       ▼
          ┌──────────────────┐    ┌──────────────────┐
          │  Quality Engine   │    │    PostgreSQL     │
          │  (app/quality)    │    │  runs, findings   │
          │  null / schema /  │    │  (metadata only — │
          │  dtype / dup /    │    │  no raw CSVs)     │
          │  drift / scoring  │    └──────────────────┘
          └──────────────────┘
```

`backend/app/quality/engine.py` is a direct refactor of the original
`data_quality_checker.py` — the thresholds and scoring formula are
unchanged, just turned into pure functions that take DataFrames instead of
scanning a folder. See **Design decisions** below for what changed and why.

## 3. Features

- **Missing-value detection** — null % per column, and (with a baseline)
  a null-rate *spike* alert when it jumps more than 20 percentage points.
- **Schema drift** — new columns, removed columns, and inferred-dtype
  changes vs. a baseline dataset.
- **Datatype validation** — flags a column that's neither cleanly numeric
  nor cleanly text (a mix of parseable and non-parseable values), even
  with no baseline supplied.
- **Duplicate detection** — duplicate row count/percentage, and a spike
  alert when the rate rises above 5% *and* above the baseline's rate.
- **Distribution drift** — mean-shift and standard-deviation-shift alerts
  on numeric columns vs. a baseline (>30% and >40% relative change).
- **Health scoring** — a deterministic, documented weighted-penalty
  formula (not a black-box ML score) — see below.
- **Validation history** — every run is persisted with its full result.
- **Report generation** — four CSV reports per run (quality/schema/anomaly/health),
  regenerated on demand from stored data, never from cached raw uploads.

### Health score formula

```
Health Score = 100
    − (avg_null_pct × 1.5)
    − (duplicate_pct × 2)
    − schema penalty:   10 × HIGH findings + 5 × MEDIUM findings
    − anomaly penalty:   8 × HIGH findings + 4 × MEDIUM findings
    − datatype penalty:  6 × HIGH findings + 3 × MEDIUM findings

Grade:  A ≥ 90 · B ≥ 75 · C ≥ 60 · D ≥ 40 · F < 40
```

## 4. Tech stack

- **Backend:** Python, FastAPI, SQLAlchemy, Pandas, NumPy
- **Database:** PostgreSQL (SQLite for local tests / zero-config dev)
- **Frontend:** React, Vite
- **Containerization:** Docker, Docker Compose
- **CI:** GitHub Actions
- **Deployment target:** Vercel (frontend) + Render (backend) + a managed
  Postgres instance

## 5. Local setup

### Option A — Docker Compose (recommended)

```bash
cp backend/.env.example backend/.env
docker compose up --build
```

- Frontend: http://localhost:4173
- Backend:  http://localhost:8000 (docs at `/docs`)
- Postgres: localhost:5432

### Option B — run backend and frontend directly

```bash
# Backend
cd backend
python -m venv .venv && source .venv/bin/activate
pip install -r requirements-dev.txt
cp .env.example .env   # defaults to a local SQLite file if you skip Postgres
uvicorn app.main:app --reload

# Frontend (separate terminal)
cd frontend
npm install
cp .env.example .env
npm run dev
```

## 6. API documentation

Interactive docs are auto-generated by FastAPI at `/docs` once the backend
is running. Summary:

| Method | Path | Description |
|---|---|---|
| `GET`  | `/health` | Liveness check. Does not touch the database. |
| `POST` | `/validate` | Multipart upload: `file` (required CSV), `baseline` (optional CSV). Runs the quality engine and returns the full result. |
| `GET`  | `/runs` | Paginated list of past validation runs (`?limit=&offset=`). |
| `GET`  | `/runs/{run_id}` | Full stored result for one run. |
| `GET`  | `/reports/{run_id}` | Download a regenerated CSV report. `?type=quality\|schema\|anomaly\|health`. |

## 7. Testing

```bash
cd backend
pytest --cov=app --cov-report=term-missing
```

The suite covers: the health endpoint, null-spike detection, schema-drift
detection, duplicate detection, distribution-drift detection (mean and
std), datatype-inconsistency detection, the health-score formula and its
grade boundaries, the `/validate` API (clean dataset, baseline-drift
dataset, malformed/empty/oversized/non-CSV rejections), run history and
run-detail retrieval, and database persistence/cascade-delete behavior.
Tests use an isolated SQLite database and never touch a production
service.

## 8. Deployment

### Backend → Render

1. Create a new **Web Service**, pointing at `backend/` with `Dockerfile` build.
2. Set environment variables: `DATABASE_URL` (from your managed Postgres),
   `CORS_ORIGINS` (your deployed frontend URL), `MAX_UPLOAD_SIZE`,
   `REPORTS_DIR=/app/reports`.
3. Render will build and expose the service on a public HTTPS URL.

### Database → Managed PostgreSQL

Provision Postgres (Render's managed Postgres, Supabase, Neon, etc.) and
put its connection string in the backend's `DATABASE_URL`. No manual
schema setup is needed — `init_db()` creates tables on startup.

### Frontend → Vercel

1. Import the repo, set the project root to `frontend/`.
2. Set `VITE_API_BASE_URL` to the deployed Render backend URL.
3. Deploy — Vercel builds with `npm run build` and serves `dist/`.

### CORS

Set the backend's `CORS_ORIGINS` env var to a comma-separated list
including the exact deployed frontend origin
(e.g. `https://dqp-frontend.vercel.app`). Avoid `*` in production.

## 9. Screenshots

_Add screenshots here after deploying:_

- `[ Dashboard screenshot placeholder ]`
- `[ Validation results screenshot placeholder ]`
- `[ Run history screenshot placeholder ]`

## 10. Design decisions

- **Baseline is explicit per request, not folder position.** The original
  script treated "the first CSV alphabetically in `data/`" as the
  baseline and compared sequential files to each other. That doesn't map
  onto a single-file upload flow, so the API takes an optional `baseline`
  file per request instead, and the response clearly marks
  `has_baseline` / `baseline_name`.
- **No baseline ⇒ intrinsic checks only.** Null %, duplicate %, and a new
  datatype-inconsistency check always run. Schema-drift and
  distribution-drift checks are skipped (not faked) without a baseline.
- **`STD_DRIFT_THRESHOLD` is now used.** The original script defined this
  constant but never checked it — it's now a real standard-deviation
  drift check, since the leftover constant looked like unfinished work
  rather than an intentional no-op.
- **Ragged CSVs are rejected, not silently reshaped.** By default, pandas
  tolerates rows with the wrong number of fields (by inventing an index
  column, or dropping data with a warning). The API validates row length
  against the header explicitly and returns a 400 instead of quietly
  parsing garbage.
- **No raw CSVs are persisted.** Only run metadata, per-column metrics,
  and findings are stored in Postgres; report CSVs are regenerated from
  that stored data on request.
- **Health score stays a documented formula, not a model.** Reproducible
  and explainable by design, per the original project's intent.

## Existing repository assets

This project builds on top of the original repository rather than
replacing it — `generate_datasets.py`, the sample datasets under `data/`,
`sql/data_quality_sql.sql`, and `POWERBI_GUIDE.md` remain in place as the
original standalone/CLI + Power BI path. This `backend/` + `frontend/` +
`docker-compose.yml` addition is the deployable platform described above.

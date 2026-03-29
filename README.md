# Backend Developer Technical Assessment

This repository contains:

- **mock-server** — Flask REST API serving `data/customers.json` (port **5000**)
- **pipeline-service** — FastAPI service that ingests mock data into PostgreSQL with **[dlt](https://dlthub.com/)** (`merge` + `upsert` on `customer_id`) (port **8000**)
- **postgres** — PostgreSQL 15 (port **5432**)

The `customers` table is created by **dlt** on the first successful `POST /api/ingest`. Until then, `GET /api/customers` returns an empty list. If you previously ran an older build that created a `customers` table without dlt metadata, reset the DB volume: `docker compose down -v` then `docker compose up -d`.

## Quick start

```bash
docker compose up -d
```

## Endpoints

### Flask (port 5000)

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/customers` | Paginated list (`page`, `limit`) |
| GET | `/api/customers/{id}` | Single customer (404 if missing) |
| GET | `/api/health` | Health check |

### FastAPI (port 8000)

| Method | Path | Description |
|--------|------|-------------|
| POST | `/api/ingest` | Pull all pages from Flask, upsert into Postgres |
| GET | `/api/customers` | Paginated rows from the database |
| GET | `/api/customers/{id}` | Single row (404 if missing) |
| GET | `/api/health` | Health check |

## Testing (from the host)

```bash
curl "http://localhost:5000/api/customers?page=1&limit=5"
curl -X POST "http://localhost:8000/api/ingest"
curl "http://localhost:8000/api/customers?page=1&limit=5"
```

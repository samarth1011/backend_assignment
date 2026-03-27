# Backend Developer Technical Assessment

This repository contains:
- A Flask mock server (`mock-server`)
- A FastAPI ingestion pipeline (`pipeline-service`)
- A PostgreSQL database via Docker Compose

## Quick Start

```bash
docker-compose up -d
```

## Planned Endpoints

### Flask (port 5000)
- `GET /api/customers`
- `GET /api/customers/{id}`
- `GET /api/health`

### FastAPI (port 8000)
- `POST /api/ingest`
- `GET /api/customers`
- `GET /api/customers/{id}`

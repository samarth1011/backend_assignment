"""Fetch paginated customer JSON from Flask and load into Postgres using dlt (merge/upsert)."""

from __future__ import annotations

import os
from typing import Any

import dlt
import requests

MOCK_SERVER_BASE_URL = os.getenv("MOCK_SERVER_BASE_URL", "http://mock-server:5000")


def _fetch_all_customers(limit: int = 100) -> list[dict[str, Any]]:
    page = 1
    total = None
    customers: list[dict[str, Any]] = []

    while True:
        response = requests.get(
            f"{MOCK_SERVER_BASE_URL}/api/customers",
            params={"page": page, "limit": limit},
            timeout=20,
        )
        response.raise_for_status()
        payload = response.json()
        data = payload.get("data", [])

        if total is None:
            total = int(payload.get("total", 0))

        customers.extend(data)
        if not data or len(customers) >= total:
            break
        page += 1

    return customers


def run_ingestion() -> int:
    database_url = os.environ["DATABASE_URL"]
    rows = _fetch_all_customers()
    if not rows:
        return 0

    pipeline = dlt.pipeline(
        pipeline_name="customer_ingestion",
        destination=dlt.destinations.postgres(database_url),
        dataset_name="public",
    )
    pipeline.run(
        rows,
        table_name="customers",
        write_disposition={"disposition": "merge", "strategy": "upsert"},
        primary_key="customer_id",
    )
    return len(rows)

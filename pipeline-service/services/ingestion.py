from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any

import requests
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from models.customer import Customer

MOCK_SERVER_BASE_URL = "http://mock-server:5000"


def _parse_date(value: str | None) -> date | None:
    if not value:
        return None
    return date.fromisoformat(value)


def _parse_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value)


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


def run_ingestion(db: Session) -> int:
    raw_customers = _fetch_all_customers()
    records_processed = 0

    for customer in raw_customers:
        stmt = insert(Customer).values(
            customer_id=customer["customer_id"],
            first_name=customer["first_name"],
            last_name=customer["last_name"],
            email=customer["email"],
            phone=customer.get("phone"),
            address=customer.get("address"),
            date_of_birth=_parse_date(customer.get("date_of_birth")),
            account_balance=Decimal(str(customer.get("account_balance", 0))),
            created_at=_parse_datetime(customer.get("created_at")),
        )

        upsert_stmt = stmt.on_conflict_do_update(
            index_elements=[Customer.customer_id],
            set_={
                "first_name": stmt.excluded.first_name,
                "last_name": stmt.excluded.last_name,
                "email": stmt.excluded.email,
                "phone": stmt.excluded.phone,
                "address": stmt.excluded.address,
                "date_of_birth": stmt.excluded.date_of_birth,
                "account_balance": stmt.excluded.account_balance,
                "created_at": stmt.excluded.created_at,
            },
        )
        db.execute(upsert_stmt)
        records_processed += 1

    db.commit()
    return records_processed

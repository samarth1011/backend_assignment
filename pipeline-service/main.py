from fastapi import Depends, FastAPI, HTTPException, Query
from sqlalchemy import inspect, select
from sqlalchemy.orm import Session

from database import engine, get_db, init_db
from models.customer import Customer
from services.ingestion import run_ingestion


def _customers_table_exists() -> bool:
    return inspect(engine).has_table("customers", schema="public")

app = FastAPI(title="Pipeline Service")


@app.on_event("startup")
def on_startup() -> None:
    init_db()


def _iso_date_or_dt(value) -> str | None:
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def _serialize_customer(customer: Customer) -> dict:
    bal = customer.account_balance
    if bal is not None and not isinstance(bal, (int, float)):
        try:
            bal = float(bal)
        except (TypeError, ValueError):
            bal = customer.account_balance

    return {
        "customer_id": customer.customer_id,
        "first_name": customer.first_name,
        "last_name": customer.last_name,
        "email": customer.email,
        "phone": customer.phone,
        "address": customer.address,
        "date_of_birth": _iso_date_or_dt(customer.date_of_birth),
        "account_balance": bal,
        "created_at": _iso_date_or_dt(customer.created_at),
    }


@app.post("/api/ingest")
def ingest_customers():
    try:
        processed = run_ingestion()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Ingestion failed: {exc}") from exc

    return {"status": "success", "records_processed": processed}


@app.get("/api/customers")
def get_customers(
    page: int = Query(default=1, ge=1),
    limit: int = Query(default=10, ge=1),
    db: Session = Depends(get_db),
):
    if not _customers_table_exists():
        return {"data": [], "total": 0, "page": page, "limit": limit}
    total = db.query(Customer).count()
    offset = (page - 1) * limit
    records = (
        db.query(Customer)
        .order_by(Customer.customer_id.asc())
        .offset(offset)
        .limit(limit)
        .all()
    )
    return {
        "data": [_serialize_customer(record) for record in records],
        "total": total,
        "page": page,
        "limit": limit,
    }


@app.get("/api/customers/{customer_id}")
def get_customer(customer_id: str, db: Session = Depends(get_db)):
    if not _customers_table_exists():
        raise HTTPException(status_code=404, detail="Customer not found")
    customer = db.scalar(select(Customer).where(Customer.customer_id == customer_id))
    if customer is None:
        raise HTTPException(status_code=404, detail="Customer not found")
    return _serialize_customer(customer)


@app.get("/api/health")
def health_check():
    return {"status": "ok"}

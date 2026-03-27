from decimal import Decimal

from fastapi import Depends, FastAPI, HTTPException, Query
from sqlalchemy import select
from sqlalchemy.orm import Session

from database import get_db, init_db
from models.customer import Customer
from services.ingestion import run_ingestion

app = FastAPI(title="Pipeline Service")


@app.on_event("startup")
def on_startup() -> None:
    init_db()


def _serialize_customer(customer: Customer) -> dict:
    return {
        "customer_id": customer.customer_id,
        "first_name": customer.first_name,
        "last_name": customer.last_name,
        "email": customer.email,
        "phone": customer.phone,
        "address": customer.address,
        "date_of_birth": customer.date_of_birth.isoformat()
        if customer.date_of_birth
        else None,
        "account_balance": float(customer.account_balance)
        if isinstance(customer.account_balance, Decimal)
        else customer.account_balance,
        "created_at": customer.created_at.isoformat() if customer.created_at else None,
    }


@app.post("/api/ingest")
def ingest_customers(db: Session = Depends(get_db)):
    try:
        processed = run_ingestion(db)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Ingestion failed: {exc}") from exc

    return {"status": "success", "records_processed": processed}


@app.get("/api/customers")
def get_customers(
    page: int = Query(default=1, ge=1),
    limit: int = Query(default=10, ge=1),
    db: Session = Depends(get_db),
):
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
    customer = db.scalar(select(Customer).where(Customer.customer_id == customer_id))
    if customer is None:
        raise HTTPException(status_code=404, detail="Customer not found")
    return _serialize_customer(customer)


@app.get("/api/health")
def health_check():
    return {"status": "ok"}

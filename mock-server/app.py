import json
from pathlib import Path

from flask import Flask, jsonify, request

app = Flask(__name__)


def load_customers() -> list[dict]:
    data_path = Path(__file__).parent / "data" / "customers.json"
    with data_path.open("r", encoding="utf-8") as file:
        return json.load(file)


CUSTOMERS = load_customers()
CUSTOMERS_BY_ID = {customer["customer_id"]: customer for customer in CUSTOMERS}


@app.get("/api/customers")
def get_customers():
    page = request.args.get("page", default=1, type=int)
    limit = request.args.get("limit", default=10, type=int)

    if not page or not limit or page < 1 or limit < 1:
        return jsonify({"error": "page and limit must be positive integers"}), 400

    total = len(CUSTOMERS)
    start = (page - 1) * limit
    end = start + limit

    return jsonify(
        {
            "data": CUSTOMERS[start:end],
            "total": total,
            "page": page,
            "limit": limit,
        }
    )


@app.get("/api/customers/<customer_id>")
def get_customer_by_id(customer_id: str):
    customer = CUSTOMERS_BY_ID.get(customer_id)
    if not customer:
        return jsonify({"error": "Customer not found"}), 404
    return jsonify(customer)


@app.get("/api/health")
def health_check():
    return jsonify({"status": "ok"})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)

"""
generate_synthetic_data.py — Generate synthetic e-commerce data with controllable defects.

Usage:
    python generate_synthetic_data.py --rows 5000 --null-rate 0.08 --duplicate-rate 0.05 --schema-drift

Defect injection parameters:
    --null-rate         float  Fraction of cells to null out              (default: 0.05)
    --duplicate-rate    float  Fraction of rows to duplicate              (default: 0.03)
    --schema-drift             Drop a column to simulate schema change
    --outlier-rate      float  Fraction of amount values to spike 100x    (default: 0.01)
    --stale-rate        float  Fraction of dates shifted 2 years back     (default: 0.02)
"""

import argparse
import random
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from faker import Faker

fake = Faker()
random.seed(42)
Faker.seed(42)


# ── Generators ─────────────────────────────────────────────────────────────────

def generate_customers(n: int = 500) -> pd.DataFrame:
    return pd.DataFrame({
        "customer_id":    range(1, n + 1),
        "name":           [fake.name() for _ in range(n)],
        "email":          [fake.email() for _ in range(n)],
        "country":        [fake.country_code() for _ in range(n)],
        "registered_at":  [fake.date_time_between("-3y", "now").isoformat() for _ in range(n)],
    })


def generate_products(n: int = 100) -> pd.DataFrame:
    categories = ["Electronics", "Clothing", "Books", "Home", "Sports"]
    return pd.DataFrame({
        "product_id":  range(1, n + 1),
        "name":        [fake.catch_phrase() for _ in range(n)],
        "category":    [random.choice(categories) for _ in range(n)],
        "unit_price":  [round(random.uniform(5.0, 500.0), 2) for _ in range(n)],
    })


def generate_orders(
    n: int,
    customer_ids: list,
    product_ids: list,
    null_rate: float,
    duplicate_rate: float,
    schema_drift: bool,
    outlier_rate: float,
    stale_rate: float,
) -> pd.DataFrame:
    base_date = datetime.utcnow()

    rows = []
    for i in range(1, n + 1):
        order_date = base_date - timedelta(days=random.randint(0, 90))
        amount = round(random.uniform(10.0, 1000.0), 2)
        rows.append({
            "order_id":    i,
            "customer_id": random.choice(customer_ids),
            "product_id":  random.choice(product_ids),
            "order_date":  order_date.strftime("%Y-%m-%d %H:%M:%S"),
            "amount":      amount,
            "quantity":    random.randint(1, 10),
            "status":      random.choice(["completed", "pending", "cancelled", "refunded"]),
        })

    df = pd.DataFrame(rows)

    # ── Inject nulls ───────────────────────────────────────────────────────────
    nullable_cols = ["customer_id", "product_id", "amount", "status"]
    for col in nullable_cols:
        mask = pd.Series([random.random() < null_rate for _ in range(len(df))])
        df.loc[mask, col] = None

    # ── Inject duplicates ──────────────────────────────────────────────────────
    n_dupes = int(len(df) * duplicate_rate)
    dupe_rows = df.sample(n=n_dupes, replace=True)
    df = pd.concat([df, dupe_rows], ignore_index=True)

    # ── Inject outliers ────────────────────────────────────────────────────────
    outlier_mask = pd.Series([random.random() < outlier_rate for _ in range(len(df))])
    df.loc[outlier_mask, "amount"] = df.loc[outlier_mask, "amount"] * 100

    # ── Inject stale timestamps ────────────────────────────────────────────────
    stale_mask = pd.Series([random.random() < stale_rate for _ in range(len(df))])
    df.loc[stale_mask, "order_date"] = (
        base_date - timedelta(days=730)
    ).strftime("%Y-%m-%d %H:%M:%S")

    # ── Schema drift — drop a column ──────────────────────────────────────────
    if schema_drift:
        df = df.drop(columns=["quantity"])

    return df.sample(frac=1).reset_index(drop=True)  # shuffle


# ── CLI ────────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Generate synthetic e-commerce data with injected defects.")
    parser.add_argument("--rows",           type=int,   default=5000)
    parser.add_argument("--null-rate",      type=float, default=0.05)
    parser.add_argument("--duplicate-rate", type=float, default=0.03)
    parser.add_argument("--outlier-rate",   type=float, default=0.01)
    parser.add_argument("--stale-rate",     type=float, default=0.02)
    parser.add_argument("--schema-drift",   action="store_true")
    parser.add_argument("--output-dir",     type=str,   default="datasets/generated")
    args = parser.parse_args()

    out = Path(args.output_dir)
    out.mkdir(parents=True, exist_ok=True)

    customers = generate_customers()
    products  = generate_products()
    orders    = generate_orders(
        n=args.rows,
        customer_ids=customers["customer_id"].tolist(),
        product_ids=products["product_id"].tolist(),
        null_rate=args.null_rate,
        duplicate_rate=args.duplicate_rate,
        schema_drift=args.schema_drift,
        outlier_rate=args.outlier_rate,
        stale_rate=args.stale_rate,
    )

    customers.to_csv(out / "customers.csv", index=False)
    products.to_csv(out  / "products.csv",  index=False)
    orders.to_csv(out    / "orders.csv",    index=False)

    print(f"Generated {len(orders)} orders → {out}/orders.csv")
    print(f"  Customers : {len(customers)}")
    print(f"  Products  : {len(products)}")
    print(f"  Null rate : {args.null_rate}")
    print(f"  Dup rate  : {args.duplicate_rate}")
    print(f"  Schema drift: {args.schema_drift}")


if __name__ == "__main__":
    main()

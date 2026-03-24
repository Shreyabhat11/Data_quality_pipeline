"""
Dataset Generator for Data Quality & Pipeline Monitoring Dashboard
Generates 3 days of simulated order data with progressively introduced issues.
"""

import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta
import os

random.seed(42)
np.random.seed(42)

OUTPUT_DIR = "data"
os.makedirs(OUTPUT_DIR, exist_ok=True)

REGIONS = ["North", "South", "East", "West", "Central"]

def generate_order_ids(n, start=1000):
    return [f"ORD-{i:05d}" for i in range(start, start + n)]

def generate_customer_ids(n):
    return [f"CUST-{random.randint(1000, 9999)}" for _ in range(n)]

def generate_dates(n, base_date="2024-01-01"):
    base = datetime.strptime(base_date, "%Y-%m-%d")
    return [(base + timedelta(days=random.randint(0, 30))).strftime("%Y-%m-%d") for _ in range(n)]

# ─────────────────────────────────────────────────────────────
# DAY 1 — Clean baseline data
# ─────────────────────────────────────────────────────────────
def generate_day1(n=120):
    df = pd.DataFrame({
        "order_id":     generate_order_ids(n, start=1000),
        "customer_id":  generate_customer_ids(n),
        "order_amount": np.round(np.random.normal(loc=150, scale=40, size=n).clip(10, 500), 2),
        "order_date":   generate_dates(n, "2024-01-01"),
        "region":       [random.choice(REGIONS) for _ in range(n)],
    })
    return df

# ─────────────────────────────────────────────────────────────
# DAY 2 — Missing values, schema changes, datatype issues
# ─────────────────────────────────────────────────────────────
def generate_day2(n=120):
    df = pd.DataFrame({
        "order_id":     generate_order_ids(n, start=1200),
        "customer_id":  generate_customer_ids(n),
        "order_amount": np.round(np.random.normal(loc=150, scale=40, size=n).clip(10, 500), 2),
        "order_date":   generate_dates(n, "2024-01-02"),
        "region":       [random.choice(REGIONS) for _ in range(n)],
    })

    # Issue 1: Introduce ~28% nulls in customer_id (null spike)
    null_idx_cust = random.sample(range(n), k=int(n * 0.28))
    df.loc[null_idx_cust, "customer_id"] = None

    # Issue 2: Introduce ~25% nulls in order_amount
    null_idx_amt = random.sample(range(n), k=int(n * 0.25))
    df.loc[null_idx_amt, "order_amount"] = None

    # Issue 3: Remove 'region' column (schema drift — missing column)
    df.drop(columns=["region"], inplace=True)

    # Issue 4: Add new column 'discount_code' (schema drift — new column)
    df["discount_code"] = [f"DC{random.randint(100,999)}" if random.random() > 0.4 else None for _ in range(n)]

    # Issue 5: Datatype inconsistency — mix strings into order_amount
    # Convert to object first, then inject bad strings
    amt_list = df["order_amount"].astype(object).tolist()
    bad_rows = random.sample([i for i in range(n) if pd.notna(df.loc[i, "order_amount"])], k=8)
    for i in bad_rows:
        amt_list[i] = random.choice(["N/A", "unknown", "TBD", "--"])
    df["order_amount"] = amt_list

    return df

# ─────────────────────────────────────────────────────────────
# DAY 3 — Duplicates, distribution shift, more nulls
# ─────────────────────────────────────────────────────────────
def generate_day3(n=120):
    df = pd.DataFrame({
        "order_id":     generate_order_ids(n, start=1400),
        "customer_id":  generate_customer_ids(n),
        "order_amount": np.round(np.random.normal(loc=320, scale=90, size=n).clip(50, 900), 2),  # distribution shift!
        "order_date":   generate_dates(n, "2024-01-03"),
        "region":       [random.choice(REGIONS) for _ in range(n)],
    })

    # Issue 1: Introduce duplicates (~15 rows duplicated)
    dup_rows = df.sample(15, random_state=7)
    df = pd.concat([df, dup_rows], ignore_index=True)

    # Issue 2: Some nulls in region (~22%)
    null_idx_reg = random.sample(range(len(df)), k=int(len(df) * 0.22))
    df.loc[null_idx_reg, "region"] = None

    # Issue 3: Some nulls in order_date (~12%)
    null_idx_date = random.sample(range(len(df)), k=int(len(df) * 0.12))
    df.loc[null_idx_date, "order_date"] = None

    # Issue 4: Re-add discount_code column (carries forward from day2 schema)
    df["discount_code"] = [f"DC{random.randint(100,999)}" if random.random() > 0.5 else None for _ in range(len(df))]

    # Issue 5: A few nulls in customer_id (~5%)
    null_idx_cust = random.sample(range(len(df)), k=int(len(df) * 0.05))
    df.loc[null_idx_cust, "customer_id"] = None

    return df

# ─────────────────────────────────────────────────────────────
# Generate and Save
# ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    d1 = generate_day1()
    d2 = generate_day2()
    d3 = generate_day3()

    d1.to_csv(f"{OUTPUT_DIR}/orders_day_1.csv", index=False)
    d2.to_csv(f"{OUTPUT_DIR}/orders_day_2.csv", index=False)
    d3.to_csv(f"{OUTPUT_DIR}/orders_day_3.csv", index=False)

    print(f"✅ Day 1: {d1.shape}  | Columns: {list(d1.columns)}")
    print(f"✅ Day 2: {d2.shape}  | Columns: {list(d2.columns)}")
    print(f"✅ Day 3: {d3.shape}  | Columns: {list(d3.columns)}")
    print("\n📁 Datasets saved to ./data/")

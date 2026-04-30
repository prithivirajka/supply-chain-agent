"""
load_raw_data.py

One-time script to load all Olist CSVs into the DuckDB database as raw tables.
Run this before dbt run to populate the source schema.

Usage (from the supply-chain-agent root):
    python data/load_raw_data.py
"""

import duckdb
from pathlib import Path

DB_PATH  = Path(__file__).parent / "supply_chain.duckdb"
DATA_DIR = Path(__file__).parent
SCHEMA   = "raw"

CSV_MAP = {
    "olist_orders_dataset":              "olist_orders_dataset.csv",
    "olist_order_items_dataset":         "olist_order_items_dataset.csv",
    "olist_order_payments_dataset":      "olist_order_payments_dataset.csv",
    "olist_order_reviews_dataset":       "olist_order_reviews_dataset.csv",
    "olist_customers_dataset":           "olist_customers_dataset.csv",
    "olist_sellers_dataset":             "olist_sellers_dataset.csv",
    "olist_products_dataset":            "olist_products_dataset.csv",
    "olist_geolocation_dataset":         "olist_geolocation_dataset.csv",
    "product_category_name_translation": "product_category_name_translation.csv",
}


def load():
    print(f"Connecting to DuckDB at: {DB_PATH}")
    con = duckdb.connect(str(DB_PATH))
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")

    for table_name, csv_file in CSV_MAP.items():
        csv_path = DATA_DIR / csv_file
        if not csv_path.exists():
            print(f"  MISSING: {csv_file}, skipping")
            continue

        con.execute(f"DROP TABLE IF EXISTS {SCHEMA}.{table_name}")
        con.execute(f"""
            CREATE TABLE {SCHEMA}.{table_name} AS
            SELECT * FROM read_csv_auto('{csv_path.as_posix()}', header=true)
        """)
        count = con.execute(
            f"SELECT count(*) FROM {SCHEMA}.{table_name}"
        ).fetchone()[0]
        print(f"  OK  {SCHEMA}.{table_name:45s} {count:>8,} rows")

    con.close()
    print(f"\nDone. Database saved to: {DB_PATH}")


if __name__ == "__main__":
    load()

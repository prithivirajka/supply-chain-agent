# Supply Chain Analytics — dbt Semantic Layer

A **dbt + DuckDB semantic layer** for supply chain analytics, built on the
[Olist Brazilian E-Commerce dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce).

## What This Project Does

Transforms raw Olist e-commerce CSV data into a clean, tested, analytics-ready
semantic layer using dbt and DuckDB. The output is a set of mart tables covering
orders, delivery performance, sellers, products, customers, and monthly KPI
summaries — ready to be queried for supply chain insights.

## Project Structure

```
supply-chain-agent/
├── dbt_project/          # dbt semantic layer & data transformations
│   ├── models/
│   │   ├── staging/      # Raw source cleaning (one model per source table)
│   │   ├── intermediate/ # Business logic, joins, and derived entities
│   │   └── marts/        # Final analytical tables
│   ├── macros/           # Reusable Jinja SQL macros
│   ├── seeds/            # Static reference data (currently unused)
│   ├── tests/            # Custom data tests (currently unused)
│   ├── dbt_project.yml   # dbt project configuration
│   ├── packages.yml      # dbt package dependencies
│   ├── profiles.yml      # Database connection config (template)
│   └── README.md         # dbt-specific documentation
└── data/
    ├── load_raw_data.py  # One-time script to load CSVs into DuckDB
    ├── supply_chain.duckdb  # DuckDB database (generated)
    └── *.csv             # Olist raw CSV files
```

## Dataset

The [Olist Brazilian E-Commerce dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
contains ~100k orders from 2016–2018 across multiple Brazilian marketplaces:

- Orders & order items
- Products & product categories
- Sellers & geolocation
- Customers & reviews
- Payments & delivery timestamps

## Quick Start

### 1. Install dependencies

```bash
pip install dbt-duckdb duckdb
```

### 2. Download the Olist dataset

Download from Kaggle and place all 9 CSV files in the `data/` folder.

### 3. Load CSVs into DuckDB

```bash
python data/load_raw_data.py
```

### 4. Set up dbt profile

```bash
# Windows
copy dbt_project\profiles.yml %USERPROFILE%\.dbt\profiles.yml

# Mac/Linux
cp dbt_project/profiles.yml ~/.dbt/profiles.yml
```

### 5. Install dbt packages and run

```bash
cd dbt_project
dbt deps
dbt debug        # verify connection
dbt run          # build all models
dbt test         # run all data quality tests
```

### 6. Browse the docs

```bash
dbt docs generate
dbt docs serve   # opens at localhost:8080
```

## Model Layers

### Staging (8 views)
Cleans and casts raw source data. One model per Olist source table.

| Model | Source |
|---|---|
| `stg_orders` | olist_orders_dataset |
| `stg_order_items` | olist_order_items_dataset |
| `stg_order_payments` | olist_order_payments_dataset |
| `stg_order_reviews` | olist_order_reviews_dataset |
| `stg_customers` | olist_customers_dataset |
| `stg_sellers` | olist_sellers_dataset |
| `stg_products` | olist_products_dataset |
| `stg_geolocation` | olist_geolocation_dataset |

### Intermediate (4 ephemeral)
Business logic and joins. Compiled inline — no database objects created.

| Model | Description |
|---|---|
| `int_orders_enriched` | Orders + items + payments + latest review |
| `int_delivery_performance` | Delivery timing, late flags, delay severity |
| `int_seller_performance` | Per-seller aggregated KPIs |
| `int_customer_orders` | Customer LTV, recency segment, repeat flag |

### Marts (6 tables)
Final analytical tables. Pre-materialised for fast queries.

| Model | Rows | Description |
|---|---|---|
| `mart_orders` | 99,441 | Full order fact table |
| `mart_delivery` | 96,478 | Delivery performance per order |
| `mart_sellers` | 3,095 | Seller scorecard + composite score |
| `mart_products` | 32,951 | Product analytics + freight ratio |
| `mart_customers` | 94,990 | Customer LTV + engagement score |
| `mart_supply_chain_summary` | 25 | Monthly KPI rollup (Sep 2016 – Sep 2018) |

## Test Results

```
Staging:      101 passed, 4 warnings (geolocation outliers in source data — expected)
Marts:          6 passed, 0 warnings
```

## Key Findings from the Data

- Platform grew from 4 orders/month (Sep 2016) to 6,500+ orders/month (mid-2018)
- November 2017 spike: 7,544 orders, late delivery rate jumps to 12.4% (Black Friday)
- Feb–Mar 2018 anomaly: late delivery rate peaks at 14–19%, review scores drop to 3.75
- Steady-state review score: ~4.1–4.3 once the platform matured

## Tech Stack

| Component | Technology |
|---|---|
| Data transformation | dbt 1.11.8 |
| Database | DuckDB 1.5.2 |
| dbt adapter | dbt-duckdb 1.10.1 |
| Data quality | dbt_expectations |
| Language | SQL + Jinja2 |

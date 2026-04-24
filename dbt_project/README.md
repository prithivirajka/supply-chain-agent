# dbt Project — Supply Chain Analytics

This dbt project transforms raw Olist Brazilian E-Commerce data into a clean,
tested semantic layer using DuckDB as the database backend.

## Quick Start

```bash
# Install dbt with DuckDB adapter
pip install dbt-duckdb

# Install dbt packages
dbt deps

# Copy profile to dbt home directory
# Windows:
copy profiles.yml %USERPROFILE%\.dbt\profiles.yml
# Mac/Linux:
cp profiles.yml ~/.dbt/profiles.yml

# Verify connection
dbt debug

# Build all models
dbt run

# Run all data quality tests
dbt test

# Generate and view documentation
dbt docs generate
dbt docs serve
```

## Running Specific Layers

```bash
dbt run --select staging               # 8 staging views
dbt run --select intermediate          # 4 ephemeral models
dbt run --select marts                 # 6 mart tables
dbt run --select +mart_delivery        # mart_delivery and all upstream deps
dbt test --select staging              # test staging layer only
```

## Model Layers

### `staging/` — Views
One model per raw source table. Responsibilities:
- Rename columns to snake_case
- Cast data types
- Apply basic null handling and normalisation
- No business logic, no joins (except lookup joins for stg_sellers and stg_products)

| Model | Key transformations |
|---|---|
| `stg_orders` | Cast timestamps, add order_date, is_delivered, is_canceled flags |
| `stg_order_items` | Cast price/freight to numeric, derive line_total |
| `stg_order_payments` | Normalise payment_type, add is_installment_purchase flag |
| `stg_order_reviews` | Cast score to int, derive review_sentiment from project vars |
| `stg_customers` | Normalise city (lowercase), state (uppercase) |
| `stg_geolocation` | Deduplicate to one row per zip_code_prefix with avg coordinates |
| `stg_sellers` | Join geolocation on zip only for coordinates |
| `stg_products` | Join category translation; correct source CSV typo (lenght → length) |

### `intermediate/` — Ephemeral
Business logic and joins. Not materialised — compiled inline into mart queries.

| Model | Description |
|---|---|
| `int_orders_enriched` | Master join: orders + items + payments + latest review per order |
| `int_delivery_performance` | Delivery timing deltas, late/critical flags, delivery_status label |
| `int_seller_performance` | Per-seller aggregation: revenue, review quality, delivery rates |
| `int_customer_orders` | Per-customer aggregation: LTV, recency segment, repeat flag |

### `marts/` — Tables
Final analytical tables. Pre-materialised for fast reads.

| Model | Rows | Description |
|---|---|---|
| `mart_orders` | 99,441 | Complete order fact table with customer and category context |
| `mart_delivery` | 96,478 | Delivered orders with all timing metrics and geographic context |
| `mart_sellers` | 3,095 | Seller scorecard with composite seller_score |
| `mart_products` | 32,951 | Product analytics with freight_to_price_ratio |
| `mart_customers` | 94,990 | Customer LTV, value tier, engagement score |
| `mart_supply_chain_summary` | 25 | Monthly KPI rollup Sep 2016 – Sep 2018 |

## Key Variables

Defined in `dbt_project.yml`. Override at runtime with `--vars`:

| Variable | Default | Description |
|---|---|---|
| `start_date` | `2016-01-01` | Analysis window start |
| `end_date` | `2018-12-31` | Analysis window end |
| `late_delivery_threshold_days` | `0` | Days past estimate = late |
| `critical_delay_threshold_days` | `7` | Days late = critical delay |
| `high_value_order_threshold` | `500` | BRL threshold for high-value orders |
| `positive_review_min_score` | `4` | Score ≥ this = positive review |
| `negative_review_max_score` | `2` | Score ≤ this = negative review |

```bash
# Example: override thresholds at runtime
dbt run --vars '{"late_delivery_threshold_days": 2, "start_date": "2017-01-01"}'
```

## Test Results

```
Staging:   101 passed, 4 warnings
           (warnings = geolocation coordinate outliers in source data — expected)
Marts:       6 passed, 0 warnings
```

## Known Source Data Issues

| Issue | Location | Impact | Handling |
|---|---|---|---|
| Column typo `product_name_lenght` | olist_products_dataset.csv | None | Mapped in stg_products.sql |
| ~30 coordinates outside Brazil bounds | olist_geolocation_dataset.csv | None | Severity set to warn |
| Multiple lat/lng per zip code | olist_geolocation_dataset.csv | Would fan-out joins | Deduplicated in stg_geolocation.sql |

## Database Connection

The default `dev` target connects to a local DuckDB file:

```yaml
path: ../data/supply_chain.duckdb
```

All dbt-built schemas live inside this file:
- `raw` — loaded by load_raw_data.py (source tables)
- `main_staging` — staging views
- `main_marts` — mart tables

# dbt Project

This directory contains the dbt project that transforms raw Olist data into a clean semantic layer backed by DuckDB.

## Quick Start

```bash
pip install dbt-duckdb

dbt deps

# Windows
copy profiles.yml %USERPROFILE%\.dbt\profiles.yml
# Mac/Linux
cp profiles.yml ~/.dbt/profiles.yml

dbt debug
dbt run
dbt test

dbt docs generate
dbt docs serve
```

## Running Specific Layers

```bash
dbt run --select staging
dbt run --select intermediate
dbt run --select marts
dbt run --select +mart_delivery
dbt test --select staging
```

## Model Layers

### staging

One model per raw source table. Each model renames columns to snake_case, casts data types, and adds simple derived fields. There is no business logic or joining in this layer, with the exception of stg_sellers (which joins geolocation for coordinates) and stg_products (which joins the category translation table).

| Model | Key work |
|---|---|
| stg_orders | Casts all timestamps, adds order_date, is_delivered and is_canceled flags |
| stg_order_items | Casts price and freight to numeric, derives line_total |
| stg_order_payments | Normalises payment_type, adds is_installment_purchase flag |
| stg_order_reviews | Casts score to integer, derives review_sentiment using project variables |
| stg_customers | Normalises city to lowercase and state to uppercase |
| stg_geolocation | Deduplicates to one row per zip_code_prefix using averaged coordinates |
| stg_sellers | Joins geolocation on zip_code_prefix only to avoid fan-out |
| stg_products | Joins category translation table, corrects source CSV typo (lenght to length) |

### intermediate

Business logic and joins. These models are materialised as ephemeral, meaning they are compiled as CTEs directly into downstream mart queries. No database objects are created for this layer.

| Model | Description |
|---|---|
| int_orders_enriched | Joins orders with aggregated items, payments, and the latest review per order |
| int_delivery_performance | Calculates days_to_deliver, days_vs_estimate, is_late, is_critical_delay, and delivery_status |
| int_seller_performance | Aggregates revenue, review scores, and delivery rates per seller |
| int_customer_orders | Aggregates order history per customer_unique_id with LTV and recency segment |

### marts

Final analytical tables materialised as DuckDB tables. These are what the LangChain agent queries directly.

| Model | Rows | Description |
|---|---|---|
| mart_orders | 99,441 | Complete order fact table with customer and category context |
| mart_delivery | 96,478 | Delivered orders with timing metrics and geographic context |
| mart_sellers | 3,095 | Seller scorecard with composite seller_score |
| mart_products | 32,951 | Product analytics including freight_to_price_ratio |
| mart_customers | 94,990 | Customer LTV, value tier, and engagement score |
| mart_supply_chain_summary | 25 | Monthly KPI rollup from Sep 2016 to Sep 2018 |

## Project Variables

All business thresholds are defined in dbt_project.yml and can be overridden at runtime.

| Variable | Default | Description |
|---|---|---|
| start_date | 2016-01-01 | Analysis window start |
| end_date | 2018-12-31 | Analysis window end |
| late_delivery_threshold_days | 0 | Days past estimated delivery date considered late |
| critical_delay_threshold_days | 7 | Days late considered a critical delay |
| high_value_order_threshold | 500 | BRL amount threshold for high-value orders |
| positive_review_min_score | 4 | Minimum score to classify a review as positive |
| negative_review_max_score | 2 | Maximum score to classify a review as negative |

```bash
dbt run --vars '{"late_delivery_threshold_days": 2, "start_date": "2017-01-01"}'
```

## Test Results

Staging layer: 101 tests passed, 4 warnings. The warnings are geolocation coordinate outliers in the source data that fall slightly outside Brazil's geographic boundaries. These are known dirty rows in the CSV and do not affect any analytics.

Marts layer: 6 tests passed, 0 warnings.

## Known Source Data Issues

| Issue | File | Handling |
|---|---|---|
| Column name typo: product_name_lenght | olist_products_dataset.csv | Mapped to correct name in stg_products.sql |
| Around 30 coordinates outside Brazil bounds | olist_geolocation_dataset.csv | Test severity set to warn |
| Multiple lat/lng readings per zip code | olist_geolocation_dataset.csv | Deduplicated by averaging in stg_geolocation.sql |

## Database Schemas

The DuckDB file at data/supply_chain.duckdb contains three schemas:

- raw: loaded by data/load_raw_data.py, contains the 9 source tables
- main_staging: contains the 8 staging views
- main_marts: contains the 6 mart tables

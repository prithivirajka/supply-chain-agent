# Supply Chain Analytics

A natural language analytics agent for supply chain data, built on the Olist Brazilian E-Commerce dataset. Ask questions in plain English and get answers backed by real SQL queries against a dbt-modeled DuckDB database.

## What This Project Does

The project has three layers working together. First, a dbt semantic layer transforms raw Olist CSV data into clean, tested analytical tables. Second, a LangChain agent powered by Claude reads those tables and answers natural language questions by writing and executing SQL. Third, a Streamlit UI and FastAPI backend expose the agent as a usable application.

## Project Structure

```
supply-chain-agent/
├── dbt_project/          # dbt models, tests, and configuration
│   ├── models/
│   │   ├── staging/      # one model per raw source table
│   │   ├── intermediate/ # joins and business logic
│   │   └── marts/        # final analytical tables
│   └── macros/           # reusable SQL macros
├── agent/                # LangChain agent and tools
│   ├── agent.py          # agent setup and query runner
│   ├── tools.py          # get_schema and run_sql tools
│   ├── prompts.py        # system prompt and few-shot examples
│   ├── schema_context.py # reads mart metadata from DuckDB
│   └── config.py         # environment and path config
├── api/                  # FastAPI backend
│   ├── main.py           # /query, /health, /schema endpoints
│   └── models.py         # Pydantic request and response schemas
├── ui/                   # Streamlit frontend
│   ├── app.py            # main app entry point
│   └── components/       # sidebar, chat, utils
├── data/                 # raw CSVs and DuckDB file (not committed)
│   └── load_raw_data.py  # one-time CSV loader
└── test_api.py           # API test script
```

## Dataset

The Olist Brazilian E-Commerce dataset contains roughly 100k orders placed between 2016 and 2018 across multiple Brazilian marketplaces. It includes orders, order items, payments, reviews, customers, sellers, products, and geolocation data. Download it from Kaggle: https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce

## Getting Started

### 1. Install dependencies

```bash
pip install dbt-duckdb duckdb
pip install -r api/requirements.txt
pip install streamlit requests
```

### 2. Download the Olist dataset

Download from Kaggle and place all 9 CSV files in the data/ folder.

### 3. Load CSVs into DuckDB

```bash
python data/load_raw_data.py
```

### 4. Set up the dbt profile

```bash
# Windows
copy dbt_project\profiles.yml %USERPROFILE%\.dbt\profiles.yml

# Mac/Linux
cp dbt_project/profiles.yml ~/.dbt/profiles.yml
```

### 5. Build the dbt models

```bash
cd dbt_project
dbt deps
dbt run
dbt test
cd ..
```

### 6. Set your Anthropic API key

Copy .env.example to .env and fill in your key:

```
ANTHROPIC_API_KEY=your_key_here
```

### 7. Start the API server

```bash
uvicorn api.main:app
```

### 8. Start the UI

```bash
streamlit run ui/app.py
```

Open http://localhost:8501 in your browser.

## dbt Models

### Staging (8 views)

One model per raw source table. Each one renames columns, casts types, and adds basic derived fields. No joins, no business logic.

| Model | Source table |
|---|---|
| stg_orders | olist_orders_dataset |
| stg_order_items | olist_order_items_dataset |
| stg_order_payments | olist_order_payments_dataset |
| stg_order_reviews | olist_order_reviews_dataset |
| stg_customers | olist_customers_dataset |
| stg_sellers | olist_sellers_dataset |
| stg_products | olist_products_dataset |
| stg_geolocation | olist_geolocation_dataset |

### Intermediate (4 ephemeral)

Business logic and joins. These are not materialised as database objects — they compile inline into the mart queries.

| Model | What it does |
|---|---|
| int_orders_enriched | Joins orders, items, payments, and the latest review into one row per order |
| int_delivery_performance | Calculates delivery timing, late flags, and delay severity |
| int_seller_performance | Aggregates revenue, review scores, and delivery rates per seller |
| int_customer_orders | Aggregates order history and lifetime value per unique customer |

### Marts (6 tables)

Final analytical tables. These are what the agent queries.

| Model | Rows | Description |
|---|---|---|
| mart_orders | 99,441 | Full order fact table |
| mart_delivery | 96,478 | Delivery performance per delivered order |
| mart_sellers | 3,095 | Seller scorecard with composite seller score |
| mart_products | 32,951 | Product analytics with freight-to-price ratio |
| mart_customers | 94,990 | Customer LTV, value tier, and engagement score |
| mart_supply_chain_summary | 25 | Monthly KPI rollup from Sep 2016 to Sep 2018 |

## API Endpoints

| Method | Endpoint | Description |
|---|---|---|
| GET | /health | Checks API, model, and database connectivity |
| GET | /schema | Returns column metadata for all mart tables |
| POST | /query | Takes a natural language question, returns an answer with SQL and data |

Example query:

```bash
curl -X POST http://localhost:8000/query \
  -H "Content-Type: application/json" \
  -d '{"question": "Which states have the worst late delivery rate?"}'
```

## Testing

Run the API test suite while the server is running:

```bash
python test_api.py health
python test_api.py schema
python test_api.py validation
python test_api.py query
```

## Key Numbers from the Data

The platform grew from 4 orders per month in September 2016 to over 6,500 per month by mid-2018. The overall on-time delivery rate is 93.23% across 96,478 delivered orders. November 2017 saw 7,544 orders with the late delivery rate jumping to 12.4%, consistent with Black Friday demand. February and March 2018 had late delivery rates of 14% and 19% respectively, coinciding with the lowest review scores in the dataset at around 3.75.

## Tech Stack

| Layer | Technology |
|---|---|
| Data transformation | dbt 1.11.8 |
| Database | DuckDB 1.5.2 |
| LLM agent | LangChain 1.2 + Claude (claude-sonnet-4-6) |
| API | FastAPI |
| UI | Streamlit |
| Data quality | dbt_expectations |

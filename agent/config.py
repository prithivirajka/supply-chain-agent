"""
config.py

Central configuration for the supply chain agent.
All settings are read from environment variables with sensible defaults.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

ROOT_DIR  = Path(__file__).parent.parent
AGENT_DIR = Path(__file__).parent
DATA_DIR  = ROOT_DIR / "data"

DUCKDB_PATH = Path(os.getenv("DUCKDB_PATH", str(DATA_DIR / "supply_chain.duckdb")))

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
MODEL_NAME        = os.getenv("MODEL_NAME", "claude-sonnet-4-6")
MAX_TOKENS        = int(os.getenv("MAX_TOKENS", "4096"))
TEMPERATURE       = float(os.getenv("TEMPERATURE", "0"))

MAX_ITERATIONS = int(os.getenv("MAX_ITERATIONS", "10"))
MAX_SQL_ROWS   = int(os.getenv("MAX_SQL_ROWS", "100"))
VERBOSE        = os.getenv("VERBOSE", "false").lower() == "true"

MARTS_SCHEMA = os.getenv("MARTS_SCHEMA", "main_marts")

ALLOWED_TABLES = [
    "mart_orders",
    "mart_delivery",
    "mart_sellers",
    "mart_products",
    "mart_customers",
    "mart_supply_chain_summary",
]


def validate():
    """Raise early if critical config is missing."""
    if not ANTHROPIC_API_KEY:
        raise EnvironmentError(
            "ANTHROPIC_API_KEY is not set. "
            "Add it to your .env file or environment."
        )
    if not DUCKDB_PATH.exists():
        raise FileNotFoundError(
            f"DuckDB database not found at: {DUCKDB_PATH}\n"
            "Run 'python data/load_raw_data.py' and 'dbt run' first."
        )

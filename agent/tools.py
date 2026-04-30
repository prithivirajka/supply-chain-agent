"""
tools.py
─────────────────────────────────────────────────────────────────────────────
Two LangChain tools the agent uses:
  1. get_schema  — look up what columns a mart table has
  2. run_sql     — execute a SELECT query against DuckDB and get back rows
"""

import re
import json
import duckdb
from langchain.tools import tool

from agent.config import DUCKDB_PATH, MARTS_SCHEMA, ALLOWED_TABLES, MAX_SQL_ROWS
from agent.schema_context import format_schema_for_prompt


# ── Tool 1: Schema lookup ─────────────────────────────────────────────────

@tool
def get_schema(table_name: str = "all") -> str:
    """
    Returns the column names and data types for one or all mart tables.

    Use this BEFORE writing any SQL so you know exactly which columns exist.

    Args:
        table_name: Name of the mart table (e.g. 'mart_sellers') or 'all'
                    to see every available table.

    Returns:
        A formatted string listing tables, their columns and row counts.
    """
    if table_name.lower() == "all":
        return format_schema_for_prompt()

    # Strip schema prefix if the agent accidentally includes it
    clean = table_name.replace(f"{MARTS_SCHEMA}.", "").strip()

    if clean not in ALLOWED_TABLES:
        available = ", ".join(ALLOWED_TABLES)
        return (
            f"Table '{clean}' not found. "
            f"Available tables: {available}"
        )

    return format_schema_for_prompt(clean)


# ── Tool 2: SQL execution ─────────────────────────────────────────────────

def _is_safe_sql(sql: str) -> tuple[bool, str]:
    """
    Validates that the SQL is a read-only SELECT statement and only
    references allowed mart tables.
    Returns (is_safe, reason).
    """
    # Normalise
    normalised = sql.strip().upper()

    # Must start with SELECT
    if not normalised.startswith("SELECT"):
        return False, "Only SELECT statements are allowed."

    # Block mutating keywords
    forbidden = ["INSERT", "UPDATE", "DELETE", "DROP", "CREATE",
                 "ALTER", "TRUNCATE", "REPLACE", "MERGE"]
    for kw in forbidden:
        if re.search(rf"\b{kw}\b", normalised):
            return False, f"Statement contains forbidden keyword: {kw}"

    # Must reference at least one allowed table
    tables_referenced = [
        t for t in ALLOWED_TABLES
        if t.upper() in normalised
    ]
    if not tables_referenced:
        allowed = ", ".join(ALLOWED_TABLES)
        return False, (
            f"Query must reference at least one allowed table. "
            f"Allowed: {allowed}"
        )

    return True, "ok"


@tool
def run_sql(sql: str) -> str:
    """
    Executes a SQL SELECT query against the supply chain DuckDB database
    and returns the results as a JSON string.

    Rules:
    - Only SELECT statements are allowed (no INSERT, UPDATE, DELETE, DROP etc.)
    - Always qualify table names with the schema: main_marts.mart_orders
    - LIMIT your query to avoid returning too many rows (max 100)
    - Available tables: mart_orders, mart_delivery, mart_sellers,
      mart_products, mart_customers, mart_supply_chain_summary

    Args:
        sql: A valid DuckDB SQL SELECT statement.

    Returns:
        JSON string with keys: columns, rows, row_count, truncated.
    """
    # Safety check
    is_safe, reason = _is_safe_sql(sql)
    if not is_safe:
        return json.dumps({"error": reason})

    con = duckdb.connect(str(DUCKDB_PATH), read_only=True)
    try:
        result = con.execute(sql).fetchdf()

        truncated = len(result) >= MAX_SQL_ROWS
        if truncated:
            result = result.head(MAX_SQL_ROWS)

        # Convert to JSON-serialisable format
        records = result.to_dict(orient="records")

        # Round floats to 4 decimal places for readability
        clean_records = []
        for row in records:
            clean_row = {}
            for k, v in row.items():
                if isinstance(v, float):
                    clean_row[k] = round(v, 4)
                elif hasattr(v, "item"):        # numpy scalar
                    clean_row[k] = v.item()
                elif hasattr(v, "isoformat"):   # date / datetime
                    clean_row[k] = v.isoformat()
                else:
                    clean_row[k] = v
            clean_records.append(clean_row)

        return json.dumps({
            "columns":   list(result.columns),
            "rows":      clean_records,
            "row_count": len(clean_records),
            "truncated": truncated,
        }, default=str)

    except Exception as e:
        return json.dumps({"error": str(e)})
    finally:
        con.close()


# ── Exported list for AgentExecutor ──────────────────────────────────────
TOOLS = [get_schema, run_sql]

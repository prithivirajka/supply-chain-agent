"""
schema_context.py
─────────────────────────────────────────────────────────────────────────────
Reads mart table metadata directly from DuckDB and returns it in a format
the agent can inject into its prompt as context.
"""

import duckdb
from typing import Optional
from agent.config import DUCKDB_PATH, MARTS_SCHEMA, ALLOWED_TABLES


def get_table_schema(table_name: str) -> dict:
    """
    Return column names, types and a row count for one mart table.
    """
    con = duckdb.connect(str(DUCKDB_PATH), read_only=True)
    try:
        full_name = f"{MARTS_SCHEMA}.{table_name}"
        columns = con.execute(f"DESCRIBE {full_name}").fetchall()
        count   = con.execute(f"SELECT count(*) FROM {full_name}").fetchone()[0]
        return {
            "table":   table_name,
            "schema":  MARTS_SCHEMA,
            "rows":    count,
            "columns": [
                {"name": col[0], "type": col[1]}
                for col in columns
            ],
        }
    finally:
        con.close()


def get_all_schemas() -> list[dict]:
    """Return schema info for every allowed mart table."""
    return [get_table_schema(t) for t in ALLOWED_TABLES]


def format_schema_for_prompt(table_name: Optional[str] = None) -> str:
    """
    Return a human-readable schema string ready to embed in a prompt.
    If table_name is None, returns all tables.
    """
    tables = (
        [get_table_schema(table_name)]
        if table_name and table_name in ALLOWED_TABLES
        else get_all_schemas()
    )

    lines = []
    for t in tables:
        lines.append(f"Table: {MARTS_SCHEMA}.{t['table']}  ({t['rows']:,} rows)")
        for col in t["columns"]:
            lines.append(f"  - {col['name']}  ({col['type']})")
        lines.append("")

    return "\n".join(lines).strip()


def get_sample_rows(table_name: str, n: int = 3) -> list[dict]:
    """Return n sample rows from a mart table as a list of dicts."""
    if table_name not in ALLOWED_TABLES:
        raise ValueError(f"Table '{table_name}' is not in the allowed list.")

    con = duckdb.connect(str(DUCKDB_PATH), read_only=True)
    try:
        full_name = f"{MARTS_SCHEMA}.{table_name}"
        result = con.execute(
            f"SELECT * FROM {full_name} LIMIT {n}"
        ).fetchdf()
        return result.to_dict(orient="records")
    finally:
        con.close()

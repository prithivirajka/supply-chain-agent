"""
test_api.py

Manual test script for the Supply Chain Agent FastAPI server.
Run this while the server is running at http://localhost:8000.

Usage:
    python test_api.py              runs all tests
    python test_api.py health       runs only the health check
    python test_api.py schema       runs only the schema check
    python test_api.py validation   runs only input validation tests
    python test_api.py query        runs only the agent query tests
"""

import sys
import json
import time
import requests

BASE_URL = "http://localhost:8000"


def green(text):  return f"\033[92m{text}\033[0m"
def red(text):    return f"\033[91m{text}\033[0m"
def dim(text):    return f"\033[2m{text}\033[0m"
def bold(text):   return f"\033[1m{text}\033[0m"

def pass_fail(ok): return green("  PASS") if ok else red("  FAIL")

def section(title):
    print(f"\n{bold(title)}")
    print("-" * 60)


def test_health():
    section("GET /health")
    try:
        r    = requests.get(f"{BASE_URL}/health", timeout=10)
        data = r.json()

        checks = {
            "status 200":       r.status_code == 200,
            "status is ok":     data.get("status") == "ok",
            "model present":    bool(data.get("model")),
            "database present": bool(data.get("database")),
            "6 tables present": len(data.get("tables", [])) == 6,
        }

        for name, ok in checks.items():
            print(f"{pass_fail(ok)}  {name}")

        print(f"\n{dim('Model:')}    {data.get('model')}")
        print(f"{dim('Database:')} {data.get('database')}")
        print(f"{dim('Tables:')}   {', '.join(data.get('tables', []))}")

        return all(checks.values())
    except Exception as e:
        print(red(f"  ERROR: {e}"))
        return False


def test_schema():
    section("GET /schema")
    try:
        r      = requests.get(f"{BASE_URL}/schema", timeout=15)
        data   = r.json()
        tables = data.get("tables", [])

        expected = [
            "mart_orders", "mart_delivery", "mart_sellers",
            "mart_products", "mart_customers", "mart_supply_chain_summary",
        ]

        checks = {
            "status 200":        r.status_code == 200,
            "6 tables returned": len(tables) == 6,
            "all tables present": all(t["table"] in expected for t in tables),
            "columns present":   all("columns" in t for t in tables),
            "row counts present": all("rows" in t for t in tables),
        }

        for name, ok in checks.items():
            print(f"{pass_fail(ok)}  {name}")

        print(f"\n{dim('Row counts:')}")
        for t in tables:
            print(f"  {t['table']:40s} {t['rows']:>8,} rows")

        return all(checks.values())
    except Exception as e:
        print(red(f"  ERROR: {e}"))
        return False


def test_query(question: str, expect_sql: bool = True, expect_rows: bool = True):
    print(f"\n  {dim('Q:')} {question}")
    try:
        start = time.time()
        r     = requests.post(
            f"{BASE_URL}/query",
            json={"question": question},
            timeout=120,
        )
        elapsed = round(time.time() - start, 1)
        data    = r.json()

        checks = {
            "status 200":   r.status_code == 200,
            "answer present": bool(data.get("answer")),
            "no error":     data.get("error") is None,
        }
        if expect_sql:
            checks["sql present"] = bool(data.get("sql_used"))
        if expect_rows:
            checks["rows present"] = data.get("row_count", 0) > 0

        for name, ok in checks.items():
            print(f"  {pass_fail(ok)}  {name}")

        if data.get("answer"):
            preview = data["answer"][:200].replace("\n", " ")
            print(f"\n  {dim('Answer:')} {preview}{'...' if len(data['answer']) > 200 else ''}")

        if data.get("sql_used"):
            sql_preview = data["sql_used"].strip().replace("\n", " ")[:120]
            print(f"  {dim('SQL:')}    {sql_preview}...")

        print(f"  {dim('Rows:')}   {data.get('row_count', 0)}  |  {dim('Time:')} {elapsed}s")

        return all(checks.values())
    except Exception as e:
        print(red(f"  ERROR: {e}"))
        return False


def test_validation():
    section("POST /query - input validation")
    results = []

    r  = requests.post(f"{BASE_URL}/query", json={"question": "hi"}, timeout=10)
    ok = r.status_code == 422
    print(f"{pass_fail(ok)}  rejects question under 5 characters (got {r.status_code})")
    results.append(ok)

    r  = requests.post(f"{BASE_URL}/query", json={"question": ""}, timeout=10)
    ok = r.status_code == 422
    print(f"{pass_fail(ok)}  rejects empty question (got {r.status_code})")
    results.append(ok)

    r  = requests.post(f"{BASE_URL}/query", json={}, timeout=10)
    ok = r.status_code == 422
    print(f"{pass_fail(ok)}  rejects missing question field (got {r.status_code})")
    results.append(ok)

    return all(results)


def test_queries():
    section("POST /query - agent questions")
    questions = [
        ("What is the overall on-time delivery rate?",      True, True),
        ("How many total orders are in the dataset?",       True, True),
        ("Which states have the worst late delivery rate?", True, True),
        ("Who are the top 5 sellers by revenue?",          True, True),
        ("What is the average order value?",                True, True),
    ]

    results = []
    for question, expect_sql, expect_rows in questions:
        ok = test_query(question, expect_sql, expect_rows)
        results.append(ok)
        print()

    return all(results)


def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else "all"

    print(bold(f"\nSupply Chain Agent - API Test Suite"))
    print(f"Target: {BASE_URL}")
    print(f"Mode:   {mode}")

    try:
        requests.get(f"{BASE_URL}/health", timeout=5)
    except requests.ConnectionError:
        print(red(f"\nERROR: Server not reachable at {BASE_URL}"))
        print("Make sure the server is running with: uvicorn api.main:app")
        sys.exit(1)

    results = {}

    if mode in ("all", "health"):
        results["health"] = test_health()

    if mode in ("all", "schema"):
        results["schema"] = test_schema()

    if mode in ("all", "validation"):
        results["validation"] = test_validation()

    if mode in ("all", "query"):
        results["queries"] = test_queries()

    section("Summary")
    all_pass = True
    for name, ok in results.items():
        print(f"{pass_fail(ok)}  {name}")
        if not ok:
            all_pass = False

    print()
    if all_pass:
        print(green("All tests passed."))
    else:
        print(red("Some tests failed."))
        sys.exit(1)


if __name__ == "__main__":
    main()

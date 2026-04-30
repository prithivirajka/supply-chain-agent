"""
utils.py

HTTP helpers for calling the FastAPI backend.
"""

import requests
import streamlit as st

API_BASE       = "http://localhost:8000"
TIMEOUT_QUERY  = 180
TIMEOUT_FAST   = 15


def get_health() -> dict | None:
    """Check if the API is up. Returns the health payload or None."""
    try:
        r = requests.get(f"{API_BASE}/health", timeout=TIMEOUT_FAST)
        if r.status_code == 200:
            return r.json()
    except requests.ConnectionError:
        pass
    return None


@st.cache_data(ttl=300)
def get_schema() -> list[dict]:
    """Fetch all mart table schemas from the API. Cached for 5 minutes."""
    try:
        r = requests.get(f"{API_BASE}/schema", timeout=TIMEOUT_FAST)
        if r.status_code == 200:
            return r.json().get("tables", [])
    except Exception:
        pass
    return []


def post_query(question: str) -> dict:
    """Send a question to the agent and return the full response dict."""
    try:
        r = requests.post(
            f"{API_BASE}/query",
            json={"question": question},
            timeout=TIMEOUT_QUERY,
        )
        if r.status_code == 200:
            return r.json()
        return {"error": f"API error {r.status_code}: {r.text}"}
    except requests.ConnectionError:
        return {"error": "Cannot connect to API. Is the FastAPI server running?"}
    except requests.Timeout:
        return {"error": "Request timed out after 3 minutes. Try a simpler question."}
    except Exception as e:
        return {"error": str(e)}

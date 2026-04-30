"""
main.py
─────────────────────────────────────────────────────────────────────────────
FastAPI application exposing the supply chain agent as a REST API.

Endpoints:
  GET  /health    — liveness check, confirms DB + model are reachable
  GET  /schema    — returns all mart table schemas
  POST /query     — ask a natural language question, get an answer + SQL
"""

from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from agent.agent import build_agent, run_query
from agent.schema_context import get_all_schemas
from agent.config import MODEL_NAME, DUCKDB_PATH, ALLOWED_TABLES
from api.models import QueryRequest, QueryResponse, SchemaResponse, HealthResponse

# ── App lifecycle ─────────────────────────────────────────────────────────

_agent = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Build the agent once at startup, tear down on shutdown."""
    global _agent
    print("Building LangChain agent...")
    _agent = build_agent()
    print(f"Agent ready  model={MODEL_NAME}  db={DUCKDB_PATH}")
    yield
    _agent = None
    print("Agent shut down.")


# ── App ───────────────────────────────────────────────────────────────────

app = FastAPI(
    title="Supply Chain Agent API",
    description=(
        "Natural language querying of Olist supply chain data "
        "powered by LangChain + Claude + DuckDB."
    ),
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── Endpoints ─────────────────────────────────────────────────────────────

@app.get("/health", response_model=HealthResponse, tags=["meta"])
def health():
    """Liveness check — confirms the agent, database and model are ready."""
    if _agent is None:
        raise HTTPException(status_code=503, detail="Agent not initialised.")
    return HealthResponse(
        status="ok",
        model=MODEL_NAME,
        database=str(DUCKDB_PATH),
        tables=ALLOWED_TABLES,
    )


@app.get("/schema", response_model=SchemaResponse, tags=["meta"])
def schema():
    """Returns column metadata for all mart tables."""
    try:
        tables = get_all_schemas()
        return SchemaResponse(tables=tables)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/query", response_model=QueryResponse, tags=["agent"])
def query(request: QueryRequest):
    """
    Ask a natural language question about the supply chain.

    The agent will:
    1. Look up the relevant table schema
    2. Write and execute SQL against DuckDB
    3. Return a plain-English answer with the SQL used and raw rows
    """
    if _agent is None:
        raise HTTPException(status_code=503, detail="Agent not initialised.")

    result = run_query(_agent, request.question)

    if result["error"]:
        raise HTTPException(status_code=500, detail=result["error"])

    return QueryResponse(
        question=request.question,
        answer=result["answer"],
        sql_used=result["sql_used"],
        rows=result["rows"],
        row_count=result["row_count"],
        execution_time_sec=result["execution_time_sec"],
        error=None,
    )

"""
main.py

FastAPI application exposing the supply chain agent as a REST API.

Endpoints:
  GET  /health   checks that the API, database, and model are reachable
  GET  /schema   returns column metadata for all mart tables
  POST /query    takes a natural language question and returns an answer with SQL and data
"""

from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from agent.agent import build_agent, run_query
from agent.schema_context import get_all_schemas
from agent.config import MODEL_NAME, DUCKDB_PATH, ALLOWED_TABLES
from api.models import QueryRequest, QueryResponse, SchemaResponse, HealthResponse

_agent = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _agent
    print("Building agent...")
    _agent = build_agent()
    print(f"Agent ready. Model: {MODEL_NAME}  DB: {DUCKDB_PATH}")
    yield
    _agent = None


app = FastAPI(
    title="Supply Chain Agent API",
    description="Natural language querying of Olist supply chain data powered by LangChain, Claude, and DuckDB.",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health", response_model=HealthResponse, tags=["meta"])
def health():
    """Confirms the agent, database, and model are ready."""
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
    The agent looks up the schema, writes SQL, executes it, and returns an answer.
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

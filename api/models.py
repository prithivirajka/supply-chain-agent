"""
models.py

Pydantic request and response schemas for the FastAPI endpoints.
"""

from typing import Any, Optional
from pydantic import BaseModel, Field


class QueryRequest(BaseModel):
    question: str = Field(
        ...,
        min_length=5,
        max_length=500,
        description="Natural language supply chain question",
        examples=["Which sellers have the highest late delivery rate?"],
    )


class QueryResponse(BaseModel):
    question:           str
    answer:             str
    sql_used:           Optional[str]  = None
    rows:               Optional[list] = None
    row_count:          int            = 0
    execution_time_sec: float          = 0.0
    error:              Optional[str]  = None


class SchemaResponse(BaseModel):
    tables: list[dict[str, Any]]


class HealthResponse(BaseModel):
    status:   str
    model:    str
    database: str
    tables:   list[str]

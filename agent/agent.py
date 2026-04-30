"""
agent.py
─────────────────────────────────────────────────────────────────────────────
Builds and returns a LangChain agent using the current recommended API.

LangChain >= 1.0 uses `langchain.agents.create_agent` (replaces the
deprecated `langgraph.prebuilt.create_react_agent`).
"""

import time
import json
from langchain_anthropic import ChatAnthropic
from langchain.agents import create_agent
from langchain_core.messages import HumanMessage

from agent.config import (
    ANTHROPIC_API_KEY,
    MODEL_NAME,
    MAX_TOKENS,
    TEMPERATURE,
    MAX_ITERATIONS,
    VERBOSE,
    validate,
)
from agent.tools import TOOLS
from agent.prompts import SYSTEM_PROMPT


def build_agent():
    """
    Construct and return the LangChain agent graph.
    Called once at API startup.
    """
    validate()

    llm = ChatAnthropic(
        model=MODEL_NAME,
        anthropic_api_key=ANTHROPIC_API_KEY,
        max_tokens=MAX_TOKENS,
        temperature=TEMPERATURE,
    )

    # create_agent is the current API in LangChain >= 1.0
    # It replaces the deprecated langgraph.prebuilt.create_react_agent
    agent = create_agent(
        model=llm,
        tools=TOOLS,
        system_prompt=SYSTEM_PROMPT,
    )

    return agent


def run_query(agent, question: str) -> dict:
    """
    Run a natural language question through the agent.

    Returns:
        {
          "answer":             str,
          "sql_used":           str | None,
          "rows":               list | None,
          "row_count":          int,
          "execution_time_sec": float,
          "error":              str | None,
        }
    """
    start = time.time()

    try:
        result = agent.invoke(
            {"messages": [HumanMessage(content=question)]},
            config={"recursion_limit": MAX_ITERATIONS * 2},
        )

        messages  = result.get("messages", [])
        sql_used  = None
        rows      = None
        row_count = 0
        answer    = ""

        for msg in messages:
            msg_type = type(msg).__name__

            # AIMessage with tool_calls → extract SQL from run_sql calls
            if msg_type == "AIMessage" and hasattr(msg, "tool_calls") and msg.tool_calls:
                for tc in msg.tool_calls:
                    if tc.get("name") == "run_sql":
                        args     = tc.get("args", {})
                        sql_used = args.get("sql") or str(args)

            # ToolMessage → extract rows from run_sql results
            if msg_type == "ToolMessage" and sql_used:
                try:
                    obs       = json.loads(msg.content)
                    if "rows" in obs:
                        rows      = obs.get("rows")
                        row_count = obs.get("row_count", 0)
                except (json.JSONDecodeError, TypeError):
                    pass

            # Final AIMessage with no tool calls = the answer
            if msg_type == "AIMessage":
                has_tool_calls = hasattr(msg, "tool_calls") and bool(msg.tool_calls)
                if not has_tool_calls and msg.content:
                    answer = msg.content

        if VERBOSE:
            print(f"\n[Agent] Q: {question}")
            print(f"[Agent] SQL: {sql_used}")
            print(f"[Agent] Rows: {row_count}")
            print(f"[Agent] A: {answer[:200]}")

        return {
            "answer":             answer,
            "sql_used":           sql_used,
            "rows":               rows,
            "row_count":          row_count,
            "execution_time_sec": round(time.time() - start, 2),
            "error":              None,
        }

    except Exception as e:
        return {
            "answer":             "",
            "sql_used":           None,
            "rows":               None,
            "row_count":          0,
            "execution_time_sec": round(time.time() - start, 2),
            "error":              str(e),
        }

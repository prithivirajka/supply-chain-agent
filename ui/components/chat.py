"""
chat.py

Renders chat messages and handles the query input.
"""

import pandas as pd
import streamlit as st


def render_user_message(question: str):
    """Render a user message bubble."""
    with st.chat_message("user"):
        st.markdown(question)


def render_agent_message(result: dict):
    """Render a full agent response including the answer, data table, SQL, and metadata."""
    with st.chat_message("assistant"):

        if result.get("error"):
            st.error(f"Error: {result['error']}")
            return

        answer = result.get("answer", "")
        if answer:
            st.markdown(answer)
        else:
            st.warning("No answer returned from the agent.")

        rows      = result.get("rows")
        row_count = result.get("row_count", 0)

        if rows and row_count > 0:
            try:
                df = pd.DataFrame(rows)
                for col in df.select_dtypes(include="float").columns:
                    df[col] = df[col].round(4)
                with st.expander(
                    f"View data ({row_count} row{'s' if row_count != 1 else ''})",
                    expanded=row_count <= 10,
                ):
                    st.dataframe(df, use_container_width=True, hide_index=True)
            except Exception:
                pass

        sql = result.get("sql_used")
        if sql:
            with st.expander("View SQL", expanded=False):
                st.code(sql.strip(), language="sql")

        exec_time  = result.get("execution_time_sec", 0)
        meta_parts = []
        if row_count:
            meta_parts.append(f"{row_count} row{'s' if row_count != 1 else ''}")
        if exec_time:
            meta_parts.append(f"{exec_time}s")
        if meta_parts:
            st.caption(" · ".join(meta_parts))


def render_chat_history(history: list):
    """Re-render all messages from session state history."""
    for msg in history:
        if msg["role"] == "user":
            render_user_message(msg["content"])
        else:
            render_agent_message(msg["content"])


def render_welcome():
    """Show a welcome message when the chat is empty."""
    with st.chat_message("assistant"):
        st.markdown(
            "Hi! I'm your supply chain analytics agent. I can answer questions "
            "about the Olist Brazilian E-Commerce dataset covering orders, deliveries, "
            "sellers, products, and customers.\n\n"
            "Try asking something like:\n"
            "- *What is the overall on-time delivery rate?*\n"
            "- *Who are the top 10 sellers by revenue?*\n"
            "- *Which states have the worst late delivery rate?*"
        )

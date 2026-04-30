"""
app.py

Main Streamlit application for the Supply Chain Analytics Agent.

Run with:
    streamlit run ui/app.py

The FastAPI server must be running first:
    uvicorn api.main:app
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
import streamlit as st
from ui.components.sidebar import render_sidebar
from ui.components.chat import (
    render_chat_history,
    render_user_message,
    render_welcome,
)
from ui.components.utils import post_query

st.set_page_config(
    page_title="Supply Chain Agent",
    page_icon="📦",
    layout="wide",
    initial_sidebar_state="expanded",
)

if "messages" not in st.session_state:
    st.session_state.messages = []

if "pending_question" not in st.session_state:
    st.session_state.pending_question = None

suggested = render_sidebar()
if suggested:
    st.session_state.pending_question = suggested

st.title("Supply Chain Agent")
st.caption(
    "Ask natural language questions about the Olist supply chain dataset. "
    "Powered by Claude, LangChain, and DuckDB."
)

st.divider()

if not st.session_state.messages:
    render_welcome()
else:
    render_chat_history(st.session_state.messages)

question = st.chat_input("Ask a supply chain question...")

if st.session_state.pending_question and not question:
    question = st.session_state.pending_question
    st.session_state.pending_question = None

if question:
    st.session_state.messages.append({"role": "user", "content": question})
    render_user_message(question)

    with st.chat_message("assistant"):
        with st.spinner("Thinking..."):
            result = post_query(question)

        if result.get("error"):
            st.error(f"Error: {result['error']}")
        else:
            answer = result.get("answer", "")
            if answer:
                st.markdown(answer)

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

    st.session_state.messages.append({"role": "assistant", "content": result})

"""
sidebar.py

Renders the left sidebar: connection status, KPI cards, and suggested questions.
"""

import streamlit as st
from ui.components.utils import get_health, get_schema


SUGGESTED_QUESTIONS = [
    "What is the overall on-time delivery rate?",
    "Who are the top 10 sellers by revenue?",
    "Which states have the worst late delivery rate?",
    "What are the top product categories by revenue?",
    "How did monthly order volume grow from 2016 to 2018?",
    "What percentage of customers are repeat buyers?",
    "Which month had the worst delivery performance?",
    "What is the average freight cost as a percentage of order value?",
]

STATIC_KPIS = {
    "Total Orders":     "99,441",
    "Delivered Orders": "96,478",
    "On-Time Rate":     "93.23%",
    "Avg Order Value":  "R$ 160",
    "Avg Review Score": "4.09 / 5",
    "Unique Customers": "94,990",
    "Active Sellers":   "3,095",
    "Data Period":      "2016 to 2018",
}


def render_sidebar():
    """Render the full sidebar. Returns a question string if a suggested button was clicked, otherwise None."""

    with st.sidebar:
        st.markdown("## Supply Chain Agent")
        st.caption("Olist Brazilian E-Commerce, 2016 to 2018")

        st.divider()

        # Connection status
        health = get_health()
        if health:
            st.success("API connected", icon="✅")
            st.caption(f"Model: `{health.get('model', 'unknown')}`")
        else:
            st.error("API offline", icon="❌")
            st.caption("Start the server: `uvicorn api.main:app`")

        st.divider()

        # KPI cards
        st.markdown("#### Dataset overview")

        col1, col2 = st.columns(2)
        items = list(STATIC_KPIS.items())

        for i, (label, value) in enumerate(items):
            col = col1 if i % 2 == 0 else col2
            with col:
                st.metric(label=label, value=value)

        st.divider()

        # Schema explorer
        with st.expander("View table schemas", expanded=False):
            tables = get_schema()
            if tables:
                for t in tables:
                    st.markdown(f"**{t['table']}** ({t['rows']:,} rows)")
                    cols = [c["name"] for c in t.get("columns", [])]
                    st.caption(", ".join(cols[:8]) + ("..." if len(cols) > 8 else ""))
            else:
                st.caption("Schema unavailable. Check the API connection.")

        st.divider()

        # Suggested questions
        st.markdown("#### Try asking")
        question_to_ask = None

        for q in SUGGESTED_QUESTIONS:
            label = q if len(q) <= 48 else q[:45] + "..."
            if st.button(label, key=f"suggested_{q}", use_container_width=True):
                question_to_ask = q

        st.divider()
        st.caption("Built with dbt, DuckDB, LangChain, and Claude")

    return question_to_ask

from __future__ import annotations

from pathlib import Path
import os
import streamlit as st

REPO_ROOT = Path(__file__).resolve().parents[2]

st.header("Overview")
st.write(
    """
This app is a live showcase of your **Text Processing Service**:
- Core pipeline: processor, filter, rate limiter
- Advanced modes: async processing, persistence, distributed mode
- Observability: tracing/metrics (as implemented in your API)
- Tests: unit/integration/distributed coverage
"""
)

st.subheader("How to run locally")

st.code(
    """
# 1) Start API (example)
uvicorn src.api:app --reload --port 8000

# 2) Start Streamlit
streamlit run streamlit_app/app.py

# 3) Run tests
python -m pytest -q
""".strip(),
    language="bash",
)

st.subheader("Repository structure")
cols = st.columns(2)
with cols[0]:
    st.write("**src/**")
    st.write("- api.py (FastAPI routes + middleware)")
    st.write("- pipeline.py (orchestration)")
    st.write("- processor.py (word stats)")
    st.write("- filter.py (content flags)")
    st.write("- rate_limiter.py (token bucket)")
with cols[1]:
    st.write("**tests/**")
    st.write("- test_integration.py (API lifecycle)")
    st.write("- test_async_processing.py (async mode)")
    st.write("- test_persistence.py (restart recovery)")
    st.write("- test_distributed_mode.py (Redis shared state)")

st.subheader("Environment notes")
st.info(
    """
If **test_distributed_mode** is skipped, it means **REDIS_URL** is not set.
You can run Redis with docker-compose and then run:
REDIS_URL="redis://localhost:6379/0" DISTRIBUTED_MODE=1 python -m pytest -q -rs
"""
)

st.subheader("Current env (from this Streamlit process)")
keys = [
    "ASYNC_MODE",
    "DISTRIBUTED_MODE",
    "REDIS_URL",
    "REDIS_PARTITION_POLICY",
    "PERSISTENCE_DB_PATH",
]
for k in keys:
    st.write(f"**{k}**: {os.environ.get(k, 'not set')}")

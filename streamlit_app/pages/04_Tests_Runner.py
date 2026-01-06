from __future__ import annotations

import os
import subprocess
from pathlib import Path
from typing import List

import streamlit as st

REPO_ROOT = Path(__file__).resolve().parents[2]


def run_cmd(cmd: List[str], env: dict | None = None) -> tuple[int, str]:
    p = subprocess.run(
        cmd,
        cwd=str(REPO_ROOT),
        env=env or os.environ.copy(),
        capture_output=True,
        text=True,
    )
    out = p.stdout + ("\n" + p.stderr if p.stderr else "")
    return p.returncode, out


st.header("Tests Runner")

st.write(
    """
Run pytest from the UI. This is helpful to demo your test suite
and advanced feature coverage.
"""
)

st.subheader("Environment for tests")
col1, col2 = st.columns(2)
with col1:
    redis_url = st.text_input("REDIS_URL (for distributed test)", value=os.environ.get("REDIS_URL", ""))
with col2:
    distributed_mode = st.selectbox("DISTRIBUTED_MODE", ["0", "1"], index=1 if os.environ.get("DISTRIBUTED_MODE") == "1" else 0)

env = os.environ.copy()
if redis_url.strip():
    env["REDIS_URL"] = redis_url.strip()
env["DISTRIBUTED_MODE"] = distributed_mode

st.divider()

colA, colB, colC = st.columns(3)

with colA:
    if st.button("Run all (pytest -q)", use_container_width=True):
        code, out = run_cmd(["python", "-m", "pytest", ""], env=env)
        st.write(f"Exit code: **{code}**")
        st.code(out, language="text")

with colB:
    if st.button("Run integration only", use_container_width=True):
        code, out = run_cmd(["python", "-m", "pytest", "", "-m", "integration"], env=env)
        st.write(f"Exit code: **{code}**")
        st.code(out, language="text")

with colC:
    if st.button("Show skips (-rs)", use_container_width=True):
        code, out = run_cmd(["python", "-m", "pytest", "", "-rs"], env=env)
        st.write(f"Exit code: **{code}**")
        st.code(out, language="text")

st.subheader("Distributed mode test only")
st.caption("This will fail if Redis is not reachable or REDIS_URL is not set.")
if st.button("Run distributed test file", use_container_width=True):
    code, out = run_cmd(["python", "-m", "pytest", "", "tests/test_distributed_mode.py", "-rs"], env=env)
    st.write(f"Exit code: **{code}**")
    st.code(out, language="text")

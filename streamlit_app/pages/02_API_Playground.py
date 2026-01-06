from __future__ import annotations

import json
import os
import time
from typing import Any, Dict, List, Optional

import requests
import streamlit as st


def _safe_secrets_get(key: str, default: Optional[str] = None) -> Optional[str]:
    """
    Streamlit raises StreamlitSecretNotFoundError if no secrets.toml exists.
    This wrapper prevents the app from crashing when secrets aren't configured.
    """
    try:
        # st.secrets behaves like a mapping but can throw if secrets file is absent
        return st.secrets.get(key, default)  # type: ignore[attr-defined]
    except Exception:
        return default


def _api_base_url() -> str:
    # Prefer Streamlit secrets, then env, then fallback
    return (
        _safe_secrets_get("API_BASE_URL", None)
        or os.environ.get("API_BASE_URL", None)
        or "http://api:8000"
    ).rstrip("/")


def _req(method: str, url: str, *, timeout_s: int, json_body: Optional[Dict[str, Any]] = None):
    try:
        r = requests.request(method, url, json=json_body, timeout=timeout_s)
        return r, None
    except requests.RequestException as e:
        return None, str(e)


def _pretty(obj: Any) -> str:
    try:
        return json.dumps(obj, indent=2, ensure_ascii=False)
    except Exception:
        return str(obj)


st.set_page_config(page_title="API Playground", layout="wide")

st.title("API Playground")
st.caption(
    "This page helps you create streams, send chunks, and fetch stats for multiple streams without writing curl commands."
)

# ---------- Sidebar config ----------
with st.sidebar:
    st.header("Connection")

    # Use safe base URL resolution
    base = st.text_input(
        "API Base URL",
        value=_api_base_url(),
        help="In Docker Compose, this is usually http://api:8000. Locally: http://localhost:8000",
    )

    timeout_s = st.number_input("Timeout seconds", min_value=1, max_value=120, value=20, step=1)

    st.divider()
    st.header("How this works")
    st.markdown(
        """
**Streams are independent.**  
You can create many stream IDs. Each stream maintains its own word stats and flags.

Typical flow:
1. Create stream → get `stream_id`
2. POST chunks to that stream
3. GET stats anytime
4. Delete stream when done
        """.strip()
    )

# ---------- Session state ----------
if "streams" not in st.session_state:
    # list of dict: {"id": "...", "label": "...", "created_at": epoch}
    st.session_state.streams = []

if "selected_stream_id" not in st.session_state:
    st.session_state.selected_stream_id = None

if "history" not in st.session_state:
    # per stream history
    st.session_state.history = {}  # stream_id -> list[dict]

# ---------- Helpers ----------
def _add_history(stream_id: str, entry: Dict[str, Any]) -> None:
    st.session_state.history.setdefault(stream_id, [])
    st.session_state.history[stream_id].insert(0, entry)


def _stream_options() -> List[str]:
    return [s["id"] for s in st.session_state.streams]


def _get_stream_label(stream_id: str) -> str:
    for s in st.session_state.streams:
        if s["id"] == stream_id:
            return s["label"]
    return stream_id


# ---------- Main layout ----------
left, right = st.columns([1, 1], gap="large")

with left:
    st.subheader("Stream lifecycle")

    c1, c2 = st.columns([1, 1])
    with c1:
        if st.button("Create stream", use_container_width=True):
            url = f"{base}/streams"
            r, err = _req("POST", url, timeout_s=int(timeout_s))
            if err:
                st.error(f"Could not connect to API: {err}")
            else:
                if r.status_code >= 400:
                    st.error(f"Create failed: {r.status_code}\n{r.text}")
                else:
                    payload = r.json()
                    sid = payload.get("stream_id") or payload.get("id") or payload.get("streamId")
                    if not sid:
                        st.error(f"API response missing stream_id:\n{r.text}")
                    else:
                        label = f"stream {len(st.session_state.streams) + 1}"
                        st.session_state.streams.insert(0, {"id": sid, "label": label, "created_at": time.time()})
                        st.session_state.selected_stream_id = sid
                        _add_history(sid, {"action": "create", "status": r.status_code, "response": payload})
                        st.success(f"Created: {sid}")

    with c2:
        if st.button("Refresh selected stats", use_container_width=True):
            sid = st.session_state.selected_stream_id
            if not sid:
                st.warning("Select a stream first.")
            else:
                url = f"{base}/streams/{sid}/stats"
                r, err = _req("GET", url, timeout_s=int(timeout_s))
                if err:
                    st.error(f"Request failed: {err}")
                else:
                    _add_history(
                        sid,
                        {"action": "get_stats", "status": r.status_code, "response": r.json() if r.ok else r.text},
                    )
                    if r.status_code >= 400:
                        st.error(f"Stats failed: {r.status_code}\n{r.text}")
                    else:
                        st.success("Stats fetched (see right panel).")

    st.divider()

    # Stream selection
    if st.session_state.streams:
        options = _stream_options()
        default_ix = 0
        if st.session_state.selected_stream_id in options:
            default_ix = options.index(st.session_state.selected_stream_id)

        selected = st.selectbox(
            "Select a stream",
            options=options,
            index=default_ix,
            format_func=lambda sid: f"{_get_stream_label(sid)}  •  {sid[:8]}…",
        )
        st.session_state.selected_stream_id = selected

        d1, d2 = st.columns([1, 1])
        with d1:
            new_label = st.text_input("Rename selected", value=_get_stream_label(selected))
        with d2:
            if st.button("Save name", use_container_width=True):
                for s in st.session_state.streams:
                    if s["id"] == selected:
                        s["label"] = new_label.strip() or s["label"]
                st.success("Updated name.")

        if st.button("Delete selected stream", type="secondary", use_container_width=True):
            sid = st.session_state.selected_stream_id
            url = f"{base}/streams/{sid}"
            r, err = _req("DELETE", url, timeout_s=int(timeout_s))
            if err:
                st.error(f"Delete failed: {err}")
            else:
                _add_history(sid, {"action": "delete", "status": r.status_code, "response": r.text})
                if r.status_code >= 400:
                    st.error(f"Delete failed: {r.status_code}\n{r.text}")
                else:
                    st.success("Deleted.")
                    st.session_state.streams = [s for s in st.session_state.streams if s["id"] != sid]
                    st.session_state.selected_stream_id = (
                        st.session_state.streams[0]["id"] if st.session_state.streams else None
                    )

    else:
        st.info("No streams yet. Click **Create stream** to start.")

with right:
    st.subheader("Add chunks and view results")

    sid = st.session_state.selected_stream_id
    if not sid:
        st.warning("Create or select a stream on the left.")
    else:
        st.markdown(f"**Selected:** `{sid}`")

        chunk_text = st.text_area("Chunk text", value="hello world", height=140)

        priority = st.selectbox("Priority", options=["normal", "high"], index=0)

        c1, c2 = st.columns([1, 1])
        with c1:
            if st.button("POST chunk", use_container_width=True):
                url = f"{base}/streams/{sid}/chunks"
                r, err = _req(
                    "POST",
                    url,
                    timeout_s=int(timeout_s),
                    json_body={"text": chunk_text, "priority": priority},
                )
                if err:
                    st.error(f"POST failed: {err}")
                else:
                    resp = r.json() if r.headers.get("content-type", "").startswith("application/json") else r.text
                    _add_history(
                        sid,
                        {
                            "action": "post_chunk",
                            "status": r.status_code,
                            "request": {"text": chunk_text, "priority": priority},
                            "response": resp,
                        },
                    )
                    if r.status_code >= 400:
                        st.error(f"POST failed: {r.status_code}\n{r.text}")
                    else:
                        st.success("Chunk sent.")
        with c2:
            if st.button("GET stats", use_container_width=True):
                url = f"{base}/streams/{sid}/stats"
                r, err = _req("GET", url, timeout_s=int(timeout_s))
                if err:
                    st.error(f"GET failed: {err}")
                else:
                    resp = r.json() if r.ok else r.text
                    _add_history(sid, {"action": "get_stats", "status": r.status_code, "response": resp})
                    if r.status_code >= 400:
                        st.error(f"Stats failed: {r.status_code}\n{r.text}")
                    else:
                        st.success("Stats updated below.")

        st.divider()

        # Show stats
        url = f"{base}/streams/{sid}/stats"
        r, err = _req("GET", url, timeout_s=int(timeout_s))
        if err:
            st.error(f"Could not fetch stats: {err}")
        else:
            if r.status_code >= 400:
                st.error(f"Stats error: {r.status_code}\n{r.text}")
            else:
                stats_payload = r.json()
                st.markdown("### Current stats")
                c1, c2, c3 = st.columns(3)
                c1.metric("word_count", stats_payload.get("word_count", 0))
                c2.metric("unique_words", stats_payload.get("unique_words", 0))
                c3.metric("avg_word_length", stats_payload.get("avg_word_length", 0.0))

                flags = stats_payload.get("flags", []) or []
                st.markdown("### Flags")
                if not flags:
                    st.caption("No flags detected yet.")
                else:
                    st.dataframe(flags, use_container_width=True)

        st.divider()

        st.markdown("### Request history")
        hist = st.session_state.history.get(sid, [])
        if not hist:
            st.caption("No actions yet for this stream.")
        else:
            for i, h in enumerate(hist[:15], start=1):
                with st.expander(f"{i}. {h.get('action')}  •  status {h.get('status')}"):
                    if "request" in h:
                        st.code(_pretty(h["request"]), language="json")
                    st.code(_pretty(h.get("response")), language="json")

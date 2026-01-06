import os
import streamlit as st

st.set_page_config(page_title="Text Processing Service Demo", layout="wide")

st.title("Text Processing Service Demo")
st.caption("Demo UI for core features, advanced features, and tests.")

api_base = os.environ.get("API_BASE_URL", "http://localhost:8000")

st.markdown(
    f"""
### What you can do here

Use the left sidebar pages:

- **API Playground**  
  Create multiple streams, send chunks, and view stats.

- **Features and Advanced**  
  (Add your docs and screenshots here)

- **Tests Runner**  
  (Optional: run pytest inside container or show last results)

### Your API Base URL
`{api_base}`

If you are running Docker Compose, your Streamlit container should use:  
`http://api:8000`
"""
)

# app.py — Study Request Submission (Databricks App)

import streamlit as st
import uuid
import json
import urllib.request
import urllib.error
import pandas as pd                          # FIX: moved from inside try block
from datetime import datetime, timezone
from databricks import sql as databricks_sql
import os

# ── Config ────────────────────────────────────────────────────────────────────
CATALOG    = "sscd_catalog"
SCHEMA     = "sscd_ingestion"
TABLE      = "study_requests"
FULL_TABLE = f"{CATALOG}.{SCHEMA}.{TABLE}"
JOB_ID     = 1099541209316497

# FIX: strip trailing slash; these are injected by Databricks Apps automatically
WORKSPACE_HOST = os.environ.get("DATABRICKS_HOST", "").rstrip("/")
TOKEN          = os.environ.get("DATABRICKS_TOKEN", "")

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Study Request Submission",
    page_icon="🧬",
    layout="wide"
)

# ── Styling ───────────────────────────────────────────────────────────────────
st.markdown("""
<style>
    .main { background-color: #f5f7fa; }
    .block-container { padding-top: 2rem; padding-bottom: 2rem; }
    .stButton > button { border-radius: 6px; font-weight: 600; font-size: 14px; height: 38px; }
    .stTextInput > div > div > input { border-radius: 5px; font-size: 13px; }
    div[data-testid="stHorizontalBlock"] { align-items: flex-end; }
    .status-box { padding: 12px 16px; border-radius: 8px; font-size: 14px; margin-top: 12px; }
    .status-success { background: #f0fdf4; border: 1px solid #86efac; color: #16a34a; }
    .status-error   { background: #fef2f2; border: 1px solid #fca5a5; color: #dc2626; }
</style>
""", unsafe_allow_html=True)

# ── Helpers ───────────────────────────────────────────────────────────────────
def get_connection():
    return databricks_sql.connect(
        server_hostname=os.environ["DATABRICKS_HOST"],
        http_path=os.environ["DATABRICKS_HTTP_PATH"]
        # FIX: no token= here — Databricks Apps uses OAuth automatically
    )

def _esc(val: str) -> str:
    """FIX: escape single quotes to prevent broken SQL strings."""
    return str(val).replace("'", "''")

def insert_row(req_id, study_id, domain, details, requested_by, now):
    # FIX: use parameterised-style escaping on every value
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                INSERT INTO {FULL_TABLE}
                    (request_id, study_id, therapeutic_domain, details,
                     requested_by, requested_at, status, workflow_run_id)
                VALUES (
                    '{_esc(req_id)}', '{_esc(study_id)}', '{_esc(domain)}',
                    '{_esc(details)}', '{_esc(

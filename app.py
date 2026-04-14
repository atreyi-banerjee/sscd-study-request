# Databricks notebook source
# DBTITLE 1,Imports
import streamlit as st
import uuid
import json
import urllib.request
import urllib.error
from datetime import datetime, timezone
from databricks import sql as databricks_sql
from databricks.sdk.runtime import dbutils
import os

# COMMAND ----------

# DBTITLE 1,Config
CATALOG        = "sscd_catalog"
SCHEMA         = "sscd_ingestion"
TABLE          = "study_requests"
FULL_TABLE     = f"{CATALOG}.{SCHEMA}.{TABLE}"
JOB_ID         = 1099541209316497
 
# These are injected automatically when running inside a Databricks App
WORKSPACE_HOST = os.environ.get("DATABRICKS_HOST", "")
TOKEN          = os.environ.get("DATABRICKS_TOKEN", "")

# COMMAND ----------

# DBTITLE 1,Page config
st.set_page_config(
    page_title = "Study Request Submission",
    page_icon  = "🧬",
    layout     = "wide"
)

# COMMAND ----------

# DBTITLE 1,Styling
st.markdown("""
<style>
    .main { background-color: #f5f7fa; }
    .block-container { padding-top: 2rem; padding-bottom: 2rem; }
    .stButton > button {
        border-radius: 6px;
        font-weight: 600;
        font-size: 14px;
        height: 38px;
    }
    .stTextInput > div > div > input {
        border-radius: 5px;
        font-size: 13px;
    }
    div[data-testid="stHorizontalBlock"] {
        align-items: flex-end;
    }
    .status-box {
        padding: 12px 16px;
        border-radius: 8px;
        font-size: 14px;
        margin-top: 12px;
    }
    .status-success {
        background: #f0fdf4;
        border: 1px solid #86efac;
        color: #16a34a;
    }
    .status-error {
        background: #fef2f2;
        border: 1px solid #fca5a5;
        color: #dc2626;
    }
</style>
""", unsafe_allow_html=True)

# COMMAND ----------

# DBTITLE 1,Helpers
def get_connection():
    return databricks_sql.connect(
        server_hostname = WORKSPACE_HOST,
        http_path       = os.environ.get("DATABRICKS_HTTP_PATH", ""),
        access_token    = TOKEN
    )
 
def ensure_table():
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {FULL_TABLE} (
                    request_id         STRING,
                    study_id           STRING,
                    therapeutic_domain STRING,
                    details            STRING,
                    requested_by       STRING,
                    requested_at       TIMESTAMP,
                    status             STRING,
                    workflow_run_id    BIGINT
                ) USING DELTA
            """)
 
def insert_row(req_id, study_id, domain, details, requested_by, now):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                INSERT INTO {FULL_TABLE} VALUES (
                    '{req_id}', '{study_id}', '{domain}', '{details}',
                    '{requested_by}', '{now.isoformat()}',
                    'PENDING', NULL
                )
            """)
 
def update_status(req_id, run_id):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                UPDATE {FULL_TABLE}
                SET    status = 'TRIGGERED', workflow_run_id = {run_id}
                WHERE  request_id = '{req_id}'
            """)
 
def trigger_workflow(req_id, study_id, domain, details):
    payload = json.dumps({
        "job_id": JOB_ID,
        "notebook_params": {
            "request_id":         req_id,
            "study_id":           study_id,
            "therapeutic_domain": domain,
            "details":            details
        }
    }).encode("utf-8")
 
    req = urllib.request.Request(
        url     = f"https://{WORKSPACE_HOST}/api/2.1/jobs/run-now",
        data    = payload,
        headers = {
            "Authorization": f"Bearer {TOKEN}",
            "Content-Type":  "application/json"
        },
        method  = "POST"
    )
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read().decode("utf-8"))["run_id"]
 
def fetch_recent():
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT
                    study_id,
                    therapeutic_domain,
                    details,
                    status,
                    workflow_run_id,
                    requested_by,
                    date_format(requested_at, 'yyyy-MM-dd HH:mm') AS requested_at
                FROM {FULL_TABLE}
                ORDER BY requested_at DESC
                LIMIT 50
            """)
            cols = [d[0] for d in cur.description]
            rows = cur.fetchall()
            return cols, rows

# COMMAND ----------

# DBTITLE 1,Session state — tracks dynamic rows
if "rows" not in st.session_state:
    st.session_state.rows = [{"id": 0, "study_id": "", "domain": "", "details": ""}]
if "next_id" not in st.session_state:
    st.session_state.next_id = 1
if "submit_results" not in st.session_state:
    st.session_state.submit_results = []
if "submit_error" not in st.session_state:
    st.session_state.submit_error = ""

# COMMAND ----------

# DBTITLE 1,UI
st.title("🧬 Study Request Submission")
st.markdown("Fill in one or more study requests below. Each row triggers an independent workflow run.")
st.divider()
 
# Column headers
h1, h2, h3, h4 = st.columns([2, 2.5, 3.5, 0.6])
h1.markdown("**Study ID**")
h2.markdown("**Therapeutic Area**")
h3.markdown("**Details**")
h4.markdown("")
 
# Dynamic rows
rows_to_delete = []
for i, row in enumerate(st.session_state.rows):
    c1, c2, c3, c4 = st.columns([2, 2.5, 3.5, 0.6])
    row["study_id"] = c1.text_input(
        label       = f"study_id_{row['id']}",
        value       = row["study_id"],
        placeholder = "e.g. STD-2024-001",
        label_visibility = "collapsed"
    )
    row["domain"] = c2.text_input(
        label       = f"domain_{row['id']}",
        value       = row["domain"],
        placeholder = "e.g. Oncology",
        label_visibility = "collapsed"
    )
    row["details"] = c3.text_input(
        label       = f"details_{row['id']}",
        value       = row["details"],
        placeholder = "Brief description...",
        label_visibility = "collapsed"
    )
    if len(st.session_state.rows) > 1:
        if c4.button("✕", key=f"del_{row['id']}"):
            rows_to_delete.append(row["id"])
 
# Apply deletions
if rows_to_delete:
    st.session_state.rows = [r for r in st.session_state.rows if r["id"] not in rows_to_delete]
    st.rerun()
 
st.markdown("")
 
# Action buttons
b1, b2, _ = st.columns([1.5, 1.5, 6])
 
if b1.button("＋ Add Row", use_container_width=True):
    st.session_state.rows.append({
        "id":       st.session_state.next_id,
        "study_id": "",
        "domain":   "",
        "details":  ""
    })
    st.session_state.next_id += 1
    st.rerun()
 
submit_clicked = b2.button("Submit", type="primary", use_container_width=True)

# COMMAND ----------

# DBTITLE 1,Submit logic
if submit_clicked:
    valid_rows = [
        r for r in st.session_state.rows
        if r["study_id"].strip() and r["domain"].strip()
    ]
 
    if not valid_rows:
        st.session_state.submit_error  = "⚠️ Fill in at least Study ID and Therapeutic Area in one row."
        st.session_state.submit_results = []
    else:
        st.session_state.submit_error = ""
        results  = []
        errors   = []
        now      = datetime.now(timezone.utc)
        # Get current user from env (Databricks Apps injects DATABRICKS_USER)
        user     = os.environ.get("DATABRICKS_USER", "unknown")
 
        with st.spinner(f"Submitting {len(valid_rows)} row(s)..."):
            for r in valid_rows:
                req_id   = str(uuid.uuid4())
                study_id = r["study_id"].strip()
                domain   = r["domain"].strip()
                details  = r["details"].strip()
                try:
                    insert_row(req_id, study_id, domain, details, user, now)
                    run_id = trigger_workflow(req_id, study_id, domain, details)
                    update_status(req_id, run_id)
                    results.append({
                        "study_id": study_id,
                        "run_id":   run_id,
                        "req_id":   req_id
                    })
                except Exception as e:
                    errors.append(f"{study_id}: {str(e)}")
 
        st.session_state.submit_results = results
        if errors:
            st.session_state.submit_error = "Errors: " + " | ".join(errors)
 
        # Reset rows after successful submit
        if results:
            st.session_state.rows    = [{"id": 0, "study_id": "", "domain": "", "details": ""}]
            st.session_state.next_id = 1
 
        st.rerun()

# COMMAND ----------

# DBTITLE 1,Status messages
if st.session_state.submit_results:
    lines = "\n".join(
        f"✅ study_id={r['study_id']} | run_id={r['run_id']}"
        for r in st.session_state.submit_results
    )
    st.markdown(
        f'<div class="status-box status-success">'
        f'<strong>{len(st.session_state.submit_results)} workflow(s) triggered</strong><br>'
        f'<pre style="margin:6px 0 0;font-size:12px;background:none;border:none">{lines}</pre>'
        f'</div>',
        unsafe_allow_html=True
    )
 
if st.session_state.submit_error:
    st.markdown(
        f'<div class="status-box status-error">{st.session_state.submit_error}</div>',
        unsafe_allow_html=True
    )

# COMMAND ----------

# DBTITLE 1,Recent submissions table
st.divider()
st.subheader("Recent Submissions")
 
try:
    cols, rows = fetch_recent()
    if rows:
        import pandas as pd
        df = pd.DataFrame(rows, columns=cols)
        st.dataframe(df, use_container_width=True, hide_index=True)
    else:
        st.info("No submissions yet.")
except Exception as e:
    st.warning(f"Could not load recent submissions: {e}")

import streamlit as st
import pandas as pd
from datetime import datetime
import plotly.express as px
from bs4 import BeautifulSoup
import base64
import json
import subprocess
import io
import time
import plotly.graph_objects as go
import plotly.express as px
from io import BytesIO
from io import BytesIO, StringIO
from auth import ENABLE_AUTH, login, logout



st.set_page_config(page_title="AWR Analyzer", layout="wide")

# 🔐 Optional Authentication
from auth import ENABLE_AUTH, login, logout

if "authenticated" not in st.session_state:
    st.session_state.authenticated = False
if "username" not in st.session_state:
    st.session_state.username = ""

# 🔐 Optional Authentication
from auth import ENABLE_AUTH, login, logout

if ENABLE_AUTH and not st.session_state.authenticated:
    login()

if not ENABLE_AUTH or st.session_state.authenticated:
    logout()  # 👤 Show logout if logged in

    # 🌙 Dark mode toggle can go here inside authenticated block
    dark_mode = st.sidebar.toggle("🌙 Dark Mode", value=False)


# --- Initialize required session_state variables to avoid AttributeError ---
default_session_vars = {
    "ai_11point_result": None,
    "compare_files": [],
    "sql_expander_open": False,
    "authenticated": False,
    "username": "",
}

for key, val in default_session_vars.items():
    if key not in st.session_state:
        st.session_state[key] = val


if dark_mode:
    st.markdown("""
        <style>
        /* Backgrounds */
        .stApp {
            background-color: #121212 !important;
            color: #e0e0e0 !important;
            transition: background-color 0.3s ease, color 0.3s ease;
        }

        /* Headers and titles */
        h1, h2, h3, h4, h5, h6 {
            color: #ffffff !important;
        }

        /* Sidebar */
        .css-1d391kg {
            background-color: #1e1e1e !important;
            color: #ddd !important;
        }

        /* Buttons */
        button[kind="primary"] {
            background-color: #00bcd4 !important;
            color: #121212 !important;
            border: none !important;
            box-shadow: 0 0 8px #00bcd4aa;
            transition: background-color 0.3s ease;
        }
        button[kind="primary"]:hover {
            background-color: #0097a7 !important;
        }

        /* Inputs and text areas */
        input, textarea, select {
            background-color: #222 !important;
            color: #eee !important;
            border: 1px solid #444 !important;
        }

        /* Cards and containers */
        .css-1v3fvcr {
            background-color: #1e1e1e !important;
            box-shadow: 0 0 10px #00bcd433;
            border-radius: 8px;
            border: 1px solid #00bcd466;
            padding: 15px;
        }

        /* Links */
        a, a:visited {
            color: #00bcd4 !important;
            text-decoration: none;
        }
        a:hover {
            text-decoration: underline;
        }
        </style>
    """, unsafe_allow_html=True)
else:
    st.markdown("""
        <style>
        .stApp {
            background-color: #fafafa !important;
            color: #111 !important;
            transition: background-color 0.3s ease, color 0.3s ease;
        }
        .css-1d391kg {
            background-color: #f0f0f0 !important;
            color: #111 !important;
        }
        button[kind="primary"] {
            background-color: #007bff !important;
            color: #fff !important;
            border: none !important;
            box-shadow: none !important;
        }
        input, textarea, select {
            background-color: #fff !important;
            color: #111 !important;
            border: 1px solid #ccc !important;
        }
        .css-1v3fvcr {
            background-color: #fff !important;
            box-shadow: 0 0 5px #ccc;
            border-radius: 6px;
            border: 1px solid #ddd;
            padding: 15px;
        }
        a, a:visited {
            color: #007bff !important;
            text-decoration: none;
        }
        a:hover {
            text-decoration: underline;
        }
        </style>
    """, unsafe_allow_html=True)


# Global card styling
st.markdown("""
<style>
.scroll-banner {
    width: 100%;
    overflow: hidden;
    background-color: transparent;
    padding: 5px 0;
    border-bottom: 1px solid #ccc;
}
.scroll-text {
    display: inline-block;
    white-space: nowrap;
    animation: scroll-left 15s linear infinite;
    font-family: 'Segoe UI', 'Roboto', sans-serif;
    font-weight: 600;
    font-size: 18px;
    color: #222;
}
@keyframes scroll-left {
    0% { transform: translateX(100vw); }
    100% { transform: translateX(-100%); }
}
</style>

<div class="scroll-banner">
    <div class="scroll-text">Developed by Fazal Shaikh</div>
</div>
""", unsafe_allow_html=True)



@st.cache_data
def parse_awr(html):
    soup = BeautifulSoup(html, "lxml")
    tables = soup.find_all('table')
    sga_advisory_df = pd.DataFrame()
    pga_advisory_df = pd.DataFrame()
    seg_physical_reads = pd.DataFrame()
    seg_row_lock_waits = pd.DataFrame()
    seg_table_scans = pd.DataFrame()  
    top_sql_events = pd.DataFrame() 
    activity_over_time = pd.DataFrame()
    init_params = pd.DataFrame()

    def to_df(table):
        try:
            return pd.read_html(str(table))[0]
        except:
            return None

    # Default values for other fields
    db_name = "N/A"
    db_time_value = "N/A"
    idle_cpu = None
    snap_time = "N/A"
    cdb_status = "N/A"
    memory_gb = "N/A"
    platform = "N/A"
    instance_name = "N/A"
    instance_num = "N/A"
    startup_time = "N/A"
    begin_snap_time = "N/A"
    end_snap_time = "N/A"
    total_cpu = "N/A"
    rac_status = "N/A"
    edition = "N/A"
    release = "N/A"

    top_sql = pd.DataFrame()
    top_cpu_sql = pd.DataFrame()
    efficiency_df = pd.DataFrame()


    for table in tables:
        df = to_df(table)
        if df is not None and not df.empty:
            df_str = df.astype(str)
            lower_headers = [str(c).lower().strip() for c in df.columns]

            # Extract Edition, Release, RAC, CDB
            if 'edition' in lower_headers and 'release' in lower_headers:
                try:
                    idx_edition = lower_headers.index('edition')
                    idx_release = lower_headers.index('release')
                    idx_rac = lower_headers.index('rac') if 'rac' in lower_headers else None
                    idx_cdb = lower_headers.index('cdb') if 'cdb' in lower_headers else None

                    edition = str(df.iloc[0, idx_edition]).strip()
                    release = str(df.iloc[0, idx_release]).strip()

                    if idx_rac is not None:
                        rac_text = str(df.iloc[0, idx_rac]).strip().lower()
                        rac_status = 'RAC' if 'yes' in rac_text else 'Single Instance'

                    if idx_cdb is not None:
                        cdb_text = str(df.iloc[0, idx_cdb]).strip().lower()
                        cdb_status = 'CDB' if 'yes' in cdb_text else 'Non-CDB'

                except:
                    pass

            # Extract Memory (GB)
            if 'memory (gb)' in lower_headers:
                try:
                    idx_mem = lower_headers.index('memory (gb)')
                    mem_val = str(df.iloc[0, idx_mem]).strip()
                    memory_gb = mem_val if mem_val else "N/A"
                except:
                    pass

            # Extract DB Time (s) from Load Profile
                db_time_value = "N/A"
                try:
                    db_time_row = data['load_profile'][data['load_profile']['Metric'].str.contains("DB Time", na=False)]
                    db_time_value = db_time_row['Per Second'].values[0]
                except:
                    pass

            # ✅ Extract Idle CPU %
            if '%idle' in lower_headers:
                try:
                    idx_idle = lower_headers.index('%idle')
                    idle_val = df.iloc[0, idx_idle]
                    if isinstance(idle_val, (int, float)) or str(idle_val).replace('.', '', 1).isdigit():
                        idle_cpu = float(idle_val)
                except:
                    pass

            # Extract Platform
            if 'platform' in lower_headers:
                try:
                    idx_platform = lower_headers.index('platform')
                    platform_val = str(df.iloc[0, idx_platform]).strip()
                    platform = platform_val if platform_val else "N/A"
                except:
                    pass

            # Extract Begin and End Snap Time
            if 'snap id' in lower_headers and 'snap time' in lower_headers:
                try:
                    idx_time = lower_headers.index('snap time')

                    begin_snap_row = df[df.iloc[:, 0].astype(str).str.contains("Begin Snap", case=False, na=False)]
                    end_snap_row = df[df.iloc[:, 0].astype(str).str.contains("End Snap", case=False, na=False)]

                    if not begin_snap_row.empty:
                        begin_snap_time = str(begin_snap_row.iloc[0, idx_time]).strip()

                    if not end_snap_row.empty:
                        end_snap_time = str(end_snap_row.iloc[0, idx_time]).strip()

                except:
                    pass

            # ✅ NEW: Extract Instance, Instance Number, Startup Time
            if 'instance' in lower_headers and 'inst num' in lower_headers and 'startup time' in lower_headers:
                try:
                    idx_instance = lower_headers.index('instance')
                    idx_inst_num = lower_headers.index('inst num')
                    idx_startup = lower_headers.index('startup time')

                    instance_name = str(df.iloc[0, idx_instance]).strip()
                    instance_num = str(df.iloc[0, idx_inst_num]).strip()
                    startup_time = str(df.iloc[0, idx_startup]).strip()
                except:
                    pass




            for i, row in df_str.iterrows():
                for j, val in enumerate(row):
                    text = str(val).strip().lower()
                    if 'cpus' in text and total_cpu == "N/A":
                        try:
                            next_val = str(row[j + 1]).strip()
                            if next_val.isdigit():
                                total_cpu = next_val
                        except:
                            continue

            headers = [str(h).lower() for h in df.columns]
            if 'db name' in headers:
                try:
                    db_name = str(df.iloc[0, 0])
                except:
                    pass

            if df.shape[1] >= 3 and df.iloc[:, 0].astype(str).str.contains("Begin Snap", na=False).any():
                try:
                    snap_time = str(df.iloc[0, 2])
                except:
                    pass

    # SQL ordered by Elapsed Time
    for tag in soup.find_all(["b", "font", "p", "div", "h2", "h3"]):
        if "SQL ordered by Elapsed Time" in tag.text:
            next_table = tag.find_next("table")
            if next_table:
                df = to_df(next_table)
                if df is not None and not df.empty:
                    # Normalize column names: strip spaces and make uppercase
                    df.columns = df.columns.str.strip().str.upper()

                    # Rename possible variations of SQL ID column
                    rename_map = {
                        'SQLID': 'SQL ID',
                        'SQL ID': 'SQL ID',
                        'SQL Id': 'SQL ID',
                        'SQLID ': 'SQL ID',
                        'SQL_ID': 'SQL ID'
                    }
                    df.rename(columns=rename_map, inplace=True)

                    # Fill down SQL IDs if some rows are blank
                    if 'SQL ID' in df.columns:
                        df['SQL ID'] = df['SQL ID'].replace('', None).fillna(method='ffill')

                    # Keep only needed columns
                    keep_cols = ['ELAPSED TIME (S)', 'EXECUTIONS', 'ELAPSED TIME PER EXEC (S)', 'SQL ID', 'SQL TEXT']
                    top_sql = df[[col for col in df.columns if col in keep_cols]]
            break




    # SQL ordered by CPU Time
    for tag in soup.find_all(["b", "font", "p", "div", "h2", "h3"]):
        if "SQL ordered by CPU Time" in tag.text:
            next_table = tag.find_next("table")
            if next_table:
                df = to_df(next_table)
                if df is not None and not df.empty:
                    cols = [str(c).strip().lower() for c in df.columns]
                    if all(col in cols for col in ['sql id', 'sql text', 'cpu time (s)']):
                        top_cpu_sql = df[[c for c in df.columns if str(c).lower() in ['sql id', 'sql text', 'cpu time (s)']]]
            break

    for tag in soup.find_all(["b", "font", "p", "div", "h2", "h3"]):
        if "Complete List of SQL Text" in tag.text:
            next_table = tag.find_next("table")
            if next_table:
                df_sql_texts = to_df(next_table)
                if df_sql_texts is not None and not df_sql_texts.empty:
                    df_sql_texts.columns = df_sql_texts.columns.str.strip().str.upper()
                    full_sql_text_df = df_sql_texts[['SQL ID', 'SQL TEXT']].copy()
            break

    
    # Corrected Initialization Parameters Extraction Logic
    for a in soup.find_all("a"):
        if "name" in a.attrs and a["name"] == "36":
            next_table = a.find_next("table")
            if next_table:
                df = to_df(next_table)
                if df is not None and df.shape[1] >= 2:
                    headers = [str(h).strip().lower() for h in df.columns]
                    if 'name' in headers[0] and any('value' in h for h in headers):
                        init_params = df.iloc[:, :2]
                        init_params.columns = ['Parameter', 'Value']
                        init_params = init_params.dropna().reset_index(drop=True)
                        break

    
    
    
    # Look for the table AFTER detecting 'Instance Efficiency Percentages' label
    for i, tag in enumerate(soup.find_all()):
        if tag.name in ["b", "font", "p", "div"] and "Instance Efficiency Percentages" in tag.text:
            next_table = tag.find_next("table")
            if next_table:
                df = to_df(next_table)
                if df is not None and not df.empty:
                
                    # The table is spread horizontally, convert to key-value pairs
                    flat_data = []
                    for row in df.itertuples(index=False):
                        row = [str(x).strip() for x in row]
                        for j in range(0, len(row), 2):
                            if j + 1 < len(row) and row[j] and row[j + 1]:
                                flat_data.append({"Metric": row[j], "Value": row[j + 1]})
                
                    if flat_data:
                        efficiency_df = pd.DataFrame(flat_data)
            break


    load_profile = pd.DataFrame(columns=["Metric", "Per Second"])
    for table in tables:
        df = to_df(table)
        if df is not None and df.shape[1] >= 2:
            first_col = df.iloc[:, 0].astype(str).str.strip()
            if first_col.str.contains("DB Time\\(s\\)").any():
                try:
                    load_profile = df.iloc[:, :2]
                    load_profile.columns = ["Metric", "Per Second"]
                    break
                except:
                    pass

    wait_events = pd.DataFrame(columns=["Event", "% DB time"])
    for table in tables:
        df = to_df(table)
        if df is not None and '% DB time' in df.columns:
            try:
                wait_events = df[[col for col in df.columns if 'Event' in col or '% DB time' in col]].head(5)
                wait_events.columns = ['Event', '% DB time']
                break
            except:
                continue

    idle_cpu = None
    for table in tables:
        df = to_df(table)
        if df is not None and '%Idle' in df.columns:
            try:
                idle_cpu = float(df['%Idle'].iloc[0])
                break
            except:
                pass

    
    for tag in soup.find_all(["b", "font", "p", "div", "h2", "h3"]):
        if "Segments by Physical Reads" in tag.text:
            next_table = tag.find_next("table")
            if next_table:
                df = to_df(next_table)
                if df is not None and not df.empty:
                    seg_physical_reads = df
            break  # Exit after finding the first occurrence

    for tag in soup.find_all(["b", "font", "p", "div", "h2", "h3"]):
        if "Segments by Row Lock Waits" in tag.text:
            next_table = tag.find_next("table")
            if next_table:
                df = to_df(next_table)
                if df is not None and not df.empty:
                    seg_row_lock_waits = df
            break

    for tag in soup.find_all(["b", "font", "p", "div", "h2", "h3"]):
        if "Segments by Table Scans" in tag.text:
            next_table = tag.find_next("table")
            if next_table:
                df = to_df(next_table)
                if df is not None and not df.empty:
                    seg_table_scans = df
            break

    for a in soup.find_all("a"):
        if a.get("name") == "26":
            table = a.find_next("table")
            while table:
                df = to_df(table)
                if df is not None and not df.empty:
                    cols = [str(c).lower() for c in df.columns]  # ✅ safe and correct
                    if "pga target" in cols[0].lower():
                        pga_advisory_df = df
                table = table.find_next_sibling("table")

    for a in soup.find_all("a"):
        if "name" in a.attrs and a["name"] == "26":  # Confirm this '26' matches your report's anchor
            next_table = a.find_next("table")
        
            while next_table:
                df = to_df(next_table)  # Your function to convert table to DataFrame

                if df is not None and df.shape[1] >= 2:
                    df.columns = [str(col).strip() for col in df.columns]  # Safe column cleanup

                    # Adjust based on actual table headers from your screenshot
                    if "SGA Target Size (M)" in df.columns[0] and "Est DB Time (s)" in df.columns[2]:
                        sga_advisory_df = df
                        break  # Found the target table

                next_table = next_table.find_next("table")  # Continue to next table if not found

    for tag in soup.find_all(["b", "font", "p", "div", "h2", "h3"]):
        if "Top SQL with Top Events" in tag.text:
            next_table = tag.find_next("table")
            if next_table:
                df = to_df(next_table)
                if df is not None and not df.empty:
                    # Fill forward SQL IDs where blank (for multi-row events in AWR)
                    if 'SQL ID' in df.columns:
                        df['SQL ID'] = df['SQL ID'].replace('', None).fillna(method='ffill')
                    top_sql_events = df
            # Do not break, in case there are multiple tables for this section


    # Extract Activity Over Time section
    for tag in soup.find_all(["b", "font", "p", "div", "h2", "h3"]):
        if "Activity Over Time" in tag.text:
            next_table = tag.find_next("table")
            if next_table:
                df = to_df(next_table)
                if df is not None and not df.empty:
                    activity_over_time = df
            break  # Exit loop after finding the section


    return {
        'db_name': db_name,
        'snap_time': snap_time,
        'load_profile': load_profile,
        'cdb_status': cdb_status,
        'memory_gb': memory_gb,
        'platform': platform,
        'instance_name': instance_name,
        'instance_num': instance_num,
        'startup_time': startup_time,
        'begin_snap_time': begin_snap_time,
        'end_snap_time': end_snap_time,
        'wait_events': wait_events,
        'idle_cpu': idle_cpu,
        'top_sql': top_sql,
        'top_cpu_sql': top_cpu_sql,
        'init_params': init_params,
        'seg_physical_reads': seg_physical_reads,
        'seg_row_lock_waits': seg_row_lock_waits,
        'seg_table_scans': seg_table_scans,
        'total_cpu': total_cpu,
        'rac_status': rac_status,
        'edition': f"{edition} {release}" if release != "N/A" else edition,
        'pga_advisory': pga_advisory_df,
        'sga_advisory': sga_advisory_df,
        'top_sql_events': top_sql_events,
        'activity_over_time': activity_over_time,
        'full_sql_texts': full_sql_text_df
    }



# ---------------------------------------------------------
# 🌟 Enhanced Mistral AI Integration for AWR Analysis
# ---------------------------------------------------------
from mistralai import Mistral
import os, time

def ai_generate(prompt: str) -> str:
    """
    Calls Mistral AI to analyze Oracle AWR data based on the given prompt.
    Automatically retries if the model is busy.
    """
    try:
        api_key = os.getenv("MISTRAL_API_KEY")
        if not api_key:
            return "⚠️ AI Error: MISTRAL_API_KEY not found in environment."

        client = Mistral(api_key=api_key)

        messages = [
            {"role": "system", "content": "You are an expert Oracle Database Performance Engineer specializing in AWR report analysis. Provide clear, technical explanations."},
            {"role": "user", "content": prompt},
        ]

        # Retry up to 3 times for capacity errors
        for attempt in range(3):
            try:
                response = client.chat.complete(
                    model="mistral-large-latest",
                    messages=messages,
                    temperature=0.2,
                )
                return response.choices[0].message.content.strip()
            except Exception as e:
                if "capacity" in str(e).lower():
                    time.sleep(5)
                else:
                    raise
        return "⚠️ Mistral API capacity issue persisted after retries."

    except Exception as e:
        return f"⚠️ AI Error: {str(e)}"



# ====================================
# Intelligent Insights
# ====================================
def get_intelligent_insights(data):
    """
    Return a list of insight strings based on parsed AWR data.
    Robustly detects full-table-scan SQLs by scanning all columns of top_sql_events
    for 'TABLE ACCESS' + 'FULL', and then tries to map to seg_table_scans table names.
    """
    insights = []

    # Rule: High DB Time per second
    try:
        if not data.get('load_profile', pd.DataFrame()).empty:
            db_time_row = data['load_profile'][data['load_profile']['Metric'].str.contains("DB Time", na=False)]
            if not db_time_row.empty:
                db_time = float(db_time_row['Per Second'].values[0])
                if db_time > 40.0:
                    insights.append(f"🔥 [CRITICAL] High average active sessions (DB Time per sec = {db_time:.2f}) — high load on DB.")
    except Exception:
        pass

    # Rule: SGA Advisory (best-effort)
    try:
        if not data.get('sga_advisory', pd.DataFrame()).empty:
            sga_df = data['sga_advisory'].copy()
            sga_df.columns = sga_df.columns.str.strip()
            if 'Est Physical Reads' in sga_df.columns:
                sga_df['Est Physical Reads'] = pd.to_numeric(
                    sga_df['Est Physical Reads'].astype(str).str.replace(',', ''), errors='coerce'
                )
                # compare middle row vs min as a heuristic
                cur_idx = len(sga_df) // 2
                cur_val = sga_df['Est Physical Reads'].iloc[cur_idx]
                min_val = sga_df['Est Physical Reads'].min()
                if pd.notna(cur_val) and pd.notna(min_val) and (cur_val - min_val) > 1000:
                    insights.append("⚠️ [WARNING] SGA Advisory suggests increasing SGA to reduce physical reads.")
    except Exception:
        pass

    # Rule: Wait Events (contention detection)
    try:
        if not data.get('wait_events', pd.DataFrame()).empty:
            df = data['wait_events'].copy()
            df.columns = df.columns.str.strip()
            if '% DB time' in df.columns:
                df['% DB time'] = pd.to_numeric(df['% DB time'], errors='coerce')
                # tx/tm contention
                tx_tm_events = df[
                    df['Event'].str.lower().isin(['enq: tx - row lock contention', 'enq: tm - contention']) &
                    (df['% DB time'] > 1)
                ]
                if not tx_tm_events.empty:
                    listed = ', '.join(tx_tm_events['Event'].astype(str).tolist())
                    insights.append(f"🔥 [CRITICAL] Contention detected: {listed} — investigate blocking sessions or DML concurrency.")
    except Exception:
        pass

    # Rule: PGA Advisory
    if not data['pga_advisory'].empty:
        try:
            pga_df = data['pga_advisory'].copy()
            pga_df.columns = pga_df.columns.str.strip()
            pga_df['Estd Time'] = pd.to_numeric(pga_df['Estd Time'].replace(',', '', regex=True), errors='coerce')
            current_row = pga_df.iloc[len(pga_df)//2]
            current_time = current_row['Estd Time']
            min_time = pga_df['Estd Time'].min()
            if current_time - min_time > 10:
                insights.append("⚠️ [WARNING] PGA Advisory suggests increasing PGA to reduce execution time.")
        except:
            pass

    # Rule: Idle CPU
    try:
        if data.get('idle_cpu') is not None and isinstance(data['idle_cpu'], (int, float)):
            if data['idle_cpu'] < 10:
                insights.append(f"🔥 [CRITICAL] Very low idle CPU ({data['idle_cpu']:.1f}%) — High CPU pressure.")
            elif data['idle_cpu'] < 30:
                insights.append(f"⚠️ [WARNING] Idle CPU below optimal ({data['idle_cpu']:.1f}%).")
            else:
                insights.append(f"✅ Idle CPU healthy at {data['idle_cpu']:.1f}%.")
    except Exception:
        pass

    # ----------------------
    # Robust Full Table Scan Detection (Combined Output, No Table Name)
    # ----------------------
    try:
        events_df = data.get('top_sql_events', pd.DataFrame()).copy()
        if not events_df.empty:
            # Normalize headers
            events_df.columns = [str(c).replace('\n', ' ').strip() for c in events_df.columns]

            # Find important columns
            sql_col = next((c for c in events_df.columns if ('SQL' in c.upper() and 'ID' in c.upper()) or c.upper() == 'SQLID'), None)
            event_col = next((c for c in events_df.columns if 'EVENT' in c.upper()), None)

            detected_list = []
            for _, row in events_df.iterrows():
                # Combine all cell text for pattern match
                row_text_combined = " ".join([str(row[c]) for c in events_df.columns if pd.notna(row[c])]).lower()

                # Detect FULL TABLE SCAN
                if 'table access' in row_text_combined and 'full' in row_text_combined:
                    sql_id = str(row[sql_col]) if sql_col and pd.notna(row.get(sql_col)) else 'N/A'
                    event_name = str(row[event_col]) if event_col and pd.notna(row.get(event_col)) else 'N/A'
                    detected_list.append(f"{sql_id} (Event: {event_name})")

            # Single combined message
            if detected_list:
                insights.append(
                    f"⚠️ The following SQL_IDs are performing FULL TABLE SCANS — review execution plans and indexing: {', '.join(detected_list)}"
                )

    except Exception as e:
        print("Full table scan detection error:", e)


    return insights


# 🧩 NEW — Add this below get_intelligent_insights()
def build_analyzer_summary(data, insights):
    """
    Builds a complete summary of the AWR Analyzer output — including key metrics,
    insights, and top sections — to feed into Mistral AI.
    """
    lines = []
    append = lines.append

    append("=== AWR Analyzer Summary ===")

    # --- General Info ---
    append(f"Database Name: {data.get('db_name', 'N/A')}")
    append(f"Instance: {data.get('instance_name', 'N/A')} #{data.get('instance_num', 'N/A')}")
    append(f"Edition/Release: {data.get('edition', 'N/A')}")
    append(f"Platform: {data.get('platform', 'N/A')}")
    append(f"Memory (GB): {data.get('memory_gb', 'N/A')}")
    append(f"Total CPUs: {data.get('total_cpu', 'N/A')}")
    append(f"Idle CPU (%): {data.get('idle_cpu', 'N/A')}")
    append(f"RAC: {data.get('rac_status', 'N/A')}")
    append(f"CDB: {data.get('cdb_status', 'N/A')}")
    append(f"Begin Snap: {data.get('begin_snap_time', 'N/A')}")
    append(f"End Snap: {data.get('end_snap_time', 'N/A')}")

    # --- Load Profile ---
    lp = data.get("load_profile", pd.DataFrame())
    if not lp.empty:
        append("\n--- Load Profile (Per Second) ---")
        append(lp.head(10).to_string(index=False))

    # --- Wait Events ---
    we = data.get("wait_events", pd.DataFrame())
    if not we.empty:
        append("\n--- Top Wait Events (% DB Time) ---")
        append(we.head(10).to_string(index=False))

    # --- Top SQLs ---
    ts = data.get("top_sql", pd.DataFrame())
    if not ts.empty:
        append("\n--- Top SQL by Elapsed Time ---")
        append(ts.head(10).to_string(index=False))

    cpu_sql = data.get("top_cpu_sql", pd.DataFrame())
    if not cpu_sql.empty:
        append("\n--- Top SQL by CPU Time ---")
        append(cpu_sql.head(10).to_string(index=False))

    # --- PGA / SGA Advisory ---
    pga = data.get("pga_advisory", pd.DataFrame())
    if not pga.empty:
        append("\n--- PGA Advisory ---")
        append(pga.head(5).to_string(index=False))

    sga = data.get("sga_advisory", pd.DataFrame())
    if not sga.empty:
        append("\n--- SGA Advisory ---")
        append(sga.head(5).to_string(index=False))

    # --- Segments ---
    segs = {
        "Segments by Physical Reads": data.get("seg_physical_reads", pd.DataFrame()),
        "Segments by Row Lock Waits": data.get("seg_row_lock_waits", pd.DataFrame()),
        "Segments by Table Scans": data.get("seg_table_scans", pd.DataFrame()),
    }
    for title, df in segs.items():
        if not df.empty:
            append(f"\n--- {title} ---")
            append(df.head(5).to_string(index=False))

    # --- Intelligent Insights ---
    if insights:
        append("\n--- Intelligent Insights ---")
        for ins in insights:
            append(f"- {ins}")

    return "\n".join(lines)



# ============================================
# MAIN APP DISPLAY (Existing Analyzer Features)
# ============================================

# ========================
# MAIN APP DISPLAY
# ========================

# Upload file - Main uploader
uploaded_files = st.file_uploader(
    "📤 Upload AWR HTML reports",
    type="html",
    accept_multiple_files=True,
    key="main_uploader"
)

if not uploaded_files:
    st.stop()

# ✅ Parse the first uploaded AWR file into `data`
selected_html = uploaded_files[0].getvalue().decode("utf-8", errors="ignore")
data = parse_awr(selected_html)

# ✅ Generate intelligent insights
insights = get_intelligent_insights(data)

# ✅ Continue with your existing AWR analyzer features:
# (AWR Info, Load Profile, Wait Events, Top SQLs, Segments, Comparisons, etc.)
file_names = [file.name for file in uploaded_files]
selected_file = file_names[0]

# ==========================================
# 🤖 Mistral AI AWR Issue Analyzer (Technical + Guided + Dynamic DBA Checklist)
# ==========================================
st.markdown("### 💬 Describe Your Performance Issue or Ask a Technical Question")

user_issue = st.text_area(
    "Example: 'Why is log file sync wait high?', 'Why is CPU usage high?', 'Which SQL causes high I/O?'",
    placeholder="Type your production performance issue or AWR-related question...",
    height=120,
)

if st.button("🧠 Run Mistral AI AWR Analysis"):
    try:
        # ✅ Load the uploaded AWR report and parse it
        selected_html = uploaded_files[0].getvalue().decode("utf-8", errors="ignore")
        parsed_data = parse_awr(selected_html)

        # ✅ Build the analyzer summary (parsed + insights)
        analyzer_summary = build_analyzer_summary(parsed_data, insights)

        # ✅ Combine the parsed analyzer output and raw AWR HTML into a single hybrid context
        # We merge both sources, not show them separately
        combined_awr_context = f"""
=== ORACLE AWR PERFORMANCE CONTEXT ===

Below is a combined and enriched representation of the Oracle AWR data.
It includes both structured analyzer metrics and embedded raw AWR report details for full fidelity.

{analyzer_summary}

--- Embedded AWR Raw Report (Full HTML/Text Context) ---
{selected_html}
"""

        # 🧠 Unified prompt for Mistral
        prompt = f"""
You are an **Oracle Database Performance Engineer** with over 15 years of experience in analyzing Oracle AWR reports.

You have access to a unified Oracle AWR performance dataset below (which combines the structured AWR Analyzer metrics with the embedded raw AWR report content). 
Use this combined data to produce a **single, cohesive analysis** — not separate summaries.

🎯 **User Issue / Question:**
{user_issue}

📊 **Unified AWR Data Context (Analyzer + Raw Report Combined):**
{combined_awr_context}

🧩 **Response Guidelines:**
1. Provide one unified technical explanation that merges findings from both the raw and parsed AWR data.
2. Correlate findings between raw AWR report text and the parsed analyzer output.
3. Provide a technical, expert-level output, similar to what an Oracle DBA or Oracle Performance Tuning specialist would deliver.
4. Be Clear: Use simple sentences suitable for beginner DBAs.
5. Be Focused: Only discuss AWR data relevant to the user's issue.
6. Be cautious and avoid making any risky changes since this is a production database
7. Be Guided: For each relevant AWR section, explain:
   - Which section to look at
   - Why it’s relevant
   - What exactly to check
   - How to interpret the values safely
8. Suggest which sections of both the should be reviewed for further analysis. For example, specify which section to focus on for deeper insights. 
9. Include technical recommendations based on the findings.
10. Provide relevant website links with proper URLs for reference its recommended i want this.


💬 **Output Format:**

**Technical Analysis:**  
(Explain the likely cause, observed patterns, and cross-validated findings from both AWR sources.)

**Guided Steps for the DBA:**  
- <Section>: <why it matters, what to review, how to interpret safely>  
- <Section>: <why it matters, what to review, how to interpret safely>  

**Recommendations and References:**  
(Provide tuning suggestions, Oracle doc links, and safe operational guidance.)

**Safety Note:**  
Always verify any tuning recommendations in a test or staging environment before applying them to production.
"""

        # Keep within Mistral’s token safety limit
        if len(prompt) > 120000:
            st.warning("⚠️ Combined AWR data is very large; trimming slightly to fit model context limits.")
            prompt = prompt[:120000]

        with st.spinner("🤖 Mistral AI is analyzing your combined AWR report data..."):
            ai_output = ai_generate(prompt)

        st.markdown("### 🧠 Mistral AI Unified AWR Performance Analysis")
        st.info("Below is a **single, cohesive analysis** that merges both raw AWR and parsed analyzer data:")
        st.success(ai_output)

        st.download_button(
            label="📄 Download Unified AI Analysis",
            data=ai_output,
            file_name=f"AWR_AI_Combined_Analysis_{user_issue[:30].replace(' ', '_')}.txt",
            mime="text/plain"
        )

    except Exception as e:
        st.error(f"⚠️ Error running Mistral AI Analysis: {e}")











# ---------------- Side-by-side Comparison Feature ---------------- #
# ---------------- Side-by-side Comparison Feature ---------------- #
if len(uploaded_files) >= 2:
    st.markdown("### 🔍 Compare Two AWR Reports Side by Side")

    if "compare_files" not in st.session_state:
        st.session_state.compare_files = []

    selected = st.multiselect(
        "Select exactly 2 reports to compare:",
        file_names,
        default=st.session_state.compare_files if len(st.session_state.compare_files) < 2 else [],
        key="select_compare_reports_internal"
    )

    if len(selected) == 2:
        st.session_state.compare_files = selected
    elif len(selected) > 2:
        st.warning("Please select only 2 reports to compare.")

    if len(st.session_state.compare_files) == 2:
        if "last_compare_pair" not in st.session_state:
            st.session_state.last_compare_pair = []

        if st.session_state.compare_files != st.session_state.last_compare_pair:
            st.session_state.sql_expander_open = False
            st.session_state.last_compare_pair = st.session_state.compare_files.copy()

        data_1, data_2 = None, None

        for uploaded in uploaded_files:
            if uploaded.name == st.session_state.compare_files[0]:
                html_1 = uploaded.getvalue().decode('utf-8')
                data_1 = parse_awr(html_1)
            elif uploaded.name == st.session_state.compare_files[1]:
                html_2 = uploaded.getvalue().decode('utf-8')
                data_2 = parse_awr(html_2)

        def render_env_card(label, value, icon="", bg="#f8f9fa"):
            return f'<div style="background:{bg};padding:1rem;border-radius:10px;width:45%;"><b>{icon} {label}</b><br>{value}</div>'

        def color_code(val, low, high, reverse=False):
            try:
                val = float(val)
                if reverse:
                    if val < low:
                        return ("🔥", "#ffcccc")
                    elif val < high:
                        return ("⚠️", "#fff3cd")
                    else:
                        return ("✅", "#d4edda")
                else:
                    if val > high:
                        return ("🔥", "#ffcccc")
                    elif val > low:
                        return ("⚠️", "#fff3cd")
                    else:
                        return ("✅", "#d4edda")
            except:
                return ("❓", "#f8f9fa")

        def compare_sections(data_1, data_2, file_names):
            sections = [
                ("Load Profile", "load_profile"),
                ("Wait Events", "wait_events"),
                ("Top SQL by Elapsed Time", "top_sql"),
                ("Top SQL by CPU Time", "top_cpu_sql"),
                ("SGA Advisory", "sga_advisory"),
                ("PGA Advisory", "pga_advisory"),
                ("Segments by Physical Reads", "seg_physical_reads"),
                ("Segments by Row Lock Waits", "seg_row_lock_waits"),
                ("Segments by Table Scans", "seg_table_scans"),
                ("Top SQL with Events", "top_sql_events"),
                ("Initialization Parameters", "init_params"),
                ("Activity Over Time", "activity_over_time")
            ]

            for title, key in sections:
                st.markdown(f"### 🔍 {title} Comparison")
                col1, col2 = st.columns(2)

                with col1:
                    st.write(f"**{file_names[0]} {title}**")
                    if key in data_1 and not data_1[key].empty:
                        st.dataframe(data_1[key], use_container_width=True)
                    else:
                        st.warning("No data found.")

                with col2:
                    st.write(f"**{file_names[1]} {title}**")
                    if key in data_2 and not data_2[key].empty:
                        st.dataframe(data_2[key], use_container_width=True)
                    else:
                        st.warning("No data found.")

                if key == "init_params":
                    st.markdown("#### 📝 Initialization Parameter Changes Summary")
                    df1 = data_1[key].copy()
                    df2 = data_2[key].copy()
                    df1.columns = ['Parameter', 'Value_1']
                    df2.columns = ['Parameter', 'Value_2']
                    merged = pd.merge(df1, df2, on='Parameter', how='outer', indicator=True)
                    notes = []
                    for _, row in merged.iterrows():
                        param = row['Parameter']
                        val1 = str(row['Value_1']) if pd.notna(row['Value_1']) else "N/A"
                        val2 = str(row['Value_2']) if pd.notna(row['Value_2']) else "N/A"
                        if row['_merge'] == 'both' and val1 != val2:
                            notes.append(f"🔁 **{param}** changed from **{val1}** to **{val2}**.")
                        elif row['_merge'] == 'left_only':
                            notes.append(f"➖ **{param}** was present in the first report with value **{val1}**, but not in the second.")
                        elif row['_merge'] == 'right_only':
                            notes.append(f"➕ **{param}** was newly added in the second report with value **{val2}**.")
                    if notes:
                        for note in notes:
                            st.markdown(note)
                    else:
                        st.success("✅ No differences in initialization parameters.")

                if key == "top_sql":
                    st.markdown("#### 🧠 SQL Differences (Elapsed Time)")
                    df1 = data_1[key].copy()
                    df2 = data_2[key].copy()
                    if not df1.empty and not df2.empty:
                        df1.columns = df1.columns.str.upper().str.strip()
                        df2.columns = df2.columns.str.upper().str.strip()
                        df1 = df1[['SQL ID', 'ELAPSED TIME (S)']].dropna()
                        df2 = df2[['SQL ID', 'ELAPSED TIME (S)']].dropna()
                        df1.rename(columns={'ELAPSED TIME (S)': 'Elapsed_1'}, inplace=True)
                        df2.rename(columns={'ELAPSED TIME (S)': 'Elapsed_2'}, inplace=True)
                        merged = pd.merge(df1, df2, on='SQL ID', how='outer', indicator=True)
                        diff_notes = []
                        for _, row in merged.iterrows():
                            sql_id = row['SQL ID']
                            e1 = row.get('Elapsed_1', None)
                            e2 = row.get('Elapsed_2', None)
                            if row['_merge'] == 'left_only':
                                diff_notes.append(f"➖ SQL ID **{sql_id}** was present in **Report 1** only.")
                            elif row['_merge'] == 'right_only':
                                diff_notes.append(f"➕ SQL ID **{sql_id}** was newly added in **Report 2**.")
                            elif pd.notna(e1) and pd.notna(e2):
                                delta = e2 - e1
                                if abs(delta) >= 1:
                                    symbol = "🔹" if delta > 0 else "🔻"
                                    diff_notes.append(f"{symbol} SQL ID **{sql_id}** changed Elapsed Time from **{e1:.2f}s** to **{e2:.2f}s** ({'increased' if delta > 0 else 'decreased'} by {abs(delta):.2f}s).")
                        if diff_notes:
                            for note in diff_notes:
                                st.markdown(note)
                        else:
                            st.success("✅ No significant SQL differences found.")

        if data_1 and data_2:
            st.success(f"Comparing **{st.session_state.compare_files[0]}** vs **{st.session_state.compare_files[1]}**")
            # Render visual environment info as cards
            col1, col2 = st.columns(2)
            with col1:
                st.markdown(f"### 📄 Report: {st.session_state.compare_files[0]}")
                db_time_val = data_1['load_profile'][data_1['load_profile']['Metric'].str.contains("DB Time", na=False)]['Per Second'].values[0] if not data_1['load_profile'].empty else "N/A"
                idle_cpu_val = data_1['idle_cpu']
                db_time_icon, db_time_bg = color_code(db_time_val, 10, 40, reverse=False)
                idle_icon, idle_bg = color_code(idle_cpu_val, 10, 30, reverse=True)
                cards = [
                    render_env_card("DB Time (s)", f"{db_time_icon} {db_time_val}", "🕒", db_time_bg),
                    render_env_card("Idle CPU (%)", f"{idle_icon} {idle_cpu_val:.1f}", "🧠", idle_bg),
                    render_env_card("DB Name", data_1['db_name'], "🗄️"),
                    render_env_card("Instance Name", data_1['instance_name'], "📛"),
                    render_env_card("Instance Number", data_1['instance_num'], "#️⃣"),
                    render_env_card("Startup Time", data_1['startup_time'], "⏱️"),
                    render_env_card("Edition / Release", data_1['edition'], "🔖"),
                    render_env_card("RAC Status", data_1['rac_status'], "🗃️"),
                    render_env_card("CDB Status", data_1['cdb_status'], "🏢"),
                    render_env_card("Total CPUs", data_1['total_cpu'], "🖥️"),
                    render_env_card("Memory (GB)", data_1['memory_gb'], "💾"),
                    render_env_card("Platform", data_1['platform'], "💻"),
                    render_env_card("Begin Snap Time", data_1['begin_snap_time'], "🚩"),
                    render_env_card("End Snap Time", data_1['end_snap_time'], "🏁")
                ]
                st.markdown('<div style="display:flex;flex-wrap:wrap;gap:10px;">' + ''.join(cards) + '</div>', unsafe_allow_html=True)
            with col2:
                st.markdown(f"### 📄 Report: {st.session_state.compare_files[1]}")
                db_time_val = data_2['load_profile'][data_2['load_profile']['Metric'].str.contains("DB Time", na=False)]['Per Second'].values[0] if not data_2['load_profile'].empty else "N/A"
                idle_cpu_val = data_2['idle_cpu']
                db_time_icon, db_time_bg = color_code(db_time_val, 10, 40, reverse=False)
                idle_icon, idle_bg = color_code(idle_cpu_val, 10, 30, reverse=True)
                cards = [
                    render_env_card("DB Time (s)", f"{db_time_icon} {db_time_val}", "🕒", db_time_bg),
                    render_env_card("Idle CPU (%)", f"{idle_icon} {idle_cpu_val:.1f}", "🧠", idle_bg),
                    render_env_card("DB Name", data_2['db_name'], "🗄️"),
                    render_env_card("Instance Name", data_2['instance_name'], "📛"),
                    render_env_card("Instance Number", data_2['instance_num'], "#️⃣"),
                    render_env_card("Startup Time", data_2['startup_time'], "⏱️"),
                    render_env_card("Edition / Release", data_2['edition'], "🔖"),
                    render_env_card("RAC Status", data_2['rac_status'], "🗃️"),
                    render_env_card("CDB Status", data_2['cdb_status'], "🏢"),
                    render_env_card("Total CPUs", data_2['total_cpu'], "🖥️"),
                    render_env_card("Memory (GB)", data_2['memory_gb'], "💾"),
                    render_env_card("Platform", data_2['platform'], "💻"),
                    render_env_card("Begin Snap Time", data_2['begin_snap_time'], "🚩"),
                    render_env_card("End Snap Time", data_2['end_snap_time'], "🏁")
                ]
                st.markdown('<div style="display:flex;flex-wrap:wrap;gap:10px;">' + ''.join(cards) + '</div>', unsafe_allow_html=True)

            # ✅ Render all comparison sections
            compare_sections(data_1, data_2, st.session_state.compare_files)



    


# DB Time
db_time = None
lp = data['load_profile']
if not lp.empty and lp['Metric'].astype(str).str.contains('DB Time').any():
    row = lp[lp['Metric'].astype(str).str.contains('DB Time')]
    db_time = row['Per Second'].iloc[0]

# AWR Summary – Card UI + Observations
cpu_count = int(data['total_cpu']) if str(data['total_cpu']).isdigit() else 0
db_time = 0
try:
    db_time = float(
        data['load_profile'][data['load_profile']['Metric'].str.contains("DB Time", na=False)]['Per Second'].values[0]
    )
except:
    db_time = 0
idle_cpu = float(data['idle_cpu']) if data['idle_cpu'] is not None else None

st.markdown("""
<div style='margin-top: 2rem; margin-bottom: 1rem; padding: 1rem; background: linear-gradient(to right, #f7971e, #ffd200); border-radius: 10px;'>
    <h4 style='color:#222; margin: 0;'>🛠️ AWR Environment Info</h4>
</div>
<div class="card-container">
    <div class="card"><h4>🖥️ Total CPUs</h4><p>{cpu}</p></div>
    <div class="card"><h4>🕒 DB Time (s)</h4><p>{db_time}</p></div>
    <div class="card"><h4>🧠 Idle CPU (%)</h4><p>{idle}</p></div>
    <div class="card"><h4>🗃️ RAC Status</h4><p>{rac}</p></div>
    <div class="card"><h4>🔖 Edition/Release</h4><p>{edition}</p></div>
    <div class="card"><h4>💾 Memory (GB)</h4><p>{memory}</p></div>
    <div class="card"><h4>💻 Platform</h4><p>{platform}</p></div>
    <div class="card"><h4>🏢 CDB Status</h4><p>{cdb}</p></div>
    <div class="card"><h4>🟢 Begin Snap Time</h4><p>{begin}</p></div>
    <div class="card"><h4>🔴 End Snap Time</h4><p>{end}</p></div>
    <div class="card"><h4>📌 Instance</h4><p>{instance}</p></div>
    <div class="card"><h4>#️⃣ Instance Number</h4><p>{inst_num}</p></div>
    <div class="card"><h4>⏰ Startup Time</h4><p>{startup}</p></div>
</div>
""".format(
    cpu=data['total_cpu'],
    db_time=f"{db_time:.1f}" if db_time else "N/A",
    idle=f"{idle_cpu:.1f}" if idle_cpu is not None else "N/A",
    rac=data['rac_status'],
    edition=data['edition'],
    memory=data['memory_gb'],
    platform=data['platform'],
    cdb=data['cdb_status'],
    begin=data['begin_snap_time'],
    end=data['end_snap_time'],
    instance=data['instance_name'],
    inst_num=data['instance_num'],
    startup=data['startup_time']
), unsafe_allow_html=True)






with st.expander("View Observations & Recommendations"):

    # 1. CPU Usage
    if idle_cpu is not None:
        if idle_cpu > 70:
            st.markdown(f"""
**🟢 1. CPU Usage (Idle CPU: {idle_cpu:.1f}%) — Sufficient Capacity**  
✅ **Observation:** Idle CPU is **{idle_cpu:.1f}%** — system has **sufficient CPU capacity**.

**💡 Recommendation:**  
- No action needed now.  
- Monitor for spikes or inefficient SQL if idle drops below 30%.
""")
        elif idle_cpu < 30:
            st.markdown(f"""
**🔥 1. CPU Usage (Idle CPU: {idle_cpu:.1f}%) — High CPU Pressure**  
⚠️ **Observation:** Idle CPU is **{idle_cpu:.1f}%**, indicating **high CPU pressure**.

**💡 Recommendation:**  
- Review **Top SQL by CPU Time** to identify CPU-heavy queries.  
- Use the **Wait Events** section to detect `CPU + Wait for CPU`.  
- Check for **high hard parse rate**; use bind variables to reduce parsing.  
- Investigate **background processes or external workloads** consuming CPU. 
""")
        else:
            st.markdown(f"""
**⚠️ 1. CPU Usage (Idle CPU: {idle_cpu:.1f}%) — Moderate Load**  
🟡 **Observation:** Idle CPU is **{idle_cpu:.1f}%** — system is under **moderate CPU load**.

**💡 Recommendation:**  
- Monitor for downward trends toward critical usage.  
- Investigate **moderately expensive SQLs** from the **Top SQL by CPU Time** section.  
- Ensure background jobs or batch processes are optimized.
""")
    else:
        st.markdown("**❓ 1. CPU Usage — Data Unavailable**")

    # 2. DB Time (Avg Active Session)
    DB_TIME_THRESHOLD = 40

    if db_time > DB_TIME_THRESHOLD:
        st.markdown(f"""
**⚠️ 2. Avg Active Session ({db_time:.1f}s) — High**  
⚠️ **Observation:** AAS is **{db_time:.1f}s**, which may indicate **performance bottlenecks**.

**💡 Recommendation:**  
- Investigate queries in **Top SQL by Elapsed**.  
- Check for concurrency, locking, or resource contention.
""")
    else:
        st.markdown(f"""
**✅ 2. Avg Active Session ({db_time:.1f}s) — Normal**  
✅ **Observation:** AAS is **{db_time:.1f}s**, within the acceptable range.
""")





# 🧠 Intelligent Insights
st.markdown("### 🧠 Intelligent Insights")
if insights:
    for insight in insights:
        st.success(insight)
else:
    st.info("✅ No major anomalies or tuning issues detected.")



# ✅ Jump to Section (Full Navigation)
st.markdown("""
### 🧭 Main Report
- [🛠️ AWR Info](#awr-environment-info)
- [📊 Load Profile](#load-profile)
- [⏳ Wait Events](#top-5-wait-events-db-time)
- [🔥 Top SQL by Elapsed Time](#top-sql-by-elapsed-time)
- [⚡ Top SQL by CPU Time](#top-sql-by-cpu-time)
- [📄 Complete SQL Texts](#complete-sql-texts)
- [⚙️ Init Parameters](#initialization-parameters)
- [🥧 Segments by Physical Reads](#segments-by-physical-reads)
- [🔒 Segments by Row Lock Waits](#segments-by-row-lock-waits)
- [🔍 Segments by Table Scans](#segments-by-table-scans)
- [🧠 PGA Advisory](#advisory-statistics--pga-advisory)
- [💡 SGA Advisory](#sga-target-advisory)
- [📝 SQL Events](#top-sql-with-top-events)
- [📊 Activity Timeline](#activity-over-time)
""", unsafe_allow_html=True)



# Load Profile
st.markdown('<div id="load-profile"></div>', unsafe_allow_html=True)
st.markdown("### 📊 Load Profile (Per Second)")

if not data['load_profile'].empty:
    fig1 = px.bar(data['load_profile'], x='Metric', y='Per Second', title='Load Profile', color='Metric', text='Per Second')
    fig1.update_traces(texttemplate='%{text}', textposition='outside')
    fig1.update_layout(yaxis_title="", xaxis_title="", plot_bgcolor='#fff0f5')
    st.plotly_chart(fig1, use_container_width=True)
else:
    st.warning("Load Profile table not found.")

# Wait Events
# Wait Event Insights
wait_event_insights = {
    "db file sequential read": {
        "observation": "This wait event indicates that Oracle is performing single-block reads, typically due to index access. High occurrences may suggest inefficient query plans or suboptimal indexing.",
        "recommendation": "Review index usage and execution plans. Ensure appropriate indexes are in place. If this wait event contributes significantly to DB time, also verify that the SGA is properly sized and configured."
    },
    "db file scattered read": {
        "observation": "The db file scattered read wait event occurs when Oracle performs multiblock reads—typically during full table scans.",
        "recommendation": "Review SQL causing full table scans. Add proper indexes or rewrite queries. Update statistics and check I/O performance for large table scans."
    },
    "db file parallel read": {
        "observation": "Multiple block reads initiated in parallel, often during recovery, backups, or parallel queries.",
        "recommendation": "Check if parallel query or RMAN is running. Tune I/O subsystem and review execution plans for parallel reads. Avoid unnecessary parallelism if not needed."
    },
    "log file sync": {
        "observation": "This wait event happens when a session commits and waits for the LGWR (Log Writer) process to flush the redo log buffer to the redo log files on disk.",
        "recommendation": "Reduce frequent commits in application code. Investigate redo log disk I/O performance."
    },
    "log file switch (checkpoint incomplete)": {
        "observation": "This wait occurs when a log switch is attempted, but the next online redo log group is not yet available because a checkpoint has not completed for it.",
        "recommendation": "Add more online redo log groups or increase their size."
    },
    "log file parallel write": {
        "observation": "Time taken by LGWR to write redo log entries to disk.",
        "recommendation": "Check I/O performance of the disks hosting redo logs."
    },
    "direct path read": {
        "observation": "Used for reading directly into PGA, often during parallel queries or temp reads.",
        "recommendation": "Check temp usage and parallel query settings. Tune SQLs causing large direct reads. Ensure I/O subsystem can handle large read operations efficiently."
    },
    "direct path write": {
        "observation": "Occurs during parallel DML operations or sorts spilling to temp.",
        "recommendation": "Check temp file I/O performance and tune large parallel DML operations."
    },
    "TCP Socket (KGAS)": {
        "observation": "This wait event occurs when Oracle is waiting for TCP/IP network communication.",
        "recommendation": "Check network latency, listener configuration, remote DB responsiveness."
    },
    "enq: TX - row lock contention": {
        "observation": "This wait event occurs when a session is waiting to acquire a row-level lock that is already held by another session.",
        "recommendation": "Identify and tune blocking sessions. Avoid DML conflicts on the same rows from multiple sessions."
    },
    "enq: TM - contention": {
        "observation": "This wait event occurs when a session waits for a DML or DDL lock on a table that’s held by another session.",
        "recommendation": "Avoid unnecessary DDL. Avoid using APPEND hint in concurrent inserts, as it causes table-level locks."
    },
    "resmgr: cpu quantum": {
        "observation": "Oracle is controlling CPU usage, and a session must wait for its turn to use the CPU again because it has used up its time slice.",
        "recommendation": "Check and tune top CPU-consuming queries. Investigate system CPU usage at the OS level."
    },
    "read by other session": {
        "observation": "Buffer busy waits due to concurrent access by multiple sessions.",
        "recommendation": "Use partitioning or tune queries to reduce contention on hot blocks."
    },
    "buffer busy wait": {
        "observation": "Waits for access to data blocks in buffer cache.",
        "recommendation": "Tune SQL to reduce concurrent access. Reduce contention by spreading workload or partitioning."
    },
    "library cache lock": {
        "observation": "This wait event occurs when a session is waiting for a lock on an object in the library cache.",
        "recommendation": "Avoid DDL during peak load. Use bind variables to reduce parse load."
    },
    "cursor: pin S wait on X": {
        "observation": "Library cache concurrency when multiple sessions access cursors. Often due to invalidations.",
        "recommendation": "Reduce literal SQL usage. Use bind variables. Minimize DDL during business hours."
    },
    "Failed Logon Delay": {
        "observation": "Wait caused by failed login attempts.",
        "recommendation": "Review audit logs. Investigate authentication issues or invalid credentials."
    },
    "row cache lock": {
        "observation": "This wait indicates contention in the data dictionary cache (row cache), typically due to DDL operations or frequent hard parsing.",
        "recommendation": "Minimize DDLs and hard parsing during peak hours by using bind variables."
    },
    "direct path read temp": {
        "observation": "Frequent 'direct path read temp' waits suggest large operations are spilling to temp due to insufficient PGA or inefficient execution plans.",
        "recommendation": "Increase PGA memory if required and tune queries to minimize temp usage."
    },
    "direct path write temp": {
        "observation": "'Direct path write temp' occurs when large operations write data to temporary tablespaces, often due to sorts or hash joins exceeding memory.",
        "recommendation": "Increase PGA memory if required and tune queries to minimize temp usage."
    },
    "enq: UL – contention": {
        "observation": "A significant number of 'enq: UL – contention' waits indicates user-defined locks are competing, causing sessions to block or wait during DB operations.",
        "recommendation": "Avoid long-held user locks; review DBMS_LOCK usage and identify blocking sessions via v$session or dba_lock_internal."
    },
    "latch: shared pool": {
        "observation": "The latch: shared pool wait event occurs when a session is waiting to acquire a latch to access the shared pool area of the SGA.",
        "recommendation": "Reduce hard parses by using bind variables instead of literals."
    },
    "library cache load lock": {
        "observation": "The library cache load lock wait event occurs when sessions wait for objects to be loaded into the library cache, often due to high hard parses, frequent DDL, or shared pool pressure."
    },
    "enq: CF - contention": {
        "observation": "The enq: CF - contention wait event occurs when sessions compete for control file access, typically due to frequent updates from checkpoints, log switches, or backups. If it appears in the top wait events with high DB Time consumption, it may significantly impact database operations that rely on control file metadata."
    }
}



# ✅ Wait Event Visualization and Insights
st.markdown("### ⏳ Top 5 Wait Events (% DB Time)")

if 'wait_events' in data and isinstance(data['wait_events'], pd.DataFrame) and not data['wait_events'].empty:
    df_waits = data['wait_events'].copy()
    df_waits.columns = df_waits.columns.str.strip()

    # Bar chart
    fig2 = px.bar(
        df_waits.head(5),  # Top 5 only
        x='Event',
        y='% DB time',
        title='Top 5 Wait Events by % DB Time',
        color='Event',
        text='% DB time'
    )
    fig2.update_traces(texttemplate='%{text:.1f}', textposition='outside')
    fig2.update_layout(yaxis_title="", xaxis_title="", plot_bgcolor='#f0f8ff')
    st.plotly_chart(fig2, use_container_width=True)

    # Insights styled like your screenshot
    with st.expander("View Observations & Recommendations"):
        shown_any = False
        for idx, (_, row) in enumerate(df_waits.head(5).iterrows(), 1):
            event_name = row['Event']
            db_time_pct = row['% DB time']

            if event_name in wait_event_insights:
                shown_any = True
                insight = wait_event_insights[event_name]
                recommendations = insight["recommendation"]

                st.markdown(
                    f"""<div style="border:1px solid #ccc;padding:10px;border-radius:8px;margin-bottom:10px;">
                    <h4 style="margin:0;">🟢 {idx}. {event_name} ({db_time_pct:.1f}% DB Time)</h4>
                    <p><strong>✅ Observation:</strong> {insight['observation']}</p>
                    <p><strong>💡 Recommendation:</strong></p>
                    <ul>
                    {"".join(f"<li>{rec}</li>" for rec in (recommendations if isinstance(recommendations, list) else [recommendations]))}
                    </ul>
                    </div>""",
                    unsafe_allow_html=True
                )

        if not shown_any:
            st.info("ℹ️ No insights available for the Top 5 wait events in this report.")
else:
    st.warning("⚠️ Wait Events table not found or is empty.")







# --------------------------------------------
# 🐢 Top SQL by Elapsed Time Section (New Code)
# --------------------------------------------
st.markdown("### 🔥 Top SQL by Elapsed Time")

if 'top_sql' in data and isinstance(data['top_sql'], pd.DataFrame) and not data['top_sql'].empty:
    df_top_sql = data['top_sql'].copy()

    # Normalize column names: strip spaces, uppercase, remove duplicate spaces
    df_top_sql.columns = df_top_sql.columns.str.strip().str.upper().str.replace(r'\s+', ' ', regex=True)

    # Map possible variations of column names to standard ones
    rename_map = {
        'SQL ID': 'SQL ID',
        'SQLID': 'SQL ID',
        'SQL Id': 'SQL ID',
        'SQL TEXT': 'SQL TEXT',
        'ELAPSED TIME (S)': 'ELAPSED TIME (S)',
        'ELAPSED TIME PER EXEC (S)': 'ELAPSED TIME PER EXEC (S)',
        'EXECUTIONS': 'EXECUTIONS'
    }
    df_top_sql.rename(columns=rename_map, inplace=True)

    required_columns = ['SQL ID', 'SQL TEXT', 'ELAPSED TIME (S)', 'EXECUTIONS', 'ELAPSED TIME PER EXEC (S)']

    if all(col in df_top_sql.columns for col in required_columns):
        # Convert numeric columns
        df_top_sql['ELAPSED TIME (S)'] = pd.to_numeric(df_top_sql['ELAPSED TIME (S)'], errors='coerce')
        df_top_sql['EXECUTIONS'] = pd.to_numeric(df_top_sql['EXECUTIONS'], errors='coerce').fillna(0).astype(int)
        df_top_sql['ELAPSED TIME PER EXEC (S)'] = pd.to_numeric(df_top_sql['ELAPSED TIME PER EXEC (S)'], errors='coerce')

        # Create bar chart using SQL ID directly for y-axis
        fig_elapsed = px.bar(
            df_top_sql.sort_values('ELAPSED TIME (S)'),
            y='SQL ID',
            x='ELAPSED TIME (S)',
            orientation='h',
            text='ELAPSED TIME (S)',
            title='Top SQL by Elapsed Time',
            color='ELAPSED TIME (S)',
            color_continuous_scale=["#800000", "#B22222", "#DC143C", "#FF6347"],
            hover_data={
                'SQL ID': True,
                'SQL TEXT': True,
                'ELAPSED TIME (S)': True,
                'EXECUTIONS': True,
                'ELAPSED TIME PER EXEC (S)': True
            }
        )

        fig_elapsed.update_layout(
            yaxis_title="SQL ID",
            xaxis_title="Elapsed Time (s)",
            plot_bgcolor='#fffaf0',
            height=40 * len(df_top_sql)
        )

        st.plotly_chart(fig_elapsed, use_container_width=True)

        # Observation & Recommendation
        with st.expander("View Observations & Recommendations"):
            st.markdown("""
**📝 Observation:**  
This section lists SQL statements that consumed the most **elapsed time** during the snapshot period. High elapsed time often indicates that these SQLs are either long-running or executed very frequently, significantly affecting database performance.

**✅ Recommendations:**  
- **Check ELAPSED TIME (S) and ELAPSED TIME PER EXEC (S) to see if queries are slow. If very high, follow the recommendations below.**
- **Review execution plans** using DBMS_XPLAN.DISPLAY_CURSOR.
- **Tune inefficient SQLs**: Look for full table scans, bad join orders, missing indexes.
- **Stabilize plans** with SQL Plan Baselines if plans change frequently and fix the good plan.
            """)
    else:
        st.error(f"❌ Required columns not found. Found columns: {df_top_sql.columns.tolist()}")
else:
    st.warning("⚠️ 'Top SQL by Elapsed Time' data not found or is empty.")







# Top SQL by CPU Time (Graphical)
st.markdown("### ⚡ Top SQL by CPU Time")

if not data['top_cpu_sql'].empty:
    df_cpu_sql = data['top_cpu_sql'].copy()

    # Normalize column names
    df_cpu_sql.columns = df_cpu_sql.columns.str.strip().str.upper()

    # Required columns
    required_columns = ['SQL ID', 'SQL TEXT', 'CPU TIME (S)']
    if all(col in df_cpu_sql.columns for col in required_columns):

        # Parse CPU Time column safely
        df_cpu_sql['CPU TIME (S)'] = pd.to_numeric(df_cpu_sql['CPU TIME (S)'], errors='coerce')

        # Bar Chart using only SQL ID for Y-axis
        fig_cpu = px.bar(
            df_cpu_sql.sort_values('CPU TIME (S)'),
            y='SQL ID',
            x='CPU TIME (S)',
            orientation='h',
            text='CPU TIME (S)',
            title='Top SQL by CPU Time',
            color='CPU TIME (S)',
            color_continuous_scale=["#00008B", "#0000CD", "#4169E1", "#6495ED", "#87CEFA"],
            hover_data={
                'SQL ID': True,
                'SQL TEXT': True,
                'CPU TIME (S)': True
            }
        )

        fig_cpu.update_layout(
            yaxis_title="SQL ID",
            xaxis_title="CPU Time (s)",
            plot_bgcolor='#f8ffff',
            margin=dict(l=10, r=10, t=40, b=10),
            height=40 * len(df_cpu_sql),
            uniformtext_mode='show'
        )

        fig_cpu.update_traces(
            texttemplate='%{text:.2f}',
            textposition='outside'
        )

        st.plotly_chart(fig_cpu, use_container_width=True)

        # ✅ Observation and Recommendation in dropdown
        max_cpu = df_cpu_sql['CPU TIME (S)'].max()
        if max_cpu > 10:
            with st.expander("View Observation and Recommendation"):
                st.markdown(f"""
**🧠 Observation:**  
This section highlights the SQL statements that consumed the most CPU time during the snapshot period. High CPU usage typically indicates that these SQLs are resource-intensive and may significantly affect overall database performance. If the idle CPU percentage is very low and CPU resources are exhausted, check the recommendations below.

💡 **Recommendation:**
- Check top CPU consuming queries. If CPU time(s) very high, follow the recommendations below.
- Investigate whether parallel execution is appropriate for these queries.
- Review execution plans for high CPU-consuming SQLs.
- Tune SQL logic and access paths (joins, filters, indexes).
- Use bind variables and avoid unnecessary computations in queries.
- Check if there is another OS-level process consuming more CPU. 
""")
        else:
            st.markdown("✅ No SQLs with significant CPU usage detected.")

    else:
        st.error("Required columns ('SQL ID', 'SQL TEXT', 'CPU TIME (S)') not found in Top SQL by CPU Time data.")
else:
    st.warning("Top SQL by CPU Time not found.")







# ✅ FULLY FIXED Scroll-Aware Anchors + Expanders
# These changes ensure TOC jumps work AND expanders remain clickable in Streamlit

# Initialize session state to track expander open state
if "sql_expander_open" not in st.session_state:
    st.session_state.sql_expander_open = False

if not data.get('full_sql_texts', pd.DataFrame()).empty:
    df_sql_texts = data['full_sql_texts'].copy()
    df_sql_texts.columns = df_sql_texts.columns.str.strip().str.upper()
    sql_ids = df_sql_texts['SQL ID'].dropna().unique().tolist()

    st.markdown('<div id="complete-sql-texts"></div>', unsafe_allow_html=True)
    st.write("")

    with st.expander("🔽 Click to View Full SQL Text by SQL ID", expanded=st.session_state.sql_expander_open):
        selected_sql_id = st.selectbox("Select SQL ID to view full SQL Text:", sql_ids, key="sql_dropdown")
        selected_row = df_sql_texts[df_sql_texts['SQL ID'] == selected_sql_id]

        if not selected_row.empty:
            st.session_state.sql_expander_open = True
            st.code(selected_row.iloc[0]['SQL TEXT'], language='sql')
        else:
            st.warning("SQL Text not found for the selected SQL ID.")
else:
    st.info("Complete List of SQL Text not found in AWR report.")

# Initialization Parameters
st.markdown('<div id="initialization-parameters"></div>', unsafe_allow_html=True)
st.write("")
with st.expander("⚙️ Initialization Parameters", expanded=False):
    if not data['init_params'].empty:
        st.dataframe(data['init_params'], use_container_width=True)
    else:
        st.warning("Initialization Parameters section not found.")

# Segments by Physical Reads
st.markdown('<div id="segments-by-physical-reads"></div>', unsafe_allow_html=True)
st.write("")
with st.expander("📊 Segments by Physical Reads", expanded=False):
    if not data['seg_physical_reads'].empty:
        chart_df = data['seg_physical_reads'].copy()
        chart_df['Physical Reads'] = chart_df['Physical Reads'].replace(',', '', regex=True)
        chart_df['Physical Reads'] = pd.to_numeric(chart_df['Physical Reads'], errors='coerce')

        # Bar chart
        fig = px.bar(
            chart_df.sort_values(by='Physical Reads', ascending=False).head(10),
            x='Object Name',
            y='Physical Reads',
            text='Physical Reads',
            color_discrete_sequence=["#3498db"]
        )
        fig.update_traces(textposition='outside')
        fig.update_layout(
            title="Top 10 Segments by Physical Reads",
            xaxis_title="Object Name",
            yaxis_title="Physical Reads",
            plot_bgcolor="#f8f9fa",
            paper_bgcolor="#ffffff",
            showlegend=False
        )
        fig.update_xaxes(tickangle=-45)
        st.plotly_chart(fig, use_container_width=True)

        # Nested dropdown for Observation & Recommendation
        with st.expander("View Observation and Recommendation", expanded=False):
            st.markdown(
                """
                ### 📦 **Observation**  
                Some database segments are responsible for high physical reads, indicating frequent disk access and potential I/O bottlenecks during the snapshot period. If the physical read wait event is consistently observed in the Wait Events section, review the Segments by Physical Reads section to identify which segments have higher physical reads, and check the recommendations below.
                
                ### 💡 **Recommendation**  
                - Review execution plans for SQL statements that access high physical read tables to avoid unnecessary full table scans.  
                - Optimize indexes and ensure table/index statistics are up to date.  
                - Consider partitioning large tables for better I/O performance.  
                - Increase the SGA size if required, as indicated by the SGA Advisory, to help reduce I/O.  
                """
            )

    else:
        st.warning("Segments by Physical Reads section not found.")



# Segments by Row Lock Waits
st.markdown('<div id="segments-by-row-lock-waits"></div>', unsafe_allow_html=True)
st.write("")
with st.expander("📊 Segments by Row Lock Waits", expanded=False):
    if not data['seg_row_lock_waits'].empty:
        chart_df = data['seg_row_lock_waits'].copy()
        chart_df['Row Lock Waits'] = chart_df['Row Lock Waits'].replace(',', '', regex=True)
        chart_df['Row Lock Waits'] = pd.to_numeric(chart_df['Row Lock Waits'], errors='coerce')

        # Bar chart
        fig = px.bar(
            chart_df.sort_values(by='Row Lock Waits', ascending=False).head(10),
            x='Object Name',
            y='Row Lock Waits',
            text='Row Lock Waits',
            color_discrete_sequence=["#3498db"]
        )
        fig.update_traces(textposition='outside')
        fig.update_layout(
            title="Top Segments by Row Lock Waits",
            xaxis_title="Object Name",
            yaxis_title="Row Lock Waits",
            plot_bgcolor="#f8f9fa",
            paper_bgcolor="#ffffff",
            showlegend=False
        )
        fig.update_xaxes(tickangle=-45)
        st.plotly_chart(fig, use_container_width=True)

        # Nested dropdown for Observation & Recommendation
        with st.expander("View Observation and Recommendation", expanded=False):
            st.markdown(
                """
                ### 📦 **Observation**  
                Certain database segments are experiencing elevated row lock waits, indicating contention from concurrent DML operations attempting to modify the same rows simultaneously. This can lead to blocking sessions and reduced transaction throughput. If contention is observed in the database, this section can help identify the tables involved. You can then determine which queries are accessing these tables to gain insight into the cause, and checkthe recommendations below. In some cases, this section(Top SQL with Top Events) also captures which wait events occurred in specific SQL statements, allowing further analysis.
                
                ### 💡 **Recommendation**  
                - Identify SQL statements causing the highest row lock waits using ASH/AWR SQL details.  
                - Redesign application logic to minimize concurrent updates/deletes on the same rows at the same time.  
                - Add appropriate indexes if queries are performing full table scans.  
                """
            )

    else:
        st.warning("Segments by Row Lock Waits section not found.")


# Segments by Table Scans
st.markdown('<div id="segments-by-table-scans"></div>', unsafe_allow_html=True)
st.write("")
with st.expander("📊 Segments by Table Scans", expanded=False):
    if not data['seg_table_scans'].empty:
        chart_df = data['seg_table_scans'].copy()
        chart_df['Table Scans'] = chart_df['Table Scans'].replace(',', '', regex=True)
        chart_df['Table Scans'] = pd.to_numeric(chart_df['Table Scans'], errors='coerce')

        # Bar chart
        fig = px.bar(
            chart_df.sort_values(by='Table Scans', ascending=False).head(10),
            x='Object Name',
            y='Table Scans',
            text='Table Scans',
            color_discrete_sequence=["#e67e22"]
        )
        fig.update_traces(textposition='outside')
        fig.update_layout(
            title="Top Segments by Table Scans",
            xaxis_title="Object Name",
            yaxis_title="Table Scans",
            plot_bgcolor="#f8f9fa",
            paper_bgcolor="#ffffff",
            showlegend=False
        )
        fig.update_xaxes(tickangle=-45)
        st.plotly_chart(fig, use_container_width=True)

        # Observation & Recommendation dropdown
        with st.expander("View Observation and Recommendation", expanded=False):
            st.markdown(
                """
                ### 📦 **Observation**  
                Some database segments recorded a high number of table scans during the snapshot period. Frequent table scans, particularly on large tables, can lead to excessive I/O and CPU usage, and may indicate missing indexes, stale statistics, or suboptimal SQL design. Analyze this section to identify the tables with the most full table scans, then determine the SQL statements accessing those tables and assess any tuning opportunities to improve performance and reduce I/O, and check the recommendations below.
                
                ### 💡 **Recommendation**  
                - Identify SQL statements causing the highest table scans using AWR/ASH SQL details.  
                - Create or optimize indexes to improve query selectivity and reduce full table scans.  
                - Refresh table and index statistics to help the optimizer choose better execution plans.  
                - Consider table partitioning to reduce scan size for large datasets.  
                """
            )

    else:
        st.warning("Segments by Table Scans section not found.")



# PGA Advisory
st.markdown('<div id="advisory-statistics--pga-advisory"></div>', unsafe_allow_html=True)
st.write("")
with st.expander("📈 Advisory Statistics – PGA Advisory", expanded=False):
    if data.get('pga_advisory') is not None and not data['pga_advisory'].empty:
        df = data['pga_advisory'][['PGA Target Est (MB)', 'Size Factr', 'Estd Time']].copy()
        df['PGA Target Est (MB)'] = df['PGA Target Est (MB)'].replace(',', '', regex=True).astype(float)
        df['Size Factr'] = df['Size Factr'].astype(str)
        df['Estd Time'] = df['Estd Time'].replace(',', '', regex=True).astype(float)

        # Pie chart
        fig = go.Figure(go.Pie(
            labels=df['PGA Target Est (MB)'],
            values=df['Estd Time'],
            text=df['Size Factr'],
            textinfo='text',
            textposition='inside',
            marker=dict(line=dict(color='#000000', width=1)),
            hovertemplate="<b>PGA Target Est (MB):</b> %{label}<br><b>Estd Time:</b> %{value}<extra></extra>"
        ))
        fig.update_layout(
            title="PGA Advisory – Estd Time by PGA Target Est (MB)",
            showlegend=False,
            template='plotly_dark',
            margin=dict(t=50, b=0, l=0, r=0)
        )

        chart_col, info_col = st.columns([3, 1])
        with chart_col:
            st.plotly_chart(fig, use_container_width=True)
        with info_col:
            st.info("**How to Read**\n\n- **Each slice** = `PGA Target Est (MB)`\n- **Slice size** = `Estd Time`\n- **Inside label** = `Size Factr`\n- **Hover shows**: PGA Target Est (MB), Estd Time")

        # ✅ Determine current PGA row using Size Factr = 1
        current_row = df[df['Size Factr'].astype(float) == 1]
        if not current_row.empty:
            current_time = current_row['Estd Time'].values[0]
        else:
            # Fallback to middle row if no Size Factr = 1 found
            current_time = df.iloc[len(df) // 2]['Estd Time']

        min_time = df['Estd Time'].min() if not df.empty else None

        # ✅ Determine recommendation message
        if current_time is not None and min_time is not None and current_time > 0:
            improvement_ratio = (current_time - min_time) / current_time
            if improvement_ratio > 0.1:  # More than 10% improvement
                pga_message = "➡ **PGA Advisory detected — increasing PGA size may improve performance.**"
            else:
                pga_message = "✅ **PGA size is appropriate based on advisory data.**"
        else:
            pga_message = "ℹ **Unable to determine PGA tuning recommendation due to missing data.**"

        # Observation & Recommendation dropdown
        with st.expander("View Observations & Recommendations", expanded=False):
            st.markdown(f"""
**📝 Observation:**  
The PGA Advisory estimates how PGA size affects work area performance.  
If a larger PGA greatly reduces execution time or disk usage, increasing PGA memory may improve performance.  
{pga_message}  

**💡 Recommendation:**  
- Compare the current estimated time with the minimum estimated time across different PGA sizes.  
- If the estimated time consistently decreases with larger PGA sizes, consider increasing PGA size if required.  
- Before increasing, ensure sufficient server memory is available, keeping at least 20% reserved for the OS as recommended.  
            """)

    else:
        st.info("PGA Memory Advisory data not found.")






# SGA Advisory
st.markdown('<div id="sga-target-advisory"></div>', unsafe_allow_html=True)
st.write("")
with st.expander("▶️ SGA Target Advisory", expanded=False):
    st.markdown("#### SGA Target Advisory")
    if data.get('sga_advisory') is not None and not data['sga_advisory'].empty:
        sga_df = data['sga_advisory'].copy()
        sga_df.columns = [str(c).strip() for c in sga_df.columns]

        # Convert numeric columns
        for col in ['SGA Target Size (M)', 'SGA Size Factor', 'Est DB Time (s)', 'Est Physical Reads']:
            sga_df[col] = sga_df[col].replace(',', '', regex=True).astype(float)

        # Create pie chart
        fig = px.pie(
            sga_df,
            values='Est DB Time (s)',
            names='SGA Target Size (M)',
            hover_data=['SGA Target Size (M)', 'Est DB Time (s)', 'Est Physical Reads']
        )
        fig.update_traces(
            text=sga_df['SGA Size Factor'].astype(str),
            textposition='inside',
            textinfo='text',
            hovertemplate=("SGA Target Size (M): %{label}<br>Est DB Time (s): %{value}"
                           "<br>Est Physical Reads: %{customdata[0]}<extra></extra>"),
            customdata=sga_df[['Est Physical Reads']],
            showlegend=False
        )
        fig.update_layout(
            title='Estimated DB Time vs SGA Target Size Advisory',
            uniformtext_minsize=12,
            uniformtext_mode='hide',
            margin=dict(t=40, b=0, l=0, r=0)
        )

        col1, col2 = st.columns([3, 1])
        with col1:
            st.plotly_chart(fig, use_container_width=True)
        with col2:
            st.info("**How to Read**\n\n- Each slice = SGA Target Size (M)\n- Slice size = Est DB Time (s)\n- Inside label = Size Factor\n- Hover shows:\n  • SGA Target Size (M)\n  • Est DB Time (s)\n  • Est Physical Reads")

        # ✅ Detect current SGA row using Size Factor = 1
        current_row = sga_df[sga_df['SGA Size Factor'] == 1]
        if not current_row.empty:
            current_reads = current_row['Est Physical Reads'].values[0]
        else:
            # Fallback: use middle row if no Size Factor == 1 found
            current_reads = sga_df.iloc[len(sga_df) // 2]['Est Physical Reads']

        min_reads = sga_df['Est Physical Reads'].min()

        # ✅ Determine message
        if current_reads is not None and min_reads is not None and current_reads > 0:
            improvement_ratio = (current_reads - min_reads) / current_reads
            if improvement_ratio > 0.1:  # More than 10% improvement
                observation_msg = "➡ **SGA Advisory detected — increasing SGA size may improve performance by reducing physical reads.**"
            else:
                observation_msg = "✅ **SGA size is appropriate based on advisory data.**"
        else:
            observation_msg = "Unable to determine SGA tuning recommendation due to missing data."

        # Observation & Recommendation Dropdown
        with st.expander("View Observations & Recommendations", expanded=False):
            st.markdown(f"""
**📝 Observation:**  
{observation_msg}

**💡 Recommendation:**  
- Compare current estimated physical reads with the minimum value across different SGA sizes.  
- If physical reads consistently decrease with larger SGA sizes, consider increasing SGA size if required.  
- Ensure sufficient system memory is available, keeping at least 20% reserved for the OS.  
            """)

    else:
        st.warning("SGA Target Advisory section not found or empty.")






# Top SQL with Top Events
st.markdown('<div id="top-sql-with-top-events"></div>', unsafe_allow_html=True)
st.write("")
with st.expander("📝 Top SQL with Top Events", expanded=False):
    if not data['top_sql_events'].empty:
        chart_df = data['top_sql_events'].copy()

        # ✅ Clean and normalize column names so 'Top Row Source' is always detected
        chart_df.columns = [col.replace('\n', ' ').strip() for col in chart_df.columns]

        chart_df.rename(columns={
            'SQL ID': 'sql_id',
            'Plan Hash': 'plan_hash',
            'Executions': 'executions',
            'Event': 'event',
            'Top Row Source': 'top_row_source',
            'SQL Text': 'sql_text'
        }, inplace=True)

        # Fill missing values so no row gets dropped
        chart_df = chart_df[['sql_id', 'plan_hash', 'executions', 'event', 'top_row_source', 'sql_text']]
        chart_df = chart_df.fillna('N/A').astype(str).apply(lambda x: x.str.strip())

        # Convert numeric columns
        chart_df['executions'] = pd.to_numeric(chart_df['executions'], errors='coerce').fillna(0).astype(int)
        chart_df['plan_hash'] = pd.to_numeric(chart_df['plan_hash'], errors='coerce').fillna(0).astype(int)

        # Group by SQL ID and event to ensure all IDs are considered
        grouped_df = chart_df.groupby(['sql_id', 'event'], as_index=False).agg({
            'executions': 'sum',
            'plan_hash': 'first',
            'top_row_source': 'first',
            'sql_text': 'first'
        })

        # Pick top N SQL IDs by total executions (sum of all their events)
        top_sql_ids = grouped_df.groupby('sql_id')['executions'].sum().nlargest(6).index
        chart_df = grouped_df[grouped_df['sql_id'].isin(top_sql_ids)]

        # Sort for horizontal bar chart
        chart_df.sort_values(by='executions', ascending=True, inplace=True)

        if not chart_df.empty:
            fig = px.bar(
                chart_df,
                x='executions',
                y='sql_id',
                orientation='h',
                text='executions',
                title='Top SQL with Top Events - Executions',
                color='event',
                hover_data={
                    'sql_id': True,
                    'plan_hash': True,
                    'top_row_source': True,
                    'executions': True,
                    'event': True,
                    'sql_text': True
                }
            )
            fig.update_layout(yaxis_title='SQL ID', xaxis_title='Executions', height=400)
            st.plotly_chart(fig, use_container_width=True)

            # Observation & Recommendation dropdown
            with st.expander("📄 Observation & Recommendation", expanded=False):
                st.markdown(
                    """
                    ### 📦 **Observation**  
                    This section displays SQL statements linked to the highest wait events during the snapshot period, detailing both the type and duration of waits for each SQL.  
                    It also includes the **Top Row Source** info, which can reveal expensive operations like `TABLE ACCESS FULL`.

                    ### 💡 **Recommendation**  
                    - Focus tuning efforts on SQL statements where critical wait events are observed, as these can degrade performance.
                    - Pay special attention to queries performing **FULL TABLE SCANS** (check `top_row_source` column).
                    - Consider creating indexes, adding selective WHERE clauses, or partitioning large tables.
                    """
                )
        else:
            st.info("No valid rows found after filtering.")
    else:
        st.warning("Top SQL with Top Events section not found.")



# Activity Over Time
st.markdown('<div id="activity-over-time"></div>', unsafe_allow_html=True)
st.write("")
with st.expander("📊 Activity Over Time", expanded=False):
    if not data['activity_over_time'].empty:
        chart_df = data['activity_over_time'].copy()
        chart_df.rename(columns={
            'Slot Time (Duration)': 'slot_time', 'Event': 'event', 'Event Count': 'event_count', '% Event': 'percent_event'
        }, inplace=True)
        chart_df = chart_df[['slot_time', 'event', 'event_count', 'percent_event']]
        chart_df = chart_df.fillna('').astype(str).apply(lambda x: x.str.strip())
        chart_df['event_count'] = pd.to_numeric(chart_df['event_count'], errors='coerce').fillna(0).astype(int)
        chart_df['percent_event'] = pd.to_numeric(chart_df['percent_event'], errors='coerce').fillna(0)
        chart_df = chart_df[chart_df['event_count'] > 0]
        if not chart_df.empty:
            fig = px.bar(chart_df, x='slot_time', y='event_count', color='event', text='event_count',
                         title='Activity Over Time - Event Count by Slot Time and Event')
            fig.update_layout(xaxis_title='Slot Time (Duration)', yaxis_title='Event Count', height=450, barmode='stack')
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No valid Event data found for Activity Over Time.")
    else:
        st.warning("Activity Over Time section not found.")




# 📥 Excel Download as Custom Green Button

# Include ASH data and summary in the Excel export
ash_df = data.get('ash_data', pd.DataFrame())
ash_top_events = pd.DataFrame()
if not ash_df.empty and 'Event' in ash_df.columns:
    ash_top_events = (
        ash_df['Event']
        .value_counts()
        .reset_index()
        .rename(columns={'index': 'Event', 'Event': 'Count'})
        .head(10)
    )

# All sections
report_dict = {
    # ✅ Environment Info comes first
    'Environment_Info': pd.DataFrame({
        'Database Name':      [data['db_name']],
        'DB Time (s)':        [data['load_profile'][data['load_profile']['Metric'].str.contains("DB Time", na=False)]['Per Second'].values[0] if not data['load_profile'].empty else "N/A"],
        'Idle CPU (%)':       [f"{data['idle_cpu']:.1f}" if data['idle_cpu'] is not None else "N/A"],

        'Instance':           [data['instance_name']],
        'Instance Number':    [data['instance_num']],
        'Startup Time':       [data['startup_time']],
        'Total CPUs':         [data['total_cpu']],
        'RAC Status':         [data['rac_status']],
        'Edition/Release':    [data['edition']],
        'Memory (GB)':        [data['memory_gb']],
        'Platform':           [data['platform']],
        'CDB Status':         [data['cdb_status']],
        'Begin Snap Time':    [data['begin_snap_time']],
        'End Snap Time':      [data['end_snap_time']]
    }),

    # Existing sections follow
    'Load_Profile': data['load_profile'],
    'Wait_Events': data['wait_events'],
    'Top_SQL_Elapsed': data['top_sql'],
    'Top_SQL_CPU': data['top_cpu_sql'],
    'Init_Params': data['init_params'],
    'PGA_Advisory': data['pga_advisory'],
    'SGA_Advisory': data['sga_advisory'],
    'Segments_Physical_Reads': data['seg_physical_reads'],
    'Segments_Row_Lock_Waits': data['seg_row_lock_waits'],
    'Segments_Table_Scans': data['seg_table_scans'],
    'Top_SQL_Events': data['top_sql_events'],
    'Activity_Over_Time': data['activity_over_time'],

    # ✅ Full SQL Texts Section
    'Complete_SQL_Texts': data['full_sql_texts'] if not data.get('full_sql_texts', pd.DataFrame()).empty else pd.DataFrame()
}





# Convert to Excel
def to_excel(df_dict):
    output = BytesIO()
    writer = pd.ExcelWriter(output, engine='xlsxwriter')
    for sheet, df in df_dict.items():
        df.to_excel(writer, index=False, sheet_name=sheet[:31])
    writer.close()
    return output.getvalue()

excel_bytes = to_excel(report_dict)
excel_b64 = base64.b64encode(excel_bytes).decode()

# Green styled download button
st.markdown("### 📊 Download Excel Report")
excel_download_link = f'''
<a href="data:application/vnd.openxmlformats-officedocument.spreadsheetml.sheet;base64,{excel_b64}" download="awr_full_report.xlsx">
    <button style="
        background-color: #4CAF50;
        border: none;
        color: white;
        padding: 10px 20px;
        text-align: center;
        text-decoration: none;
        display: inline-block;
        font-size: 16px;
        border-radius: 8px;
        cursor: pointer;">
    📥 Download AWR Full Excel Report
    </button>
</a>
'''
st.markdown(excel_download_link, unsafe_allow_html=True)


# 📄 Full AWR Text Report Download Button

# Helper to convert DataFrame to plain text
def df_to_text(df, title):
    if df.empty:
        return f"\n\n{title}\n{'-' * len(title)}\nNo data available."
    else:
        return f"\n\n{title}\n{'-' * len(title)}\n{df.to_string(index=False)}"

# Build full report content
pdf_content = f"""Oracle AWR Analyzer - Full Text Report

"""

# Add data sections
# Add Environment Info to Text Report
pdf_content += "\n\n🛠️ AWR Environment Info\n" + "-" * 30
pdf_content += f"\nDatabase Name:      {data['db_name']}"
pdf_content += f"\nIdle CPU (%):       {f'{data['idle_cpu']:.1f}' if data['idle_cpu'] is not None else 'N/A'}"
pdf_content += f"\nDB Time (s):        {data['load_profile'][data['load_profile']['Metric'].str.contains('DB Time', na=False)]['Per Second'].values[0] if not data['load_profile'].empty else 'N/A'}"
pdf_content += f"\nInstance:           {data['instance_name']}"
pdf_content += f"\nInstance Number:    {data['instance_num']}"
pdf_content += f"\nStartup Time:       {data['startup_time']}"
pdf_content += f"\nTotal CPUs:         {data['total_cpu']}"
pdf_content += f"\nRAC Status:         {data['rac_status']}"
pdf_content += f"\nEdition/Release:    {data['edition']}"
pdf_content += f"\nMemory (GB):        {data['memory_gb']}"
pdf_content += f"\nPlatform:           {data['platform']}"
pdf_content += f"\nCDB Status:         {data['cdb_status']}"
pdf_content += f"\nBegin Snap Time:    {data['begin_snap_time']}"
pdf_content += f"\nEnd Snap Time:      {data['end_snap_time']}"

pdf_content += df_to_text(data['load_profile'], "📊 Load Profile (Per Second)")
pdf_content += df_to_text(data['wait_events'], "⏳ Top Wait Events (% DB Time)")
pdf_content += df_to_text(data['top_sql'], "🔥 Top SQL by Elapsed Time")
pdf_content += df_to_text(data['top_cpu_sql'], "⚡ Top SQL by CPU Time")
pdf_content += df_to_text(data['init_params'], "⚙️ Initialization Parameters")
pdf_content += df_to_text(data['pga_advisory'], "🧠 PGA Memory Advisory")
pdf_content += df_to_text(data['sga_advisory'], "💡 SGA Target Advisory")
pdf_content += df_to_text(data['seg_physical_reads'], "🥧 Segments by Physical Reads")
pdf_content += df_to_text(data['seg_row_lock_waits'], "🔒 Segments by Row Lock Waits")
pdf_content += df_to_text(data['seg_table_scans'], "🔍 Segments by Table Scans")
pdf_content += df_to_text(data['top_sql_events'], "📝 Top SQL with Top Events")
pdf_content += df_to_text(data['activity_over_time'], "📊 Activity Over Time Breakdown")

# ✅ Add Complete List of SQL Text Section
if not data.get('full_sql_texts', pd.DataFrame()).empty:
    pdf_content += "\n\n📄 Complete List of SQL Text\n" + "-" * 30
    for _, row in data['full_sql_texts'].iterrows():
        pdf_content += f"\n\nSQL ID: {row['SQL ID']}\nSQL Text:\n{row['SQL TEXT']}\n"









# Add ASH if available
ash_df = data.get("ash_data", pd.DataFrame())
if not ash_df.empty and 'Event' in ash_df.columns:
    top_ash = (
        ash_df['Event']
        .value_counts()
        .reset_index()
        .rename(columns={'index': 'Event', 'Event': 'Count'})
        .head(10)
    )
    pdf_content += df_to_text(top_ash, "🧠 Top 10 ASH Wait Events")

# Encode full content
pdf_b64 = base64.b64encode(pdf_content.encode()).decode()

# Stylish download button
st.markdown("### 📄 Download Full Text Report")
download_link = f'''
<a href="data:application/octet-stream;base64,{pdf_b64}" download="awr_full_report.txt">
    <button style="
        background-color: #4CAF50;
        border: none;
        color: white;
        padding: 10px 20px;
        text-align: center;
        text-decoration: none;
        display: inline-block;
        font-size: 16px;
        border-radius: 8px;
        cursor: pointer;">
    📥 Download AWR Full Text Report
    </button>
</a>
'''
st.markdown(download_link, unsafe_allow_html=True)

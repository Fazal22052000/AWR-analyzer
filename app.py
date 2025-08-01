import streamlit as st
import pandas as pd
import plotly.express as px
from bs4 import BeautifulSoup
import base64
import plotly.graph_objects as go
import plotly.express as px
from io import BytesIO




st.set_page_config(page_title="AWR Analyzer", layout="wide")



# 🌙 Dark mode toggle can go here inside authenticated block
dark_mode = st.sidebar.toggle("🌙 Dark Mode", value=False)

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
                    cols = [str(c).strip().lower() for c in df.columns]
                    if all(col in cols for col in ['sql id', 'sql text', 'elapsed time (s)']):
                        top_sql = df[[c for c in df.columns if str(c).lower() in ['sql id', 'sql text', 'elapsed time (s)']]]
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
                    top_sql_events = df
            break

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


# --- Add this BELOW your parse_awr() function ---
def get_intelligent_insights(data):
    insights = []

    # Rule: High DB Time per second
    if not data['load_profile'].empty:
        try:
            db_time_row = data['load_profile'][data['load_profile']['Metric'].str.contains("DB Time", na=False)]
            db_time = float(db_time_row['Per Second'].values[0])
            if db_time > 40.0:
                insights.append(f"🔥 [CRITICAL] High average active Session detected: {db_time:.2f}/sec — High load on database.")
        except:
            pass

    # Rule: SGA Advisory suggests increasing SGA size could reduce estimated physical reads and improve buffer cache performance
    if not data['sga_advisory'].empty:
        sga_df = data['sga_advisory'].copy()
        try:
            sga_df.columns = sga_df.columns.str.strip()
            sga_df['Est Physical Reads'] = pd.to_numeric(sga_df['Est Physical Reads'].replace(',', '', regex=True), errors='coerce')
            current_physical_reads = sga_df.iloc[len(sga_df)//2]['Est Physical Reads']
            min_physical_reads = sga_df['Est Physical Reads'].min()
            if pd.notna(current_physical_reads) and (current_physical_reads - min_physical_reads) > 1000:
                insights.append("⚠️ [WARNING] SGA Advisory suggests increasing SGA size could reduce estimated physical reads and improve buffer cache performance.")
        except:
            pass

    # Rule: Detect contention events (TX/TM)
    if not data['wait_events'].empty:
        try:
            df = data['wait_events'].copy()
            df.columns = df.columns.str.strip()
            df['% DB time'] = pd.to_numeric(df['% DB time'], errors='coerce')
            df['Event'] = df['Event'].astype(str)
            tx_tm_events = df[
                df['Event'].str.lower().isin(['enq: tx - row lock contention', 'enq: tm - contention'])
                & (df['% DB time'] > 1)
            ]
            if not tx_tm_events.empty:
                listed = ', '.join(tx_tm_events['Event'])
                insights.append(f"🔥 [CRITICAL] Contention detected: {listed} — investigate blocking sessions or DML concurrency.")
        except:
            pass

    # Rule: Suboptimal PGA
    if not data['pga_advisory'].empty:
        try:
            pga_df = data['pga_advisory'].copy()
            pga_df.columns = pga_df.columns.str.strip()
            pga_df['Estd Time'] = pd.to_numeric(pga_df['Estd Time'].replace(',', '', regex=True), errors='coerce')
            current_row = pga_df.iloc[len(pga_df)//2]
            current_time = current_row['Estd Time']
            min_time = pga_df['Estd Time'].min()
            if current_time - min_time > 10:
                insights.append("⚠️ [WARNING] PGA Advisory suggests increasing PGA size could improve execution time.")
        except:
            pass

    # Rule: Idle CPU
    try:
        if data['idle_cpu'] is not None and isinstance(data['idle_cpu'], (int, float)):
            if data['idle_cpu'] < 10:
                insights.append(f"🔥 [CRITICAL] Very low idle CPU ({data['idle_cpu']:.1f}%) — High CPU pressure.")
            elif data['idle_cpu'] < 30:
                insights.append(f"⚠️ [WARNING] Idle CPU below optimal level ({data['idle_cpu']:.1f}%).")
            else:
                insights.append(f"✅ Idle CPU healthy at {data['idle_cpu']:.1f}%.")
    except:
        pass

    return insights



# Upload file - Main uploader
uploaded_files = st.file_uploader("📤 Upload AWR HTML reports", type="html", accept_multiple_files=True, key="main_uploader")

if not uploaded_files:
    st.info("Please upload one or more AWR HTML reports to parse.")
    st.stop()

# Build list of file names
file_names = [file.name for file in uploaded_files]


# Select report to view
selected_file = st.selectbox("Select an AWR report to view:", file_names, key="select_main_report")

# 🔁 Reset SQL Text expander state if new file selected
if "last_uploaded_file" not in st.session_state:
    st.session_state.last_uploaded_file = ""

if selected_file != st.session_state.last_uploaded_file:
    st.session_state.sql_expander_open = False
    st.session_state.last_uploaded_file = selected_file


# Find and parse selected file
data = None
for uploaded in uploaded_files:
    if uploaded.name == selected_file:
        html = uploaded.getvalue().decode('utf-8')
        try:
            data = parse_awr(html)
            insights = get_intelligent_insights(data)  # ✅ Add here
        except Exception as e:
            st.error(f"Error parsing AWR report: {e}")
            data = None
        break

if data:
    st.subheader(f"📄 Report: {selected_file}")
    st.write(f"**Database Name:** {data['db_name']}")
else:
    st.error("Failed to parse the selected AWR report. Please check the file.")
    st.stop()


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

# AWR Summary
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
    db_time=data['load_profile'][data['load_profile']['Metric'].str.contains("DB Time", na=False)]['Per Second'].values[0] if not data['load_profile'].empty else "N/A",
    idle=f"{data['idle_cpu']:.1f}" if data['idle_cpu'] is not None else "N/A",
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
st.markdown("### 📊 Load Profile (Per Second)")
if not data['load_profile'].empty:
    fig1 = px.bar(data['load_profile'], x='Metric', y='Per Second', title='Load Profile', color='Metric', text='Per Second')
    fig1.update_traces(texttemplate='%{text}', textposition='outside')
    fig1.update_layout(yaxis_title="", xaxis_title="", plot_bgcolor='#fff0f5')
    st.plotly_chart(fig1, use_container_width=True)
else:
    st.warning("Load Profile table not found.")

# Wait Events
st.markdown("### ⏳ Top 5 Wait Events (% DB time)")
if not data['wait_events'].empty:
    fig2 = px.bar(data['wait_events'], x='Event', y='% DB time', title='Wait Events', color='Event', text='% DB time')
    fig2.update_traces(texttemplate='%{text}', textposition='outside')
    fig2.update_layout(yaxis_title="", xaxis_title="", plot_bgcolor='#f0f8ff')
    st.plotly_chart(fig2, use_container_width=True)
else:
    st.warning("Wait Events table not found.")

# 🔥 Top SQL by Elapsed Time
st.markdown("### 🔥 Top SQL by Elapsed Time")

if not data['top_sql'].empty:
    df_top_sql = data['top_sql'].copy()

    # Normalize column names
    df_top_sql.columns = df_top_sql.columns.str.strip().str.upper()

    # Required columns
    required_columns = ['SQL ID', 'SQL TEXT', 'ELAPSED TIME (S)']
    if all(col in df_top_sql.columns for col in required_columns):

        # Parse elapsed time
        df_top_sql['ELAPSED TIME (S)'] = pd.to_numeric(df_top_sql['ELAPSED TIME (S)'], errors='coerce')

        # Create a unique label for Y-axis
        df_top_sql['LABEL'] = df_top_sql['SQL ID'].astype(str) + " | " + df_top_sql['SQL TEXT'].astype(str).str.slice(0, 50) + '...'

        # Plot with improved dark red gradient
        fig_elapsed = px.bar(
            df_top_sql.sort_values('ELAPSED TIME (S)'),
            y='LABEL',
            x='ELAPSED TIME (S)',
            orientation='h',
            text='ELAPSED TIME (S)',
            title='Top SQL by Elapsed Time',
            color='ELAPSED TIME (S)',
            color_continuous_scale=["#800000", "#B22222", "#DC143C", "#FF6347"]
        )

        fig_elapsed.update_layout(
            yaxis_title="SQL ID | SQL Text",
            xaxis_title="Elapsed Time (s)",
            plot_bgcolor='#fffaf0',
            margin=dict(l=10, r=10, t=40, b=10),
            height=40 * len(df_top_sql),   # Ensure enough height per bar
            uniformtext_mode='show'        # Force labels to render
        )

        fig_elapsed.update_traces(
            texttemplate='%{text:.2f}',
            textposition='outside',
            hovertemplate="<b>Elapsed Time (s):</b> %{x}<br><b>SQL:</b> %{y}<extra></extra>"
        )

        st.plotly_chart(fig_elapsed, use_container_width=True)

    else:
        st.error("Required columns ('SQL ID', 'SQL TEXT', 'ELAPSED TIME (S)') not found in Top SQL by Elapsed Time data.")
else:
    st.warning("Top SQL by Elapsed Time not found.")




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

        # Label: SQL ID with truncated SQL text
        df_cpu_sql['LABEL'] = df_cpu_sql['SQL ID'].astype(str) + " | " + df_cpu_sql['SQL TEXT'].str.slice(0, 50) + '...'

        # Improved Bar Chart with Darker Blue Gradient
        fig_cpu = px.bar(
            df_cpu_sql.sort_values('CPU TIME (S)'),
            y='LABEL',
            x='CPU TIME (S)',
            orientation='h',
            text='CPU TIME (S)',
            title='Top SQL by CPU Time',
            color='CPU TIME (S)',
            color_continuous_scale=["#00008B", "#0000CD", "#4169E1", "#6495ED", "#87CEFA"]
        )

        fig_cpu.update_layout(
            yaxis_title="SQL ID | SQL Text",
            xaxis_title="CPU Time (s)",
            plot_bgcolor='#f8ffff',
            margin=dict(l=10, r=10, t=40, b=10),
            height=40 * len(df_cpu_sql),   # Ensure enough height per bar
            uniformtext_mode='show'        # Force labels to render
        )

        fig_cpu.update_traces(
            texttemplate='%{text:.2f}',
            textposition='outside',
            hovertemplate="<b>CPU Time (s):</b> %{x}<br><b>SQL:</b> %{y}<extra></extra>"
        )

        st.plotly_chart(fig_cpu, use_container_width=True)

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
        fig = px.bar(
            chart_df.sort_values(by='Physical Reads', ascending=False).head(10),
            x='Object Name',
            y='Physical Reads',
            text='Physical Reads',
            color_discrete_sequence=["#3498db"]
        )
        fig.update_traces(textposition='outside')
        fig.update_layout(title="Top 10 Segments by Physical Reads", xaxis_title="Object Name", yaxis_title="Physical Reads",
                          plot_bgcolor="#f8f9fa", paper_bgcolor="#ffffff", showlegend=False)
        fig.update_xaxes(tickangle=-45)
        st.plotly_chart(fig, use_container_width=True)
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
        fig = px.bar(
            chart_df.sort_values(by='Row Lock Waits', ascending=False).head(10),
            x='Object Name', y='Row Lock Waits', text='Row Lock Waits', color_discrete_sequence=["#3498db"]
        )
        fig.update_traces(textposition='outside')
        fig.update_layout(title="Top Segments by Row Lock Waits", xaxis_title="Object Name", yaxis_title="Row Lock Waits",
                          plot_bgcolor="#f8f9fa", paper_bgcolor="#ffffff", showlegend=False)
        fig.update_xaxes(tickangle=-45)
        st.plotly_chart(fig, use_container_width=True)
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
        fig = px.bar(
            chart_df.sort_values(by='Table Scans', ascending=False).head(10),
            x='Object Name', y='Table Scans', text='Table Scans', color_discrete_sequence=["#e67e22"]
        )
        fig.update_traces(textposition='outside')
        fig.update_layout(title="Top Segments by Table Scans", xaxis_title="Object Name", yaxis_title="Table Scans",
                          plot_bgcolor="#f8f9fa", paper_bgcolor="#ffffff", showlegend=False)
        fig.update_xaxes(tickangle=-45)
        st.plotly_chart(fig, use_container_width=True)
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
        fig = go.Figure(go.Pie(labels=df['PGA Target Est (MB)'], values=df['Estd Time'], text=df['Size Factr'],
                               textinfo='text', textposition='inside', marker=dict(line=dict(color='#000000', width=1)),
                               hovertemplate="<b>PGA Target Est (MB):</b> %{label}<br><b>Estd Time:</b> %{value}<extra></extra>"))
        fig.update_layout(title="PGA Advisory – Estd Time by PGA Target Est (MB)", showlegend=False,
                          template='plotly_dark', margin=dict(t=50, b=0, l=0, r=0))
        chart_col, info_col = st.columns([3, 1])
        with chart_col:
            st.plotly_chart(fig, use_container_width=True)
        with info_col:
            st.info("**How to Read**\n\n- **Each slice** = `PGA Target Est (MB)`\n- **Slice size** = `Estd Time`\n- **Inside label** = `Size Factr`\n- **Hover shows**: PGA Target Est (MB), Estd Time")
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
        for col in ['SGA Target Size (M)', 'SGA Size Factor', 'Est DB Time (s)', 'Est Physical Reads']:
            sga_df[col] = sga_df[col].replace(',', '', regex=True).astype(float)
        fig = px.pie(sga_df, values='Est DB Time (s)', names='SGA Target Size (M)',
                     hover_data=['SGA Target Size (M)', 'Est DB Time (s)', 'Est Physical Reads'])
        fig.update_traces(text=sga_df['SGA Size Factor'].astype(str), textposition='inside', textinfo='text',
                          hovertemplate=("SGA Target Size (M): %{label}<br>Est DB Time (s): %{value}<br>Est Physical Reads: %{customdata[0]}<extra></extra>"),
                          customdata=sga_df[['Est Physical Reads']], showlegend=False)
        fig.update_layout(title='Estimated DB Time vs SGA Target Size Advisory', uniformtext_minsize=12,
                          uniformtext_mode='hide', margin=dict(t=40, b=0, l=0, r=0))
        col1, col2 = st.columns([3, 1])
        with col1:
            st.plotly_chart(fig, use_container_width=True)
        with col2:
            st.info("**How to Read**\n\n- Each slice = SGA Target Size (M)\n- Slice size = Est DB Time (s)\n- Inside label = Size Factor\n- Hover shows:\n  • SGA Target Size (M)\n  • Est DB Time (s)\n  • Est Physical Reads")
    else:
        st.warning("SGA Target Advisory section not found or empty.")

# Top SQL with Top Events
st.markdown('<div id="top-sql-with-top-events"></div>', unsafe_allow_html=True)
st.write("")
with st.expander("📝 Top SQL with Top Events", expanded=False):
    if not data['top_sql_events'].empty:
        chart_df = data['top_sql_events'].copy()
        chart_df.rename(columns={
            'SQL ID': 'sql_id', 'Plan Hash': 'plan_hash', 'Executions': 'executions',
            'Event': 'event', 'Top Row Source': 'top_row_source', 'SQL Text': 'sql_text'
        }, inplace=True)
        chart_df = chart_df[['sql_id', 'plan_hash', 'executions', 'event', 'top_row_source', 'sql_text']]
        chart_df = chart_df.fillna('').astype(str).apply(lambda x: x.str.strip())
        chart_df = chart_df[(chart_df['sql_id'] != '') & (chart_df['plan_hash'] != '') &
                            (chart_df['executions'] != '') & (chart_df['sql_text'] != '')]
        chart_df['executions'] = pd.to_numeric(chart_df['executions'], errors='coerce').fillna(0).astype(int)
        chart_df['plan_hash'] = pd.to_numeric(chart_df['plan_hash'], errors='coerce').fillna(0).astype(int)
        chart_df.sort_values(by='executions', ascending=False, inplace=True)
        chart_df = chart_df.head(6).reset_index(drop=True)
        if not chart_df.empty:
            fig = px.bar(chart_df, x='executions', y='sql_id', orientation='h', text='executions',
                         title='Top SQL with Top Events - Executions', color='event',
                         hover_data={'sql_id': False, 'top_row_source': True, 'executions': True, 'event': True})
            fig.update_layout(yaxis_title='SQL ID', xaxis_title='Executions', height=400)
            st.plotly_chart(fig, use_container_width=True)
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

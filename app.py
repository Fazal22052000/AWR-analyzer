import streamlit as st
import pandas as pd
import plotly.express as px
from bs4 import BeautifulSoup
import base64
import plotly.graph_objects as go
import plotly.express as px
from io import BytesIO


st.set_page_config(page_title="AWR Analyzer", layout="wide")

# Dark mode toggle
dark_mode = st.sidebar.checkbox("🌙 Dark Mode", value=False)

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
    top_sql_events = pd.DataFrame() 
    activity_over_time = pd.DataFrame()

    def to_df(table):
        try:
            return pd.read_html(str(table))[0]
        except:
            return None

    # Default values
    db_name = "N/A"
    snap_time = "N/A"
    cdb_status = "N/A"
    memory_gb = "N/A"  # Initialize at the beginning of parse_awr
    platform = "N/A"
    instance_name = "N/A"
    instance_num = "N/A"
    startup_time = "N/A"
    begin_snap_time = "N/A"
    end_snap_time = "N/A"
    total_cpu = rac_status = edition = release = "N/A"
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

            try:
                cols = [str(c).lower() for c in df.columns]
                if all(col in cols for col in ['sql id', 'sql text', 'elapsed time (s)']):
                    top_sql = df[[c for c in df.columns if str(c).lower() in ['sql id', 'sql text', 'elapsed time (s)']]].head(5)

                if all(col in cols for col in ['sql id', 'sql text', 'cpu time (s)']):
                    top_cpu_sql = df[[c for c in df.columns if str(c).lower() in ['sql id', 'sql text', 'cpu time (s)']]].head(5)
            except:
                pass
    
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

    for tag in soup.find_all(["b", "font", "p", "div", "h2", "h3"]):
        if "Activity Over Time" in tag.text:
            next_table = tag.find_next("table")
            if next_table:
                df = to_df(next_table)
                if df is not None and not df.empty:
                    activity_over_time = df
            break

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
        'total_cpu': total_cpu,
        'rac_status': rac_status,
        'edition': f"{edition} {release}" if release != "N/A" else edition,
        
        # ✅ Newly added advisory outputs
        'pga_advisory': pga_advisory_df,
        'sga_advisory': sga_advisory_df,
        'top_sql_events': top_sql_events,
        'activity_over_time': activity_over_time
    }


# Upload file
# Multi-file uploader
uploaded_files = st.file_uploader("📤 Upload AWR HTML reports", type="html", accept_multiple_files=True)

if not uploaded_files:
    st.info("Please upload one or more AWR HTML reports to parse.")
    st.stop()

# Build a list of file names
file_names = [file.name for file in uploaded_files]

# Let user pick which report to display
selected_file = st.selectbox("Select an AWR report to view:", file_names)

# Find the selected file
for uploaded in uploaded_files:
    if uploaded.name == selected_file:
        html = uploaded.read().decode('utf-8')
        data = parse_awr(html)  # Assumes parse_awr() is defined
        break

# Show the selected report's data
st.subheader(f"📄 Report: {selected_file}")
st.write(f"**Database Name:** {data['db_name']}")





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

# 🔥 Top 5 SQL by Elapsed Time
st.markdown("### 🔥 Top 5 SQL by Elapsed Time")

if not data['top_sql'].empty:
    df_top_sql = data['top_sql'].copy()

    # Normalize column names
    df_top_sql.columns = df_top_sql.columns.str.strip().str.upper()

    # Required columns
    required_columns = ['SQL ID', 'SQL TEXT', 'ELAPSED TIME (S)']
    if all(col in df_top_sql.columns for col in required_columns):
        # Parse elapsed time
        df_top_sql['ELAPSED TIME (S)'] = pd.to_numeric(df_top_sql['ELAPSED TIME (S)'], errors='coerce')

        # Create label as SQL_ID | truncated SQL_TEXT
        df_top_sql['SQL_ID'] = df_top_sql['SQL ID'].astype(str) + " | " + df_top_sql['SQL TEXT'].astype(str).str.slice(0, 50) + '...'

        # Plot
        fig_elapsed = px.bar(
            df_top_sql.sort_values('ELAPSED TIME (S)'),
            y='SQL_ID',
            x='ELAPSED TIME (S)',
            orientation='h',
            text='ELAPSED TIME (S)',
            title='Top 5 SQL by Elapsed Time',
            color='ELAPSED TIME (S)',
            color_continuous_scale='reds'
        )
        fig_elapsed.update_layout(
            yaxis_title="SQL ID | SQL Text",
            xaxis_title="Elapsed Time (s)",
            plot_bgcolor='#fffaf0',
            margin=dict(l=10, r=10, t=40, b=10)
        )
        fig_elapsed.update_traces(texttemplate='%{text:.2f}', textposition='outside')
        st.plotly_chart(fig_elapsed, use_container_width=True)
    else:
        st.error("Required columns ('SQL ID', 'SQL TEXT', 'ELAPSED TIME (S)') not found in Top SQL by Elapsed Time data.")
else:
    st.warning("Top SQL by Elapsed Time not found.")


# Top SQL by CPU Time (Graphical)
# Top SQL by CPU Time (Graphical)
st.markdown("### ⚡ Top 5 SQL by CPU Time")

if not data['top_cpu_sql'].empty:
    df_cpu_sql = data['top_cpu_sql'].copy()

    # Normalize column names
    df_cpu_sql.columns = df_cpu_sql.columns.str.strip().str.upper()

    # Required columns
    required_columns = ['SQL ID', 'SQL TEXT', 'CPU TIME (S)']
    if all(col in df_cpu_sql.columns for col in required_columns):
        df_cpu_sql['CPU TIME (S)'] = pd.to_numeric(df_cpu_sql['CPU TIME (S)'], errors='coerce')
        df_cpu_sql['SQL_ID'] = df_cpu_sql['SQL ID'].astype(str) + " | " + df_cpu_sql['SQL TEXT'].str.slice(0, 50) + '...'

        fig_cpu = px.bar(
            df_cpu_sql,
            y='SQL_ID',
            x='CPU TIME (S)',
            orientation='h',
            text='CPU TIME (S)',
            title='Top 5 SQL by CPU Time',
            color='CPU TIME (S)',
            color_continuous_scale='blues'
        )
        fig_cpu.update_layout(
            yaxis_title="SQL ID | SQL Text",
            xaxis_title="CPU Time (s)",
            plot_bgcolor='#f8ffff',
            margin=dict(l=10, r=10, t=40, b=10)
        )
        fig_cpu.update_traces(texttemplate='%{text:.2f}', textposition='outside')
        st.plotly_chart(fig_cpu, use_container_width=True)
    else:
        st.error("Required columns ('SQL ID', 'SQL TEXT', 'CPU TIME (S)') not found in Top SQL by CPU Time data.")
else:
    st.warning("Top SQL by CPU Time not found.")

# Initialization Parameters
with st.expander("⚙️ Initialization Parameters"):
    if not data.get('init_params', pd.DataFrame()).empty:
        st.dataframe(data['init_params'], use_container_width=True, height=300)
    else:
        st.warning("Initialization Parameters section not found.")



with st.expander("📊 Segments by Physical Reads"):
    if not data['seg_physical_reads'].empty:

        chart_df = data['seg_physical_reads'].copy()

        # Clean and convert 'Physical Reads' column
        chart_df['Physical Reads'] = chart_df['Physical Reads'].replace(',', '', regex=True)
        chart_df['Physical Reads'] = pd.to_numeric(chart_df['Physical Reads'], errors='coerce')

        fig = px.bar(
            chart_df.sort_values(by='Physical Reads', ascending=False).head(10),
            x='Object Name',               # Categories on X-axis
            y='Physical Reads',            # Numeric values on Y-axis
            text='Physical Reads',
            color_discrete_sequence=["#3498db"]  # Blue bars, can change color
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

        fig.update_xaxes(tickangle=-45)  # Rotate x-axis labels for readability

        st.plotly_chart(fig, use_container_width=True)

    else:
        st.warning("Segments by Physical Reads section not found.")


with st.expander("📊 Segments by Row Lock Waits"):
    if not data['seg_row_lock_waits'].empty:

        chart_df = data['seg_row_lock_waits'].copy()

        # Clean and convert 'Row Lock Waits' column to numeric
        chart_df['Row Lock Waits'] = chart_df['Row Lock Waits'].replace(',', '', regex=True)
        chart_df['Row Lock Waits'] = pd.to_numeric(chart_df['Row Lock Waits'], errors='coerce')

        fig = px.bar(
            chart_df.sort_values(by='Row Lock Waits', ascending=False).head(10),  # Top 10 segments
            x='Object Name',
            y='Row Lock Waits',
            text='Row Lock Waits',
            color_discrete_sequence=["#3498db"]  # Blue bars, change if desired
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

        fig.update_xaxes(tickangle=-45)  # Rotate x-axis labels for readability

        st.plotly_chart(fig, use_container_width=True)

    else:
        st.warning("Segments by Row Lock Waits section not found.")








with st.expander("📈 Advisory Statistics – PGA Advisory", expanded=False):
    if data.get('pga_advisory') is not None and not data['pga_advisory'].empty:
        df = data['pga_advisory'][[
            'PGA Target Est (MB)',
            'Size Factr',
            'Estd Time'
        ]].copy()

        # Clean & convert
        df['PGA Target Est (MB)'] = (
            df['PGA Target Est (MB)']
            .replace(',', '', regex=True)
            .astype(float)
        )
        df['Size Factr'] = df['Size Factr'].astype(str)
        df['Estd Time'] = (
            df['Estd Time']
            .replace(',', '', regex=True)
            .astype(float)
        )

        # Build the pie chart
        fig = go.Figure(go.Pie(
            labels=df['PGA Target Est (MB)'],
            values=df['Estd Time'],
            text=df['Size Factr'],             # show only Size Factor inside
            textinfo='text',
            textposition='inside',
            marker=dict(line=dict(color='#000000', width=1)),
            hovertemplate=(
                "<b>PGA Target Est (MB):</b> %{label}<br>"
                "<b>Estd Time:</b> %{value}<extra></extra>"
            )
        ))

        fig.update_layout(
            title="PGA Advisory – Estd Time by PGA Target Est (MB)",
            showlegend=False,
            template='plotly_dark',
            margin=dict(t=50, b=0, l=0, r=0)
        )

        # Two‑column layout
        chart_col, info_col = st.columns([3, 1])

        with chart_col:
            st.plotly_chart(fig, use_container_width=True)

        with info_col:
            st.info(
                "**How to Read**\n\n"
                "- **Each slice** = `PGA Target Est (MB)`\n"
                "- **Slice size** = `Estd Time`\n"
                "- **Inside label** = `Size Factr`\n"
                "- **Hover shows**: PGA Target Est (MB), Estd Time"
            )

    else:
        st.info("PGA Memory Advisory data not found.")



# === SGA Target Advisory Section ===

with st.expander("▶️ SGA Target Advisory", expanded=False):
    st.markdown("#### SGA Target Advisory")

    if data.get('sga_advisory') is not None and not data['sga_advisory'].empty:
        sga_df = data['sga_advisory'].copy()
        # Clean column names & types
        sga_df.columns = [str(c).strip() for c in sga_df.columns]
        for col in ['SGA Target Size (M)', 'SGA Size Factor', 'Est DB Time (s)', 'Est Physical Reads']:
            sga_df[col] = (
                sga_df[col]
                .replace(',', '', regex=True)
                .astype(float)
            )

        # Build the pie chart
        fig = px.pie(
            sga_df,
            values='Est DB Time (s)',
            names='SGA Target Size (M)',
            hover_data=['SGA Target Size (M)', 'Est DB Time (s)', 'Est Physical Reads'],
        )
        fig.update_traces(
            text=sga_df['SGA Size Factor'].astype(str),
            textposition='inside',
            textinfo='text',
            hovertemplate=(
                "SGA Target Size (M): %{label}<br>"
                "Est DB Time (s): %{value}<br>"
                "Est Physical Reads: %{customdata[0]}<extra></extra>"
            ),
            customdata=sga_df[['Est Physical Reads']],
            showlegend=False
        )
        fig.update_layout(
            title='Estimated DB Time vs SGA Target Size Advisory',
            uniformtext_minsize=12,
            uniformtext_mode='hide',
            margin=dict(t=40, b=0, l=0, r=0)
        )

        # Two‑column layout: chart on left, instructions on right
        col1, col2 = st.columns([3, 1])
        with col1:
            st.plotly_chart(fig, use_container_width=True)
        with col2:
            st.info(
                "**How to Read**\n\n"
                "- Each slice = SGA Target Size (M)\n"
                "- Slice size = Est DB Time (s)\n"
                "- Inside label = Size Factor\n"
                "- Hover shows:\n"
                "  • SGA Target Size (M)\n"
                "  • Est DB Time (s)\n"
                "  • Est Physical Reads"
            )

    else:
        st.warning("SGA Target Advisory section not found or empty.")


with st.expander("📝 Top SQL with Top Events"):
    if not data['top_sql_events'].empty:

        chart_df = data['top_sql_events'].copy()

        # Rename columns to standard format
        chart_df.rename(columns={
            'SQL ID': 'sql_id',
            'Plan Hash': 'plan_hash',
            'Executions': 'executions',
            'Event': 'event',
            'Top Row Source': 'top_row_source',
            'SQL Text': 'sql_text'
        }, inplace=True)

        # Keep only required columns
        chart_df = chart_df[['sql_id', 'plan_hash', 'executions', 'event', 'top_row_source', 'sql_text']]

        # Replace NaNs with empty strings and strip whitespace
        chart_df = chart_df.fillna('').astype(str).apply(lambda x: x.str.strip())

        # Remove blank rows based on key columns
        chart_df = chart_df[
            (chart_df['sql_id'] != '') &
            (chart_df['plan_hash'] != '') &
            (chart_df['executions'] != '') &
            (chart_df['sql_text'] != '')
        ]

        # Convert numeric columns safely
        chart_df['executions'] = pd.to_numeric(chart_df['executions'], errors='coerce').fillna(0).astype(int)
        chart_df['plan_hash'] = pd.to_numeric(chart_df['plan_hash'], errors='coerce').fillna(0).astype(int)

        # Sort by executions
        chart_df.sort_values(by='executions', ascending=False, inplace=True)

        # Limit to top 6 rows
        chart_df = chart_df.head(6)

        # Reset index
        chart_df.reset_index(drop=True, inplace=True)

        if not chart_df.empty:
            # Horizontal Bar Chart with Custom Hover
            fig = px.bar(
                chart_df,
                x='executions',
                y='sql_id',
                orientation='h',
                text='executions',
                title='Top SQL with Top Events - Executions',
                color='event',
                hover_data={'sql_id': False, 'top_row_source': True, 'executions': True, 'event': True}
            )
            fig.update_layout(
                yaxis_title='SQL ID',
                xaxis_title='Executions',
                height=400
            )
            st.plotly_chart(fig, use_container_width=True)
            
        else:
            st.info("No valid rows found after filtering.")

    else:
        st.warning("Top SQL with Top Events section not found.")


with st.expander("📊 Activity Over Time"):
    if not data['activity_over_time'].empty:

        chart_df = data['activity_over_time'].copy()

        # Keep only the required columns and rename for consistency
        chart_df.rename(columns={
            'Slot Time (Duration)': 'slot_time',
            'Event': 'event',
            'Event Count': 'event_count',
            '% Event': 'percent_event'
        }, inplace=True)

        chart_df = chart_df[['slot_time', 'event', 'event_count', 'percent_event']]

        # Clean and convert columns
        chart_df = chart_df.fillna('').astype(str).apply(lambda x: x.str.strip())
        chart_df['event_count'] = pd.to_numeric(chart_df['event_count'], errors='coerce').fillna(0).astype(int)
        chart_df['percent_event'] = pd.to_numeric(chart_df['percent_event'], errors='coerce').fillna(0)

        # Filter out invalid event counts
        chart_df = chart_df[chart_df['event_count'] > 0]

        if not chart_df.empty:
            # Bar chart: Event Count by Slot Time and Event
            fig = px.bar(
                chart_df,
                x='slot_time',
                y='event_count',
                color='event',
                text='event_count',
                title='Activity Over Time - Event Count by Slot Time and Event',
            )
            fig.update_layout(
                xaxis_title='Slot Time (Duration)',
                yaxis_title='Event Count',
                height=450,
                barmode='stack'  # Options: 'stack', 'group', 'overlay', 'relative'
            )
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
# All sections
report_dict = {
    # ✅ Environment Info comes first
    'Environment_Info': pd.DataFrame({
        'Database Name':      [data['db_name']],
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
    'Top_SQL_Events': data['top_sql_events'],
    'Activity_Over_Time': data['activity_over_time']
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
pdf_content += df_to_text(data['top_sql'], "🔥 Top 5 SQL by Elapsed Time")
pdf_content += df_to_text(data['top_cpu_sql'], "⚡ Top 5 SQL by CPU Time")
pdf_content += df_to_text(data['init_params'], "⚙️ Initialization Parameters")
pdf_content += df_to_text(data['pga_advisory'], "🧠 PGA Memory Advisory")
pdf_content += df_to_text(data['sga_advisory'], "💡 SGA Target Advisory")
pdf_content += df_to_text(data['seg_physical_reads'], "🥧 Segments by Physical Reads")
pdf_content += df_to_text(data['seg_row_lock_waits'], "🔒 Segments by Row Lock Waits")  # ✅ Added Row Lock Waits
pdf_content += df_to_text(data['top_sql_events'], "📝 Top SQL with Top Events")  # ✅ Top SQL Events Added
pdf_content += df_to_text(data['activity_over_time'], "📊 Activity Over Time Breakdown")  # ✅ New Activity Over Time Section Added








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

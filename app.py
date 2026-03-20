"""
app.py — MetroPT-3 Railway APU Fault Detection Dashboard
Enhanced Edition: Industrial Ops-Center Design
Run: streamlit run app.py
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
from pymongo import MongoClient, DESCENDING
from datetime import datetime, timedelta
import time

# ── Page config ────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="MetroPT-3 · APU Monitor",
    page_icon="🚆",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Design System & CSS ────────────────────────────────────────────────────────
st.markdown("""
<link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@300;400;500&family=Barlow+Condensed:wght@300;400;500;600;700&family=Barlow:wght@300;400;500&display=swap" rel="stylesheet">

<style>
/* ── Root variables ── */
:root {
    --bg-void:      #06080d;
    --bg-deep:      #090c14;
    --bg-panel:     #0c1018;
    --bg-card:      #0f1520;
    --bg-card-alt:  #111826;
    --border-dim:   #1a2540;
    --border-mid:   #243050;
    --border-glow:  #2a4070;
    --amber:        #e8a020;
    --amber-bright: #ffb733;
    --amber-dim:    #8a5c10;
    --green:        #22c55e;
    --green-dim:    #166534;
    --red:          #ef4444;
    --red-dim:      #7f1d1d;
    --cyan:         #22d3ee;
    --cyan-dim:     #164e63;
    --blue:         #3b82f6;
    --text-primary: #d4d8e2;
    --text-muted:   #6b7a99;
    --text-dim:     #3d4a66;
    --font-mono:    'JetBrains Mono', monospace;
    --font-cond:    'Barlow Condensed', sans-serif;
    --font-body:    'Barlow', sans-serif;
}

/* ── Global resets ── */
html, body, [data-testid="stAppViewContainer"] {
    background: var(--bg-void) !important;
    color: var(--text-primary);
    font-family: var(--font-body);
}

/* Scanline overlay */
[data-testid="stAppViewContainer"]::before {
    content: '';
    position: fixed;
    top: 0; left: 0; right: 0; bottom: 0;
    background: repeating-linear-gradient(
        0deg,
        transparent,
        transparent 2px,
        rgba(0,0,0,0.08) 2px,
        rgba(0,0,0,0.08) 4px
    );
    pointer-events: none;
    z-index: 9999;
}

/* ── Sidebar ── */
[data-testid="stSidebar"] {
    background: var(--bg-deep) !important;
    border-right: 1px solid var(--border-dim) !important;
}
[data-testid="stSidebar"] * { color: var(--text-primary) !important; }
[data-testid="stSidebar"] .stSelectbox label,
[data-testid="stSidebar"] .stCheckbox label,
[data-testid="stSidebar"] .stSlider label {
    font-family: var(--font-cond);
    font-size: 0.8rem;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    color: var(--text-muted) !important;
}

/* ── Hide default streamlit chrome ── */
#MainMenu, footer, header { visibility: hidden; }
[data-testid="stToolbar"] { display: none; }
.block-container { padding-top: 1rem !important; padding-bottom: 2rem !important; }

/* ── Top header bar ── */
.header-bar {
    display: flex;
    align-items: center;
    justify-content: space-between;
    background: var(--bg-panel);
    border: 1px solid var(--border-dim);
    border-top: 2px solid var(--amber);
    border-radius: 4px;
    padding: 12px 24px;
    margin-bottom: 1rem;
    position: relative;
    overflow: hidden;
}
.header-bar::before {
    content: '';
    position: absolute;
    top: 0; left: 0; right: 0;
    height: 1px;
    background: linear-gradient(90deg, transparent, var(--amber), transparent);
    opacity: 0.6;
}
.header-logo {
    display: flex;
    align-items: center;
    gap: 14px;
}
.header-icon {
    font-size: 1.6rem;
    filter: drop-shadow(0 0 8px var(--amber));
}
.header-title {
    font-family: var(--font-cond);
    font-size: 1.5rem;
    font-weight: 700;
    letter-spacing: 0.12em;
    text-transform: uppercase;
    color: var(--amber-bright);
    text-shadow: 0 0 20px rgba(232,160,32,0.4);
}
.header-subtitle {
    font-family: var(--font-mono);
    font-size: 0.65rem;
    color: var(--text-muted);
    letter-spacing: 0.06em;
    margin-top: 1px;
}
.header-meta {
    text-align: right;
}
.header-time {
    font-family: var(--font-mono);
    font-size: 1rem;
    color: var(--amber);
    letter-spacing: 0.05em;
}
.header-status {
    font-family: var(--font-mono);
    font-size: 0.65rem;
    color: var(--text-muted);
    margin-top: 2px;
}

/* ── Status indicator dot ── */
.dot-live {
    display: inline-block;
    width: 8px; height: 8px;
    background: var(--green);
    border-radius: 50%;
    box-shadow: 0 0 8px var(--green);
    animation: pulse-green 2s infinite;
    margin-right: 6px;
    vertical-align: middle;
}
.dot-alert {
    display: inline-block;
    width: 8px; height: 8px;
    background: var(--red);
    border-radius: 50%;
    box-shadow: 0 0 8px var(--red);
    animation: pulse-red 1s infinite;
    margin-right: 6px;
    vertical-align: middle;
}
@keyframes pulse-green {
    0%, 100% { opacity: 1; box-shadow: 0 0 8px var(--green); }
    50%       { opacity: 0.5; box-shadow: 0 0 16px var(--green); }
}
@keyframes pulse-red {
    0%, 100% { opacity: 1; box-shadow: 0 0 8px var(--red); }
    50%       { opacity: 0.4; box-shadow: 0 0 20px var(--red); }
}

/* ── KPI cards ── */
.kpi-grid {
    display: grid;
    grid-template-columns: repeat(5, 1fr);
    gap: 10px;
    margin-bottom: 1rem;
}
.kpi-card {
    background: var(--bg-card);
    border: 1px solid var(--border-dim);
    border-radius: 4px;
    padding: 16px 18px 14px;
    position: relative;
    overflow: hidden;
    transition: border-color 0.2s;
}
.kpi-card:hover { border-color: var(--border-glow); }
.kpi-card::after {
    content: '';
    position: absolute;
    bottom: 0; left: 0; right: 0;
    height: 2px;
}
.kpi-card.amber::after  { background: var(--amber); }
.kpi-card.green::after  { background: var(--green); }
.kpi-card.red::after    { background: var(--red); }
.kpi-card.cyan::after   { background: var(--cyan); }
.kpi-card.blue::after   { background: var(--blue); }
.kpi-label {
    font-family: var(--font-cond);
    font-size: 0.68rem;
    font-weight: 500;
    letter-spacing: 0.12em;
    text-transform: uppercase;
    color: var(--text-muted);
    margin-bottom: 6px;
}
.kpi-value {
    font-family: var(--font-mono);
    font-size: 1.7rem;
    line-height: 1;
    font-weight: 400;
    margin-bottom: 4px;
}
.kpi-card.amber .kpi-value { color: var(--amber-bright); text-shadow: 0 0 16px rgba(232,160,32,0.35); }
.kpi-card.green .kpi-value { color: var(--green); text-shadow: 0 0 16px rgba(34,197,94,0.3); }
.kpi-card.red   .kpi-value { color: var(--red);   text-shadow: 0 0 16px rgba(239,68,68,0.3); }
.kpi-card.cyan  .kpi-value { color: var(--cyan);  text-shadow: 0 0 16px rgba(34,211,238,0.3); }
.kpi-card.blue  .kpi-value { color: var(--blue);  text-shadow: 0 0 16px rgba(59,130,246,0.3); }
.kpi-sub {
    font-family: var(--font-mono);
    font-size: 0.65rem;
    color: var(--text-muted);
}
.kpi-trend-up   { color: var(--red) !important; }
.kpi-trend-down { color: var(--green) !important; }
.kpi-icon {
    position: absolute;
    top: 14px; right: 16px;
    font-size: 1.3rem;
    opacity: 0.25;
}

/* ── Section headers ── */
.section-header {
    display: flex;
    align-items: center;
    gap: 10px;
    margin: 1.2rem 0 0.6rem;
    padding-bottom: 6px;
    border-bottom: 1px solid var(--border-dim);
}
.section-tag {
    font-family: var(--font-mono);
    font-size: 0.6rem;
    letter-spacing: 0.1em;
    color: var(--amber-dim);
    background: rgba(232,160,32,0.08);
    border: 1px solid var(--amber-dim);
    padding: 1px 6px;
    border-radius: 2px;
}
.section-title {
    font-family: var(--font-cond);
    font-size: 0.95rem;
    font-weight: 600;
    letter-spacing: 0.1em;
    text-transform: uppercase;
    color: var(--text-primary);
}

/* ── Chart container ── */
.chart-panel {
    background: var(--bg-card);
    border: 1px solid var(--border-dim);
    border-radius: 4px;
    padding: 16px;
    margin-bottom: 10px;
}
.chart-title {
    font-family: var(--font-cond);
    font-size: 0.75rem;
    font-weight: 500;
    letter-spacing: 0.1em;
    text-transform: uppercase;
    color: var(--text-muted);
    margin-bottom: 8px;
}

/* ── Alert feed ── */
.alert-feed {
    background: var(--bg-card);
    border: 1px solid var(--border-dim);
    border-radius: 4px;
    overflow: hidden;
}
.alert-feed-header {
    background: rgba(239,68,68,0.08);
    border-bottom: 1px solid var(--border-dim);
    padding: 8px 14px;
    font-family: var(--font-cond);
    font-size: 0.72rem;
    letter-spacing: 0.1em;
    text-transform: uppercase;
    color: var(--red);
    display: flex;
    align-items: center;
    gap: 8px;
}
.alert-row {
    display: flex;
    align-items: center;
    gap: 12px;
    padding: 9px 14px;
    border-bottom: 1px solid rgba(26,37,64,0.6);
    font-family: var(--font-mono);
    font-size: 0.72rem;
    transition: background 0.15s;
}
.alert-row:hover { background: rgba(239,68,68,0.05); }
.alert-row:last-child { border-bottom: none; }
.alert-ts   { color: var(--text-muted); min-width: 140px; }
.alert-score {
    background: rgba(239,68,68,0.15);
    border: 1px solid rgba(239,68,68,0.3);
    color: var(--red);
    padding: 1px 7px;
    border-radius: 2px;
    min-width: 60px;
    text-align: center;
}
.alert-hint {
    color: var(--amber);
    font-size: 0.68rem;
    letter-spacing: 0.06em;
}
.no-alert {
    padding: 16px 14px;
    font-family: var(--font-mono);
    font-size: 0.72rem;
    color: var(--green);
}

/* ── Sensor gauge row ── */
.gauge-row {
    display: grid;
    grid-template-columns: repeat(4, 1fr);
    gap: 10px;
    margin-bottom: 10px;
}
.gauge-card {
    background: var(--bg-card);
    border: 1px solid var(--border-dim);
    border-radius: 4px;
    padding: 12px 14px;
    text-align: center;
}
.gauge-label {
    font-family: var(--font-cond);
    font-size: 0.65rem;
    letter-spacing: 0.1em;
    text-transform: uppercase;
    color: var(--text-muted);
    margin-bottom: 4px;
}
.gauge-val {
    font-family: var(--font-mono);
    font-size: 1.4rem;
    color: var(--cyan);
    text-shadow: 0 0 12px rgba(34,211,238,0.3);
}
.gauge-unit {
    font-family: var(--font-mono);
    font-size: 0.6rem;
    color: var(--text-dim);
    margin-top: 1px;
}

/* ── Fault badge ── */
.fault-badge {
    display: inline-block;
    font-family: var(--font-mono);
    font-size: 0.62rem;
    letter-spacing: 0.05em;
    padding: 2px 8px;
    border-radius: 2px;
    margin: 2px;
}
.fault-LOW  { background: rgba(59,130,246,0.15); border: 1px solid rgba(59,130,246,0.4); color: var(--blue); }
.fault-HIGH { background: rgba(232,160,32,0.15); border: 1px solid rgba(232,160,32,0.4); color: var(--amber); }
.fault-OVER { background: rgba(239,68,68,0.15);  border: 1px solid rgba(239,68,68,0.4);  color: var(--red); }
.fault-VALV { background: rgba(34,211,238,0.15); border: 1px solid rgba(34,211,238,0.4); color: var(--cyan); }
.fault-GEN  { background: rgba(107,122,153,0.15);border: 1px solid rgba(107,122,153,0.4);color: var(--text-muted); }

/* ── Sidebar branding ── */
.sidebar-brand {
    font-family: var(--font-cond);
    font-size: 1rem;
    font-weight: 700;
    letter-spacing: 0.15em;
    text-transform: uppercase;
    color: var(--amber) !important;
    text-shadow: 0 0 12px rgba(232,160,32,0.3);
    padding: 4px 0 12px;
    border-bottom: 1px solid var(--border-dim);
    margin-bottom: 12px;
}
.sidebar-section {
    font-family: var(--font-mono);
    font-size: 0.6rem;
    letter-spacing: 0.1em;
    text-transform: uppercase;
    color: var(--amber-dim) !important;
    margin: 12px 0 6px;
}

/* ── Metrics override ── */
[data-testid="metric-container"] {
    background: transparent !important;
    border: none !important;
}
div[data-testid="stMetric"] {
    background: transparent !important;
}

/* ── Audit log table ── */
.audit-table {
    width: 100%;
    border-collapse: collapse;
    font-family: var(--font-mono);
    font-size: 0.7rem;
}
.audit-table th {
    background: rgba(26,37,64,0.8);
    color: var(--text-muted);
    letter-spacing: 0.08em;
    text-transform: uppercase;
    font-family: var(--font-cond);
    font-size: 0.65rem;
    padding: 7px 12px;
    text-align: left;
    border-bottom: 1px solid var(--border-dim);
}
.audit-table td {
    padding: 7px 12px;
    border-bottom: 1px solid rgba(26,37,64,0.5);
    color: var(--text-primary);
}
.audit-table tr:hover td { background: rgba(255,255,255,0.02); }

/* ── Progress bar ── */
.prog-bar-wrap {
    background: var(--bg-void);
    border: 1px solid var(--border-dim);
    border-radius: 2px;
    height: 6px;
    overflow: hidden;
    margin-top: 4px;
}
.prog-bar-fill {
    height: 100%;
    border-radius: 2px;
    transition: width 0.5s;
}

/* ── Scrollbar styling ── */
::-webkit-scrollbar { width: 6px; height: 6px; }
::-webkit-scrollbar-track { background: var(--bg-void); }
::-webkit-scrollbar-thumb { background: var(--border-mid); border-radius: 3px; }
::-webkit-scrollbar-thumb:hover { background: var(--border-glow); }

/* ── Streamlit element overrides ── */
.stSelectbox > div > div {
    background: var(--bg-card) !important;
    border-color: var(--border-dim) !important;
    color: var(--text-primary) !important;
    font-family: var(--font-mono) !important;
    font-size: 0.8rem !important;
}
.stSlider [data-baseweb="slider"] { padding: 0; }

/* ── Dividers ── */
hr { border-color: var(--border-dim) !important; margin: 0.5rem 0 !important; }

/* ── Plotly chart backgrounds ── */
.js-plotly-plot { border-radius: 2px; }

/* ── Fade-in animation ── */
@keyframes fadeUp {
    from { opacity: 0; transform: translateY(8px); }
    to   { opacity: 1; transform: translateY(0); }
}
.kpi-card   { animation: fadeUp 0.4s ease both; }
.kpi-card:nth-child(1) { animation-delay: 0.05s; }
.kpi-card:nth-child(2) { animation-delay: 0.10s; }
.kpi-card:nth-child(3) { animation-delay: 0.15s; }
.kpi-card:nth-child(4) { animation-delay: 0.20s; }
.kpi-card:nth-child(5) { animation-delay: 0.25s; }
</style>
""", unsafe_allow_html=True)

# ── Plotly theme helper ─────────────────────────────────────────────────────────
PLOT_BG   = "#0c1018"
PAPER_BG  = "#0c1018"
GRID_COL  = "#1a2540"
TEXT_COL  = "#6b7a99"
FONT_MONO = "JetBrains Mono"

def base_layout(height=300, margin=None):
    m = margin or dict(l=44, r=16, t=10, b=36)
    return dict(
        paper_bgcolor=PAPER_BG,
        plot_bgcolor=PLOT_BG,
        font=dict(color=TEXT_COL, family=FONT_MONO, size=10),
        height=height,
        margin=m,
        xaxis=dict(gridcolor=GRID_COL, zerolinecolor=GRID_COL, tickfont=dict(size=9)),
        yaxis=dict(gridcolor=GRID_COL, zerolinecolor=GRID_COL, tickfont=dict(size=9)),
        legend=dict(bgcolor="rgba(0,0,0,0)", font=dict(size=9)),
    )

# ── MongoDB connection ─────────────────────────────────────────────────────────
@st.cache_resource
def get_mongo():
    client = MongoClient("mongodb://localhost:27017", serverSelectionTimeoutMS=3000)
    return client["railway_db"]

def load_predictions(db, hours=6):
    since = (datetime.utcnow() - timedelta(hours=hours)).isoformat()
    cursor = db["processed_predictions"].find(
        {"source_timestamp": {"$gte": since}},
        {"_id": 0}
    ).sort("source_timestamp", DESCENDING).limit(8000)
    df = pd.DataFrame(list(cursor))
    if not df.empty:
        ts_col = "source_timestamp" if "source_timestamp" in df.columns else "timestamp"
        if ts_col in df.columns:
            df["timestamp"] = pd.to_datetime(df[ts_col])
            df = df.sort_values("timestamp")
    return df

def load_sensor_data(db, hours=6):
    since = (datetime.utcnow() - timedelta(hours=hours)).isoformat()
    cursor = db["raw_sensor_data"].find(
        {"timestamp": {"$gte": since}},
        {"_id": 0}
    ).sort("timestamp", DESCENDING).limit(8000)
    df = pd.DataFrame(list(cursor))
    if not df.empty and "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df = df.sort_values("timestamp")
    return df

def load_spark_log(db):
    cursor = db["spark_run_log"].find({}, {"_id": 0}).sort("_id", DESCENDING).limit(20)
    return pd.DataFrame(list(cursor))

# ── Demo data ─────────────────────────────────────────────────────────────────
def generate_demo_data(n=3000):
    np.random.seed(42)
    timestamps = pd.date_range(end=datetime.utcnow(), periods=n, freq="10s")

    tp2      = np.random.normal(10.5, 0.3, n)
    tp3      = np.random.normal(8.8,  0.2, n)
    h1       = np.random.normal(9.2,  0.3, n)
    dv_p     = np.random.normal(1.2,  0.1, n)
    reserv   = np.random.normal(9.0,  0.4, n)
    oil_temp = np.random.normal(65.0, 4.0, n)
    motor    = np.random.normal(18.0, 1.5, n)

    # Inject 4 anomaly windows of different fault types
    anomaly_windows = [
        (300, 325,  "LOW_COMPRESSOR_PRESSURE",  -3.5,  0,    0),
        (800, 835,  "HIGH_MOTOR_CURRENT",         0,    9.0,  0),
        (1400,1430, "OVERHEATING",                0,    0,   28.0),
        (2100,2130, "VALVE_PRESSURE_DROP",         0,    0,    0),
    ]
    fault_labels = ["NORMAL"] * n
    for start, end, label, dtp, dmotor, doil in anomaly_windows:
        tp2[start:end]      += dtp
        motor[start:end]    += dmotor
        oil_temp[start:end] += doil
        dv_p[start:end]     -= (0.9 if label == "VALVE_PRESSURE_DROP" else 0)
        for i in range(start, end):
            fault_labels[i] = label

    sensor_df = pd.DataFrame({
        "timestamp": timestamps,
        "TP2": tp2, "TP3": tp3, "H1": h1,
        "DV_pressure": dv_p, "Reservoirs": reserv,
        "Oil_temperature": oil_temp, "Motor_current": motor,
    })

    scores = np.abs(np.random.normal(2.2, 0.7, n))
    for start, end, *_ in anomaly_windows:
        scores[start:end] = np.random.normal(10.5, 0.8, end - start)

    pred_df = pd.DataFrame({
        "timestamp":     timestamps,
        "anomaly_score": scores,
        "is_anomaly":    scores > 6.0,
        "fault_hint":    fault_labels,
        "cluster_id":    np.random.randint(0, 8, n),
        "TP2":           tp2,
        "Motor_current": motor,
        "Oil_temperature": oil_temp,
        "DV_pressure":   dv_p,
        "rolling_mean_TP2":    pd.Series(tp2).rolling(180, min_periods=1).mean().values,
        "rolling_mean_motor":  pd.Series(motor).rolling(180, min_periods=1).mean().values,
    })

    spark_log = pd.DataFrame([{
        "run_id": "spark-20240315-001",
        "started_at": (datetime.utcnow() - timedelta(minutes=7)).strftime("%Y-%m-%d %H:%M:%S"),
        "completed_at": (datetime.utcnow() - timedelta(minutes=2)).strftime("%Y-%m-%d %H:%M:%S"),
        "records_processed": 3000,
        "anomalies_detected": int(scores.sum() > 6.0),
        "status": "SUCCESS",
    }])

    return sensor_df, pred_df, spark_log

# ── Sidebar ────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown('<div class="sidebar-brand">⬡ MetroPT-3</div>', unsafe_allow_html=True)
    st.markdown('<div class="sidebar-section">// Data Source</div>', unsafe_allow_html=True)
    use_demo = st.checkbox("Demo Mode", value=True, help="Uncheck when MongoDB is live")

    st.markdown('<div class="sidebar-section">// Time Window</div>', unsafe_allow_html=True)
    time_window = st.selectbox("", [1, 3, 6, 12, 24], index=2, format_func=lambda x: f"{x}h window")

    st.markdown('<div class="sidebar-section">// Anomaly Threshold</div>', unsafe_allow_html=True)
    threshold = st.slider("Score threshold", 3.0, 15.0, 6.0, 0.5, format="%.1f")

    st.markdown('<div class="sidebar-section">// Signal Channels</div>', unsafe_allow_html=True)
    show_tp2   = st.checkbox("TP2 · Compressor Pressure", True)
    show_tp3   = st.checkbox("TP3 · Panel Pressure", False)
    show_motor = st.checkbox("Motor Current", True)
    show_oil   = st.checkbox("Oil Temperature", True)
    show_dv    = st.checkbox("DV Pressure", False)

    st.markdown('<div class="sidebar-section">// Display</div>', unsafe_allow_html=True)
    auto_refresh = st.checkbox("Auto-refresh (30s)", value=False)
    show_rolling = st.checkbox("Show rolling averages", value=True)

    st.markdown("---")
    st.markdown(f'<div style="font-family:var(--font-mono);font-size:0.6rem;color:#3d4a66;line-height:1.8">DATASET · MetroPT-3<br>SOURCE · Metro do Porto<br>SENSORS · 15 channels<br>RATE · 0.1 Hz (10s)</div>', unsafe_allow_html=True)

# ── Load data ─────────────────────────────────────────────────────────────────
if use_demo:
    sensor_df, pred_df, spark_log_df = generate_demo_data(3000)
else:
    try:
        db = get_mongo()
        sensor_df    = load_sensor_data(db, time_window)
        pred_df      = load_predictions(db, time_window)
        spark_log_df = load_spark_log(db)
        if sensor_df.empty or pred_df.empty:
            st.warning("MongoDB returned empty — using demo data.")
            sensor_df, pred_df, spark_log_df = generate_demo_data(3000)
    except Exception as e:
        st.error(f"MongoDB: {e}")
        sensor_df, pred_df, spark_log_df = generate_demo_data(3000)

# Apply threshold
if not pred_df.empty:
    pred_df["is_anomaly"] = pred_df["anomaly_score"] > threshold

# ── HEADER BAR ─────────────────────────────────────────────────────────────────
now_utc = datetime.utcnow()
any_active_alert = not pred_df.empty and pred_df["is_anomaly"].iloc[-20:].any()
dot_html = '<span class="dot-alert"></span>' if any_active_alert else '<span class="dot-live"></span>'
sys_status = "ALERT — ANOMALY DETECTED" if any_active_alert else "NOMINAL — ALL SYSTEMS GO"

st.markdown(f"""
<div class="header-bar">
    <div class="header-logo">
        <span class="header-icon">🚆</span>
        <div>
            <div class="header-title">APU Fault Monitor</div>
            <div class="header-subtitle">AIR PRODUCTION UNIT · METROPT-3 · METRO DO PORTO, PORTUGAL</div>
        </div>
    </div>
    <div style="display:flex;align-items:center;gap:32px">
        <div style="text-align:center">
            <div style="font-family:var(--font-mono);font-size:0.6rem;color:var(--text-muted);letter-spacing:0.1em;text-transform:uppercase">System</div>
            <div style="font-family:var(--font-mono);font-size:0.72rem;margin-top:2px">{dot_html}{sys_status}</div>
        </div>
        <div class="header-meta">
            <div class="header-time">{now_utc.strftime('%H:%M:%S')} UTC</div>
            <div class="header-status">{now_utc.strftime('%Y-%m-%d')} · {'DEMO MODE' if use_demo else 'LIVE · MongoDB'}</div>
        </div>
    </div>
</div>
""", unsafe_allow_html=True)

# ── KPI CARDS ─────────────────────────────────────────────────────────────────
if not pred_df.empty:
    total       = len(pred_df)
    anomalies   = int(pred_df["is_anomaly"].sum())
    anomaly_pct = 100 * anomalies / total if total else 0
    avg_score   = pred_df["anomaly_score"].mean()
    max_score   = pred_df["anomaly_score"].max()
    latest_score = pred_df["anomaly_score"].iloc[-1]
    latest_status = "FAULT" if latest_score > threshold else "NORMAL"

    last_tp2   = pred_df["TP2"].iloc[-1] if "TP2" in pred_df.columns else sensor_df["TP2"].iloc[-1] if not sensor_df.empty else 0
    last_motor = pred_df["Motor_current"].iloc[-1] if "Motor_current" in pred_df.columns else sensor_df["Motor_current"].iloc[-1] if not sensor_df.empty else 0

    status_color = "red" if latest_score > threshold else "green"

    st.markdown(f"""
    <div class="kpi-grid">
        <div class="kpi-card amber">
            <div class="kpi-icon">⬡</div>
            <div class="kpi-label">Records Processed</div>
            <div class="kpi-value">{total:,}</div>
            <div class="kpi-sub">{time_window}h window · 10s interval</div>
        </div>
        <div class="kpi-card red">
            <div class="kpi-icon">⚠</div>
            <div class="kpi-label">Anomalies Detected</div>
            <div class="kpi-value">{anomalies:,}</div>
            <div class="kpi-sub"><span class="kpi-trend-up">▲</span> {anomaly_pct:.1f}% of total</div>
        </div>
        <div class="kpi-card cyan">
            <div class="kpi-icon">◈</div>
            <div class="kpi-label">Avg Anomaly Score</div>
            <div class="kpi-value">{avg_score:.2f}</div>
            <div class="kpi-sub">Peak: {max_score:.2f} · Threshold: {threshold:.1f}</div>
        </div>
        <div class="kpi-card blue">
            <div class="kpi-icon">⊙</div>
            <div class="kpi-label">Compressor TP2</div>
            <div class="kpi-value">{last_tp2:.1f}<span style="font-size:0.8rem;color:var(--text-muted)"> bar</span></div>
            <div class="kpi-sub">Nominal: 8.0–12.0 bar</div>
        </div>
        <div class="kpi-card {status_color}">
            <div class="kpi-icon">◉</div>
            <div class="kpi-label">Current Status</div>
            <div class="kpi-value" style="font-size:1.2rem;letter-spacing:0.05em">{latest_status}</div>
            <div class="kpi-sub">Score: {latest_score:.2f} · Motor: {last_motor:.1f}A</div>
        </div>
    </div>
    """, unsafe_allow_html=True)

# ── ROW 1: Anomaly Score + Fault Distribution ──────────────────────────────────
col_left, col_right = st.columns([3, 1])

with col_left:
    st.markdown("""
    <div class="section-header">
        <span class="section-tag">CH.01</span>
        <span class="section-title">Anomaly Score Timeline</span>
    </div>""", unsafe_allow_html=True)

    if not pred_df.empty:
        fig_score = go.Figure()

        # Fill area under curve
        fig_score.add_trace(go.Scatter(
            x=pred_df["timestamp"], y=pred_df["anomaly_score"],
            mode="lines", name="Anomaly Score",
            line=dict(color="#22d3ee", width=1.5),
            fill="tozeroy",
            fillcolor="rgba(34,211,238,0.05)",
        ))

        # Anomaly markers
        anom_pts = pred_df[pred_df["is_anomaly"]]
        if not anom_pts.empty:
            fig_score.add_trace(go.Scatter(
                x=anom_pts["timestamp"], y=anom_pts["anomaly_score"],
                mode="markers", name="Fault Event",
                marker=dict(color="#ef4444", size=6, symbol="x-thin", line=dict(width=2, color="#ef4444")),
            ))

        # Threshold line
        fig_score.add_hline(
            y=threshold, line_dash="dot", line_color="rgba(239,68,68,0.6)", line_width=1,
            annotation_text=f"  THR {threshold:.1f}", annotation_font_color="#ef4444",
            annotation_font_size=9, annotation_position="top left",
        )

        # Anomaly window shading
        in_anomaly = False
        start_ts = None
        for _, row in pred_df.iterrows():
            if row["is_anomaly"] and not in_anomaly:
                in_anomaly = True
                start_ts = row["timestamp"]
            elif not row["is_anomaly"] and in_anomaly:
                in_anomaly = False
                fig_score.add_vrect(
                    x0=start_ts, x1=row["timestamp"],
                    fillcolor="rgba(239,68,68,0.07)",
                    layer="below", line_width=0,
                )

        layout = base_layout(height=260)
        layout.update(
            yaxis=dict(gridcolor=GRID_COL, zerolinecolor=GRID_COL, tickfont=dict(size=9), title="Score", title_font_size=9),
            xaxis=dict(gridcolor=GRID_COL, zerolinecolor=GRID_COL, tickfont=dict(size=9)),
            showlegend=True,
            legend=dict(orientation="h", y=1.05, x=0, font=dict(size=9)),
        )
        fig_score.update_layout(**layout)
        st.plotly_chart(fig_score, use_container_width=True, config={"displayModeBar": False})

with col_right:
    st.markdown("""
    <div class="section-header">
        <span class="section-tag">CH.02</span>
        <span class="section-title">Fault Breakdown</span>
    </div>""", unsafe_allow_html=True)

    if not pred_df.empty and "fault_hint" in pred_df.columns:
        anom_sub = pred_df[pred_df["is_anomaly"]]
        if not anom_sub.empty:
            fc = anom_sub["fault_hint"].value_counts()
            fig_donut = go.Figure(go.Pie(
                labels=fc.index.tolist(),
                values=fc.values.tolist(),
                hole=0.62,
                direction="clockwise",
                textinfo="none",
                hoverinfo="label+percent+value",
                marker=dict(
                    colors=["#3b82f6", "#e8a020", "#ef4444", "#22d3ee", "#6b7a99"],
                    line=dict(color=PAPER_BG, width=2),
                ),
            ))
            fig_donut.add_annotation(
                text=f"<b>{len(anom_sub)}</b><br><span style='font-size:8px'>EVENTS</span>",
                x=0.5, y=0.5, showarrow=False,
                font=dict(color="#d4d8e2", size=14, family=FONT_MONO),
                align="center",
            )
            layout_d = base_layout(height=220, margin=dict(l=0, r=0, t=0, b=0))
            layout_d.pop("xaxis", None); layout_d.pop("yaxis", None)
            layout_d.update(showlegend=True,
                legend=dict(orientation="v", x=0, y=0, font=dict(size=8, family=FONT_MONO)))
            fig_donut.update_layout(**layout_d)
            st.plotly_chart(fig_donut, use_container_width=True, config={"displayModeBar": False})
        else:
            st.markdown('<div style="font-family:var(--font-mono);font-size:0.72rem;color:var(--green);padding:20px 0">◉ No fault events in window</div>', unsafe_allow_html=True)

# ── ROW 2: Sensor signals ──────────────────────────────────────────────────────
st.markdown("""
<div class="section-header">
    <span class="section-tag">CH.03</span>
    <span class="section-title">Sensor Signal Monitor</span>
</div>""", unsafe_allow_html=True)

selected_signals = []
if show_tp2   and "TP2"           in sensor_df.columns: selected_signals.append(("TP2",           "#22d3ee", "TP2 (bar)",   8.0, 12.0))
if show_tp3   and "TP3"           in sensor_df.columns: selected_signals.append(("TP3",           "#3b82f6", "TP3 (bar)",   7.0, 10.5))
if show_motor and "Motor_current" in sensor_df.columns: selected_signals.append(("Motor_current", "#e8a020", "Motor (A)",  10.0, 25.0))
if show_oil   and "Oil_temperature" in sensor_df.columns: selected_signals.append(("Oil_temperature", "#ef4444", "Oil Temp (°C)", 40.0, 90.0))
if show_dv    and "DV_pressure"   in sensor_df.columns: selected_signals.append(("DV_pressure",   "#22c55e", "DV Press (bar)", 0.5, 2.0))

if selected_signals and not sensor_df.empty:
    n_rows = len(selected_signals)
    row_h  = 150
    fig_sens = make_subplots(
        rows=n_rows, cols=1,
        shared_xaxes=True,
        vertical_spacing=0.04,
        subplot_titles=[s[2] for s in selected_signals],
    )
    for i, (col, color, label, lo, hi) in enumerate(selected_signals, 1):
        # Signal line
        fig_sens.add_trace(go.Scatter(
            x=sensor_df["timestamp"], y=sensor_df[col],
            mode="lines", name=label,
            line=dict(color=color, width=1.2),
            showlegend=False,
        ), row=i, col=1)

        # Rolling average overlay
        if show_rolling:
            roll_col = f"rolling_{col}"
            if roll_col in pred_df.columns:
                roll_data = pred_df[roll_col]
            else:
                roll_data = sensor_df[col].rolling(180, min_periods=1).mean()
            fig_sens.add_trace(go.Scatter(
                x=sensor_df["timestamp"], y=roll_data,
                mode="lines", name=f"{label} (30m avg)",
                line=dict(color=color, width=1.5, dash="dot"),
                opacity=0.5, showlegend=False,
            ), row=i, col=1)

        # Safe zone band
        y_range_vals = sensor_df[col]
        fig_sens.add_hrect(
            y0=lo, y1=hi,
            fillcolor=f"rgba({int(color[1:3],16)},{int(color[3:5],16)},{int(color[5:7],16)},0.03)",
            line_width=0, row=i, col=1,
        )
        # Limit lines
        fig_sens.add_hline(y=lo, line_dash="dot", line_color=f"rgba({int(color[1:3],16)},{int(color[3:5],16)},{int(color[5:7],16)},0.3)",
                           line_width=0.8, row=i, col=1)
        fig_sens.add_hline(y=hi, line_dash="dot", line_color=f"rgba({int(color[1:3],16)},{int(color[3:5],16)},{int(color[5:7],16)},0.3)",
                           line_width=0.8, row=i, col=1)

        fig_sens.update_xaxes(gridcolor=GRID_COL, zerolinecolor=GRID_COL, tickfont=dict(size=8), row=i, col=1)
        fig_sens.update_yaxes(gridcolor=GRID_COL, zerolinecolor=GRID_COL, tickfont=dict(size=8), row=i, col=1)

    fig_sens.update_layout(
        paper_bgcolor=PAPER_BG, plot_bgcolor=PLOT_BG,
        font=dict(color=TEXT_COL, family=FONT_MONO, size=9),
        height=n_rows * row_h + 40,
        margin=dict(l=50, r=16, t=28, b=30),
        showlegend=False,
    )
    # Style subplot titles
    for annotation in fig_sens.layout.annotations:
        annotation.font.update(size=9, color="#6b7a99", family=FONT_MONO)
        annotation.x = 0

    st.plotly_chart(fig_sens, use_container_width=True, config={"displayModeBar": False})

# ── ROW 3: Correlation heatmap + Cluster ─────────────────────────────────────
col_a, col_b = st.columns([1, 1])

with col_a:
    st.markdown("""
    <div class="section-header">
        <span class="section-tag">CH.04</span>
        <span class="section-title">Sensor Correlation</span>
    </div>""", unsafe_allow_html=True)

    if not sensor_df.empty:
        corr_cols = [c for c in ["TP2","TP3","H1","DV_pressure","Reservoirs","Oil_temperature","Motor_current"] if c in sensor_df.columns]
        if len(corr_cols) >= 2:
            corr = sensor_df[corr_cols].corr()
            short = {"TP2":"TP2","TP3":"TP3","H1":"H1","DV_pressure":"DV","Reservoirs":"RES","Oil_temperature":"OIL","Motor_current":"MOT"}
            labels = [short.get(c,c) for c in corr_cols]
            fig_heat = go.Figure(go.Heatmap(
                z=corr.values,
                x=labels, y=labels,
                colorscale=[
                    [0.0, "#0c1018"], [0.3, "#164e63"],
                    [0.5, "#0f766e"], [0.7, "#ca8a04"],
                    [1.0, "#e8a020"]
                ],
                zmin=-1, zmax=1,
                showscale=False,
                text=[[f"{v:.2f}" for v in row] for row in corr.values],
                texttemplate="%{text}",
                textfont=dict(size=8, family=FONT_MONO, color="#d4d8e2"),
            ))
            layout_h = base_layout(height=240, margin=dict(l=40,r=10,t=10,b=30))
            layout_h.pop("xaxis", None); layout_h.pop("yaxis", None)
            fig_heat.update_layout(**layout_h)
            fig_heat.update_xaxes(tickfont=dict(size=8, family=FONT_MONO))
            fig_heat.update_yaxes(tickfont=dict(size=8, family=FONT_MONO))
            st.plotly_chart(fig_heat, use_container_width=True, config={"displayModeBar": False})

with col_b:
    st.markdown("""
    <div class="section-header">
        <span class="section-tag">CH.05</span>
        <span class="section-title">KMeans Cluster Map</span>
    </div>""", unsafe_allow_html=True)

    if not pred_df.empty and "cluster_id" in pred_df.columns:
        sample = pred_df.sample(min(1200, len(pred_df)), random_state=42).copy()
        sample["color"] = sample["is_anomaly"].map({True: "#ef4444", False: "#22d3ee"})
        sample["size"]  = sample["is_anomaly"].map({True: 5, False: 3})

        fig_clust = go.Figure()
        for is_anom, label, color, sz in [(False, "Normal", "#22d3ee", 3), (True, "Anomaly", "#ef4444", 5)]:
            sub = sample[sample["is_anomaly"] == is_anom]
            if not sub.empty:
                fig_clust.add_trace(go.Scatter(
                    x=sub["TP2"] if "TP2" in sub.columns else sub.index,
                    y=sub["Motor_current"] if "Motor_current" in sub.columns else sub["anomaly_score"],
                    mode="markers",
                    name=label,
                    marker=dict(
                        color=color, size=sz, opacity=0.65,
                        line=dict(width=0),
                    ),
                ))

        layout_c = base_layout(height=240, margin=dict(l=44,r=10,t=10,b=36))
        layout_c.update(
            xaxis=dict(title="TP2 (bar)", gridcolor=GRID_COL, tickfont=dict(size=8), title_font_size=9),
            yaxis=dict(title="Motor (A)", gridcolor=GRID_COL, tickfont=dict(size=8), title_font_size=9),
            legend=dict(orientation="h", y=1.05, font=dict(size=8)),
            showlegend=True,
        )
        fig_clust.update_layout(**layout_c)
        st.plotly_chart(fig_clust, use_container_width=True, config={"displayModeBar": False})

# ── ROW 3b: Full-width scrollable Alert Feed ──────────────────────────────────
st.markdown("""
<div class="section-header">
    <span class="section-tag">CH.06</span>
    <span class="section-title">Alert Feed — Fault Event Log</span>
</div>""", unsafe_allow_html=True)

if not pred_df.empty:
    all_anoms = pred_df[pred_df["is_anomaly"]].copy().iloc[::-1].reset_index(drop=True)

    fault_cls = {
        "LOW_COMPRESSOR_PRESSURE": "fault-LOW",
        "HIGH_MOTOR_CURRENT":      "fault-HIGH",
        "OVERHEATING":             "fault-OVER",
        "VALVE_PRESSURE_DROP":     "fault-VALV",
        "GENERAL_ANOMALY":         "fault-GEN",
    }
    fault_icons = {
        "LOW_COMPRESSOR_PRESSURE": "&#8595;P",
        "HIGH_MOTOR_CURRENT":      "&#8593;A",
        "OVERHEATING":             "&#9651;T",
        "VALVE_PRESSURE_DROP":     "&#8596;V",
        "GENERAL_ANOMALY":         "&#9651;!",
    }

    af = []
    af.append('<div style="background:var(--bg-card);border:1px solid var(--border-dim);border-radius:4px;overflow:hidden;margin-bottom:3.5rem;">')

    # Header bar
    count_label = f"ALL {len(all_anoms)} FAULT EVENTS" if not all_anoms.empty else "NO ACTIVE FAULTS"
    af.append(
        '<div style="display:flex;align-items:center;justify-content:space-between;'
        'background:rgba(239,68,68,0.07);border-bottom:1px solid var(--border-dim);padding:8px 16px;">'
        '<div style="display:flex;align-items:center;gap:8px;font-family:var(--font-cond);'
        'font-size:0.72rem;letter-spacing:0.1em;text-transform:uppercase;color:var(--red);">'
        '<span class="dot-alert"></span>'
        f'FAULT LOG &middot; {count_label}'
        '</div>'
        '<div style="font-family:var(--font-mono);font-size:0.6rem;color:var(--text-dim);letter-spacing:0.06em;">'
        f'THRESHOLD &gt; {threshold:.1f} &middot; SCROLL TO BROWSE'
        '</div>'
        '</div>'
    )

    if all_anoms.empty:
        af.append('<div style="padding:18px 20px;font-family:var(--font-mono);font-size:0.75rem;color:var(--green);">&#9711; No fault events in current window</div>')
    else:
        # Column header row
        af.append(
            '<div style="display:grid;grid-template-columns:36px 160px 80px 70px 70px 70px 1fr;'
            'gap:0;padding:5px 16px;background:rgba(26,37,64,0.6);border-bottom:1px solid var(--border-dim);">'
            '<div style="font-family:var(--font-cond);font-size:0.62rem;letter-spacing:0.1em;text-transform:uppercase;color:var(--text-dim);">#</div>'
            '<div style="font-family:var(--font-cond);font-size:0.62rem;letter-spacing:0.1em;text-transform:uppercase;color:var(--text-dim);">Timestamp</div>'
            '<div style="font-family:var(--font-cond);font-size:0.62rem;letter-spacing:0.1em;text-transform:uppercase;color:var(--text-dim);text-align:center;">Score</div>'
            '<div style="font-family:var(--font-cond);font-size:0.62rem;letter-spacing:0.1em;text-transform:uppercase;color:var(--text-dim);text-align:center;">TP2</div>'
            '<div style="font-family:var(--font-cond);font-size:0.62rem;letter-spacing:0.1em;text-transform:uppercase;color:var(--text-dim);text-align:center;">Motor</div>'
            '<div style="font-family:var(--font-cond);font-size:0.62rem;letter-spacing:0.1em;text-transform:uppercase;color:var(--text-dim);text-align:center;">Oil &#176;C</div>'
            '<div style="font-family:var(--font-cond);font-size:0.62rem;letter-spacing:0.1em;text-transform:uppercase;color:var(--text-dim);">Fault Type</div>'
            '</div>'
        )

        # Scrollable event rows
        af.append('<div style="max-height:260px;overflow-y:auto;">')
        for idx, arow in all_anoms.iterrows():
            ts    = arow["timestamp"].strftime("%Y-%m-%d %H:%M:%S") if hasattr(arow["timestamp"], "strftime") else str(arow["timestamp"])
            score = float(arow.get("anomaly_score", 0))
            hint  = str(arow.get("fault_hint", "UNKNOWN"))
            cls   = fault_cls.get(hint, "fault-GEN")
            icon  = fault_icons.get(hint, "!")
            label = hint.replace("_", " ")
            tp2_v = f'{float(arow["TP2"]):.2f}'             if "TP2"             in arow and pd.notna(arow["TP2"])             else "&#8212;"
            mot_v = f'{float(arow["Motor_current"]):.1f}'   if "Motor_current"   in arow and pd.notna(arow["Motor_current"])   else "&#8212;"
            oil_v = f'{float(arow["Oil_temperature"]):.1f}' if "Oil_temperature" in arow and pd.notna(arow["Oil_temperature"]) else "&#8212;"

            if score >= 12:
                score_bg = "rgba(239,68,68,0.30)"; score_col = "#fca5a5"
            elif score >= 9:
                score_bg = "rgba(239,68,68,0.18)"; score_col = "#ef4444"
            else:
                score_bg = "rgba(239,68,68,0.10)"; score_col = "#f87171"

            row_bg = "rgba(239,68,68,0.04)" if idx % 2 == 0 else "transparent"
            af.append(
                f'<div style="display:grid;grid-template-columns:36px 160px 80px 70px 70px 70px 1fr;'
                f'gap:0;padding:7px 16px;border-bottom:1px solid rgba(26,37,64,0.5);'
                f'background:{row_bg};align-items:center;" '
                f'onmouseover="this.style.background=\'rgba(239,68,68,0.08)\'" '
                f'onmouseout="this.style.background=\'{row_bg}\'">'
                f'<div style="font-family:var(--font-mono);font-size:0.6rem;color:var(--text-dim);">{idx+1}</div>'
                f'<div style="font-family:var(--font-mono);font-size:0.7rem;color:var(--text-muted);">{ts}</div>'
                f'<div style="text-align:center;">'
                f'<span style="font-family:var(--font-mono);font-size:0.72rem;font-weight:600;'
                f'background:{score_bg};color:{score_col};padding:2px 9px;border-radius:2px;">{score:.1f}</span>'
                f'</div>'
                f'<div style="font-family:var(--font-mono);font-size:0.68rem;color:var(--cyan);text-align:center;">{tp2_v}</div>'
                f'<div style="font-family:var(--font-mono);font-size:0.68rem;color:var(--amber);text-align:center;">{mot_v}</div>'
                f'<div style="font-family:var(--font-mono);font-size:0.68rem;color:var(--red);text-align:center;">{oil_v}</div>'
                f'<div><span class="{cls} fault-badge" style="white-space:nowrap;">{icon} {label}</span></div>'
                f'</div>'
            )
        af.append('</div>')  # close scrollable div

    af.append('</div>')  # close outer card
    st.markdown("".join(af), unsafe_allow_html=True)

# ── ROW 4: Score histogram + Anomaly rate bar + Spark audit ───────────────────

col_x, col_y, col_z = st.columns([1, 1.2, 1])

with col_x:
    if not pred_df.empty:
        fig_hist = go.Figure()
        normal_scores = pred_df[~pred_df["is_anomaly"]]["anomaly_score"]
        fault_scores  = pred_df[pred_df["is_anomaly"]]["anomaly_score"]

        fig_hist.add_trace(go.Histogram(
            x=normal_scores, name="Normal", nbinsx=40,
            marker_color="rgba(34,211,238,0.5)",
            marker_line=dict(color="rgba(34,211,238,0.8)", width=0.5),
        ))
        fig_hist.add_trace(go.Histogram(
            x=fault_scores, name="Anomaly", nbinsx=20,
            marker_color="rgba(239,68,68,0.6)",
            marker_line=dict(color="rgba(239,68,68,0.9)", width=0.5),
        ))
        fig_hist.add_vline(x=threshold, line_dash="dot", line_color="rgba(232,160,32,0.7)", line_width=1.5)

        layout_hist = base_layout(height=200, margin=dict(l=40,r=10,t=10,b=36))
        layout_hist.update(
            barmode="overlay",
            xaxis=dict(title="Score", gridcolor=GRID_COL, tickfont=dict(size=8), title_font_size=9),
            yaxis=dict(title="Count", gridcolor=GRID_COL, tickfont=dict(size=8), title_font_size=9),
            legend=dict(orientation="h", y=1.05, font=dict(size=8)),
        )
        fig_hist.update_layout(**layout_hist)
        st.plotly_chart(fig_hist, use_container_width=True, config={"displayModeBar": False})

with col_y:
    if not pred_df.empty and "timestamp" in pred_df.columns:
        # Hourly anomaly counts
        df_hr = pred_df.copy()
        df_hr["hour"] = df_hr["timestamp"].dt.floor("1h")
        hourly = df_hr.groupby("hour").agg(
            total=("is_anomaly", "count"),
            faults=("is_anomaly", "sum")
        ).reset_index()
        hourly["rate"] = 100 * hourly["faults"] / hourly["total"]

        fig_bar = go.Figure()
        fig_bar.add_trace(go.Bar(
            x=hourly["hour"], y=hourly["total"],
            name="Total", marker_color="rgba(34,211,238,0.15)",
            marker_line=dict(color="rgba(34,211,238,0.3)", width=0.5),
        ))
        fig_bar.add_trace(go.Bar(
            x=hourly["hour"], y=hourly["faults"],
            name="Faults", marker_color="rgba(239,68,68,0.55)",
            marker_line=dict(color="rgba(239,68,68,0.8)", width=0.5),
        ))

        layout_bar = base_layout(height=200, margin=dict(l=40,r=10,t=10,b=50))
        layout_bar.update(
            barmode="overlay",
            xaxis=dict(gridcolor=GRID_COL, tickfont=dict(size=8), tickangle=-30),
            yaxis=dict(title="Records", gridcolor=GRID_COL, tickfont=dict(size=8), title_font_size=9),
            legend=dict(orientation="h", y=1.05, font=dict(size=8)),
        )
        fig_bar.update_layout(**layout_bar)
        st.plotly_chart(fig_bar, use_container_width=True, config={"displayModeBar": False})

with col_z:
    # Spark audit log
    # Build the entire block as a plain list then join — never interpolate
    # row HTML into an outer f-string, which causes Streamlit to escape it.
    ap = []
    ap.append('<div style="background:var(--bg-card);border:1px solid var(--border-dim);border-radius:4px;overflow:hidden;height:220px">')
    ap.append('<div style="background:rgba(26,37,64,0.8);border-bottom:1px solid var(--border-dim);padding:7px 12px;font-family:var(--font-cond);font-size:0.68rem;letter-spacing:0.1em;text-transform:uppercase;color:var(--amber-dim)">&#9670; Spark Run Audit Log</div>')
    ap.append('<table class="audit-table" style="width:100%"><thead><tr><th>Status</th><th>Started</th><th>Records</th></tr></thead><tbody>')
    if not spark_log_df.empty:
        for _, srow in spark_log_df.head(6).iterrows():
            s_status  = str(srow.get("status", "UNKNOWN"))
            s_color   = "#22c55e" if s_status == "SUCCESS" else "#ef4444"
            s_started = str(srow.get("started_at", "&#8212;"))[:19]
            s_recs    = f"{int(srow.get('records_processed', 0)):,}" if "records_processed" in srow else "&#8212;"
            ap.append(f'<tr><td style="color:{s_color};font-size:0.62rem">{s_status}</td><td style="color:var(--text-muted)">{s_started}</td><td style="color:var(--cyan)">{s_recs}</td></tr>')
    else:
        ap.append('<tr><td colspan="3" style="color:var(--text-muted);padding:16px 12px">No Spark runs found</td></tr>')
    ap.append('</tbody></table></div>')
    st.markdown("".join(ap), unsafe_allow_html=True)

# ── Footer ─────────────────────────────────────────────────────────────────────
st.markdown(f"""
<div style="margin-top:1.5rem;padding:10px 16px;border-top:1px solid var(--border-dim);
            display:flex;justify-content:space-between;align-items:center">
    <div style="font-family:var(--font-mono);font-size:0.6rem;color:var(--text-dim)">
        MetroPT-3 · Veloso et al. (2022) · UCI ML Repository · CC BY 4.0
    </div>
    <div style="font-family:var(--font-mono);font-size:0.6rem;color:var(--text-dim)">
        Kafka → MongoDB → Spark ML → Streamlit · Orchestrated by Airflow (15min)
    </div>
    <div style="font-family:var(--font-mono);font-size:0.6rem;color:var(--text-dim)">
        Last render: {now_utc.strftime('%Y-%m-%d %H:%M:%S')} UTC
    </div>
</div>
""", unsafe_allow_html=True)

# ── Auto-refresh ───────────────────────────────────────────────────────────────
if auto_refresh:
    time.sleep(30)
    st.rerun()
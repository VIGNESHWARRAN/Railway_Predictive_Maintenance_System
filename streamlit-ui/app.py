"""
app.py — MetroPT-3 Railway APU Fault Detection Dashboard
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
    page_title="Railway APU Monitor",
    page_icon="🚆",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Custom CSS ─────────────────────────────────────────────────────────────────
st.markdown("""
<style>
    [data-testid="stAppViewContainer"] { background: #0a0f1e; color: #e0e6f0; }
    [data-testid="stSidebar"]          { background: #0d1526; border-right: 1px solid #1e3a5f; }
    .metric-card {
        background: linear-gradient(135deg, #0d1f3c, #112240);
        border: 1px solid #1e3a5f;
        border-radius: 12px;
        padding: 20px;
        text-align: center;
    }
    .alert-box {
        background: rgba(255,60,60,0.12);
        border: 1px solid #ff3c3c;
        border-radius: 8px;
        padding: 12px 16px;
        margin: 6px 0;
        font-size: 0.9rem;
    }
    .normal-box {
        background: rgba(0,200,100,0.1);
        border: 1px solid #00c864;
        border-radius: 8px;
        padding: 12px 16px;
        margin: 6px 0;
        font-size: 0.9rem;
    }
    h1, h2, h3 { color: #4fc3f7 !important; }
    .stMetric label { color: #90caf9 !important; }
    .stMetric [data-testid="metric-container"] { background: #0d1f3c; border-radius: 10px; padding: 12px; }
</style>
""", unsafe_allow_html=True)

# ── MongoDB connection ─────────────────────────────────────────────────────────
@st.cache_resource
def get_mongo():
    client = MongoClient("mongodb://localhost:27017", serverSelectionTimeoutMS=3000)
    return client["railway_db"]

def load_sensor_data(db, hours: int = 6) -> pd.DataFrame:
    since = (datetime.utcnow() - timedelta(hours=hours)).isoformat()
    cursor = db["sensor_data"].find(
        {"timestamp": {"$gte": since}},
        {"_id": 0}
    ).sort("timestamp", DESCENDING).limit(5000)
    df = pd.DataFrame(list(cursor))
    if not df.empty and "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df = df.sort_values("timestamp")
    return df

def load_predictions(db, hours: int = 6) -> pd.DataFrame:
    since = (datetime.utcnow() - timedelta(hours=hours)).isoformat()
    cursor = db["predictions"].find(
        {"timestamp": {"$gte": since}},
        {"_id": 0}
    ).sort("timestamp", DESCENDING).limit(5000)
    df = pd.DataFrame(list(cursor))
    if not df.empty and "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df = df.sort_values("timestamp")
    return df

# ── Demo data fallback (when MongoDB is not connected) ─────────────────────────
def generate_demo_data(n: int = 2000) -> tuple[pd.DataFrame, pd.DataFrame]:
    np.random.seed(42)
    timestamps = pd.date_range(end=datetime.utcnow(), periods=n, freq="10s")

    # Simulate normal + anomalous readings
    tp2      = np.random.normal(10.5, 0.3, n)
    motor    = np.random.normal(18.0, 1.5, n)
    oil_temp = np.random.normal(65.0, 4.0, n)

    # Inject 3 anomaly windows
    for start, end in [(400, 420), (900, 930), (1600, 1620)]:
        tp2[start:end]      -= 3.5
        motor[start:end]    += 8.0
        oil_temp[start:end] += 25.0

    sensor_df = pd.DataFrame({
        "timestamp":     timestamps,
        "TP2":           tp2,
        "TP3":           np.random.normal(8.8, 0.2, n),
        "H1":            np.random.normal(9.2, 0.3, n),
        "DV_pressure":   np.random.normal(1.2, 0.1, n),
        "Reservoirs":    np.random.normal(9.0, 0.4, n),
        "Oil_temperature": oil_temp,
        "Motor_current": motor,
    })

    scores = np.abs(np.random.normal(2.0, 0.8, n))
    for start, end in [(400, 420), (900, 930), (1600, 1620)]:
        scores[start:end] = np.random.normal(9.5, 0.5, end - start)

    pred_df = pd.DataFrame({
        "timestamp":    timestamps,
        "anomaly_score": scores,
        "is_anomaly":    scores > 6.0,
        "fault_hint":    np.where(
            scores > 6.0,
            np.random.choice(
                ["LOW_COMPRESSOR_PRESSURE", "HIGH_MOTOR_CURRENT", "OVERHEATING", "VALVE_PRESSURE_DROP"],
                n
            ),
            "NORMAL"
        ),
    })

    return sensor_df, pred_df

# ── Sidebar ────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## ⚙️ Controls")
    time_window = st.selectbox("Time Window", [1, 3, 6, 12, 24], index=2, format_func=lambda x: f"{x}h")
    auto_refresh = st.checkbox("Auto-refresh (30s)", value=False)
    use_demo     = st.checkbox("Use demo data", value=True, help="Uncheck when MongoDB is running")
    st.markdown("---")
    st.markdown("### 🎚️ Alert Threshold")
    threshold = st.slider("Anomaly score threshold", 3.0, 15.0, 6.0, 0.5)
    st.markdown("---")
    st.markdown("### 📡 Sensor Toggles")
    show_tp2   = st.checkbox("TP2 — Compressor pressure", True)
    show_motor = st.checkbox("Motor current", True)
    show_oil   = st.checkbox("Oil temperature", True)

# ── Load data ─────────────────────────────────────────────────────────────────
if use_demo:
    sensor_df, pred_df = generate_demo_data(3000)
else:
    try:
        db        = get_mongo()
        sensor_df = load_sensor_data(db, time_window)
        pred_df   = load_predictions(db, time_window)
        if sensor_df.empty:
            st.warning("No data in MongoDB — switching to demo mode.")
            sensor_df, pred_df = generate_demo_data(3000)
    except Exception as e:
        st.error(f"MongoDB connection failed: {e}")
        sensor_df, pred_df = generate_demo_data(3000)

# Apply threshold
if not pred_df.empty:
    pred_df["is_anomaly"] = pred_df["anomaly_score"] > threshold

# ── Header ─────────────────────────────────────────────────────────────────────
st.markdown("# 🚆 Railway APU Fault Detection")
st.markdown(f"**MetroPT-3 · Metro do Porto** — Last updated: `{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC`")
st.markdown("---")

# ── KPI Row ────────────────────────────────────────────────────────────────────
if not pred_df.empty:
    total      = len(pred_df)
    anomalies  = pred_df["is_anomaly"].sum()
    anomaly_pct = 100 * anomalies / total if total else 0
    avg_score  = pred_df["anomaly_score"].mean()
    latest_score = pred_df["anomaly_score"].iloc[-1]

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("📊 Total Records", f"{total:,}")
    with col2:
        st.metric("🔴 Anomalies", f"{anomalies:,}", delta=f"{anomaly_pct:.1f}%", delta_color="inverse")
    with col3:
        st.metric("📈 Avg Anomaly Score", f"{avg_score:.2f}")
    with col4:
        status = "🔴 ANOMALY" if latest_score > threshold else "🟢 NORMAL"
        st.metric("⚡ Current Status", status)

st.markdown("---")

# ── Anomaly score time series ──────────────────────────────────────────────────
if not pred_df.empty:
    st.subheader("🔮 Anomaly Score Over Time")
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=pred_df["timestamp"], y=pred_df["anomaly_score"],
        mode="lines", name="Anomaly Score",
        line=dict(color="#4fc3f7", width=1.5)
    ))
    fig.add_hline(y=threshold, line_dash="dash", line_color="#ff5252",
                  annotation_text=f"Threshold ({threshold})", annotation_position="top left")

    anomaly_pts = pred_df[pred_df["is_anomaly"]]
    if not anomaly_pts.empty:
        fig.add_trace(go.Scatter(
            x=anomaly_pts["timestamp"], y=anomaly_pts["anomaly_score"],
            mode="markers", name="Anomaly",
            marker=dict(color="#ff5252", size=7, symbol="x")
        ))

    fig.update_layout(
        paper_bgcolor="#0a0f1e", plot_bgcolor="#0d1526",
        font=dict(color="#e0e6f0"),
        xaxis=dict(gridcolor="#1e3a5f"), yaxis=dict(gridcolor="#1e3a5f"),
        legend=dict(bgcolor="#0d1526"),
        height=320, margin=dict(l=40, r=20, t=20, b=40)
    )
    st.plotly_chart(fig, use_container_width=True)

# ── Sensor signal panels ───────────────────────────────────────────────────────
if not sensor_df.empty:
    traces = []
    titles = []
    if show_tp2:
        traces.append(("TP2", "#4fc3f7", "TP2 — Compressor Pressure (bar)"))
    if show_motor:
        traces.append(("Motor_current", "#ffb74d", "Motor Current (A)"))
    if show_oil:
        traces.append(("Oil_temperature", "#ef5350", "Oil Temperature (°C)"))

    if traces:
        st.subheader("📈 Sensor Signals")
        fig2 = make_subplots(rows=len(traces), cols=1, shared_xaxes=True,
                             subplot_titles=[t[2] for t in traces],
                             vertical_spacing=0.08)

        for i, (col_name, color, title) in enumerate(traces, 1):
            if col_name in sensor_df.columns:
                fig2.add_trace(
                    go.Scatter(x=sensor_df["timestamp"], y=sensor_df[col_name],
                               mode="lines", name=title,
                               line=dict(color=color, width=1.2)),
                    row=i, col=1
                )

        fig2.update_layout(
            paper_bgcolor="#0a0f1e", plot_bgcolor="#0d1526",
            font=dict(color="#e0e6f0"),
            height=120 * len(traces) + 100,
            margin=dict(l=40, r=20, t=40, b=40),
            showlegend=False
        )
        for i in range(1, len(traces) + 1):
            fig2.update_xaxes(gridcolor="#1e3a5f", row=i, col=1)
            fig2.update_yaxes(gridcolor="#1e3a5f", row=i, col=1)

        st.plotly_chart(fig2, use_container_width=True)

# ── Recent alerts ──────────────────────────────────────────────────────────────
st.subheader("🚨 Recent Alerts")
if not pred_df.empty:
    recent_anomalies = pred_df[pred_df["is_anomaly"]].tail(10)
    if recent_anomalies.empty:
        st.markdown('<div class="normal-box">✅ No anomalies detected in the current window.</div>', unsafe_allow_html=True)
    else:
        for _, row in recent_anomalies.iloc[::-1].iterrows():
            ts    = row["timestamp"].strftime("%Y-%m-%d %H:%M:%S") if hasattr(row["timestamp"], "strftime") else row["timestamp"]
            score = row.get("anomaly_score", 0)
            hint  = row.get("fault_hint", "UNKNOWN")
            st.markdown(
                f'<div class="alert-box">🔴 <strong>{ts}</strong> — Score: <strong>{score:.2f}</strong> — Fault: <strong>{hint}</strong></div>',
                unsafe_allow_html=True
            )

# ── Fault distribution pie ─────────────────────────────────────────────────────
if not pred_df.empty and "fault_hint" in pred_df.columns:
    anomaly_subset = pred_df[pred_df["is_anomaly"]]
    if not anomaly_subset.empty:
        st.subheader("🗂️ Fault Type Distribution")
        fault_counts = anomaly_subset["fault_hint"].value_counts().reset_index()
        fault_counts.columns = ["Fault Type", "Count"]
        fig3 = px.pie(fault_counts, values="Count", names="Fault Type",
                      color_discrete_sequence=["#4fc3f7", "#ff5252", "#ffb74d", "#81c784", "#ce93d8"])
        fig3.update_layout(
            paper_bgcolor="#0a0f1e", font=dict(color="#e0e6f0"),
            height=350, margin=dict(l=20, r=20, t=20, b=20)
        )
        st.plotly_chart(fig3, use_container_width=True)

# ── Auto-refresh ───────────────────────────────────────────────────────────────
if auto_refresh:
    time.sleep(30)
    st.rerun()

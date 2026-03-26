import streamlit as st
import plotly.express as px
import pandas as pd
import os
import sys

# ---------------------------------------------------
# PATH SETUP
# ---------------------------------------------------
current_script_path = os.path.dirname(os.path.abspath(__file__))

# Project root = two levels up from dashboard
project_root = os.path.abspath(os.path.join(current_script_path, "..", ".."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from backend.pipeline.processing import process_pipeline
from backend.anamoly.detection import detect_anamoly

# ---------------------------------------------------
# PAGE CONFIG
# ---------------------------------------------------
st.set_page_config(
    page_title="Log Analytics Engine",
    layout="wide"
)

st.title("Python Based High Throughput Log Analytics Monitoring Engine")

# ---------------------------------------------------
# SIDEBAR
# ---------------------------------------------------
st.sidebar.header("Settings")
log_file_path = st.sidebar.text_input(
    "Log File Path",
    value=r"C:\Users\Dell\Downloads\log-analytics-monitoring-engine\backend\sample_data\log_data.log"
)

if st.sidebar.button("Refresh Dashboard"):
    st.rerun()

# ---------------------------------------------------
# LOAD DATA
# ---------------------------------------------------
try:
    log_df_dask = process_pipeline(log_file_path)
    log_data = log_df_dask.compute()

    # anomaly detection
    anamoly_result = detect_anamoly(log_df_dask, threshold=1.0)
    anamoly_df = anamoly_result.compute() if hasattr(anamoly_result, "compute") else anamoly_result

    # ---------------------------------------------------
    # PIE CHART
    # ---------------------------------------------------
    st.subheader("Log Level Distribution")

    level_counts = log_data["level"].value_counts().reset_index()
    level_counts.columns = ["level", "count"]

    pie_chart = px.pie(
        level_counts,
        names="level",
        values="count",
        title="Distribution of Log Levels"
    )

    st.plotly_chart(pie_chart, width="stretch")

    # ---------------------------------------------------
    # FILTER LEVELS
    # ---------------------------------------------------
    error_df = log_data[log_data["level"] == "ERROR"].copy()
    info_df  = log_data[log_data["level"] == "INFO"].copy()
    warn_df  = log_data[log_data["level"] == "WARN"].copy()

    # ---------------------------------------------------
    # THREE TIMELINE GRAPHS
    # ---------------------------------------------------
    col1, col2, col3 = st.columns(3)

    with col1:
        st.subheader("ERROR Logs Over Time")
        if not error_df.empty:
            fig_error = px.histogram(error_df, x="timestamp", title="ERROR Timeline")
            st.plotly_chart(fig_error, width="stretch")
        else:
            st.info("No ERROR logs")

    with col2:
        st.subheader("INFO Logs Over Time")
        if not info_df.empty:
            fig_info = px.histogram(info_df, x="timestamp", title="INFO Timeline")
            st.plotly_chart(fig_info, width="stretch")
        else:
            st.info("No INFO logs")

    with col3:
        st.subheader("WARN Logs Over Time")
        if not warn_df.empty:
            fig_warn = px.histogram(warn_df, x="timestamp", title="WARN Timeline")
            st.plotly_chart(fig_warn, width="stretch")
        else:
            st.info("No WARN logs")

    # ---------------------------------------------------
    # ERROR TREND PER MINUTE
    # ---------------------------------------------------
    st.subheader("Error Count Per Minute")

    if not error_df.empty:
        error_df["timestamp"] = pd.to_datetime(error_df["timestamp"])

        error_trend = (
            error_df
            .set_index("timestamp")
            .resample("1min")
            .size()
            .reset_index(name="error_count")
        )

        line_chart = px.line(
            error_trend,
            x="timestamp",
            y="error_count",
            title="Error Frequency Per Minute",
            markers=True
        )

        st.plotly_chart(line_chart, width="stretch")
    else:
        st.info("No error data available for trend analysis")

    # ---------------------------------------------------
    # ANOMALY GRAPH
    # ---------------------------------------------------
    st.subheader("Anomaly Detection Graph")

    if not anamoly_df.empty:
        fig_anomaly = px.line(
            anamoly_df,
            x="minute",
            y="error_count",
            title="Detected Anomalies (Error Count per Minute)",
            markers=True
        )
        st.plotly_chart(fig_anomaly, width="stretch")
    else:
        st.info("No anomaly graph to display (No anomalies found)")

    # ---------------------------------------------------
    # ANOMALY TABLE
    # ---------------------------------------------------
    st.subheader("Detected Anomalies")

    if anamoly_df.empty:
        st.success("No anomalies detected")
    else:
        st.warning(f"🚨 {len(anamoly_df)} anomalies detected!")
        st.dataframe(anamoly_df, width="stretch")

except Exception as e:
    st.error(f"Error loading or processing data: {str(e)}")
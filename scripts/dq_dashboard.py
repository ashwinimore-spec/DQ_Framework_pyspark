import pandas as pd
import streamlit as st
from pathlib import Path

st.set_page_config(page_title="Data Quality Dashboard", layout="wide")

st.title("📊 Data Quality Dashboard")

# -----------------------------
# Load data safely
# -----------------------------
def load_csv(folder):
    files = list(Path(folder).glob("*.csv"))
    return pd.read_csv(files[0]) if files else pd.DataFrame()

source_df = load_csv("data/raw_source_data.csv")
target_df = load_csv("data/target_data.csv")
defect_df = load_csv("data/dq_defect_report.csv")

# -----------------------------
# KPIs
# -----------------------------
col1, col2, col3, col4 = st.columns(4)

col1.metric("Source Rows", len(source_df))
col2.metric("Target Rows", len(target_df))
col3.metric("Total Defects", len(defect_df))
col4.metric(
    "Critical Defects",
    len(defect_df[defect_df["severity"] == "critical"]) if not defect_df.empty else 0
)

st.divider()

# -----------------------------
# Tabs
# -----------------------------
tab1, tab2, tab3, tab4 = st.tabs(
    ["🚨 Defects", "📄 Source Sample", "📄 Target Sample", "📊 Summary"]
)

# -----------------------------
# Defects Tab
# -----------------------------
with tab1:
    st.subheader("Full Defect Report")

    if defect_df.empty:
        st.success("No defects found 🎉")
    else:
        st.dataframe(defect_df, use_container_width=True)

        st.download_button(
            "⬇ Download Defect CSV",
            defect_df.to_csv(index=False),
            "dq_defect_report.csv",
            "text/csv"
        )

# -----------------------------
# Source Sample
# -----------------------------
with tab2:
    st.subheader("Raw Source Data (Sample)")
    st.dataframe(source_df.head(10), use_container_width=True)

# -----------------------------
# Target Sample
# -----------------------------
with tab3:
    st.subheader("Target Data (Sample)")
    st.dataframe(target_df.head(10), use_container_width=True)

# -----------------------------
# Summary
# -----------------------------
with tab4:
    st.subheader("Defects by Type")
    if not defect_df.empty:
        st.dataframe(
            defect_df.groupby(["defect_type", "severity"])
            .size()
            .reset_index(name="count"),
            use_container_width=True
        )

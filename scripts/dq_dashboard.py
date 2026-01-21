import os
import pandas as pd
import streamlit as st

st.set_page_config(page_title="Data Quality Dashboard", layout="wide")
st.title("📊 Data Quality Dashboard")

DATA_PATH = "data"
DEFECT_DIR = os.path.join(DATA_PATH, "dq_defect_report.csv")
RAW_DIR = os.path.join(DATA_PATH, "raw_source_data.csv")
TARGET_DIR = os.path.join(DATA_PATH, "target_data.csv")

# ---------------------------
# Utility: read Spark CSV dir
# ---------------------------
def read_spark_csv(folder_path):
    if not os.path.exists(folder_path):
        return pd.DataFrame()

    files = [
        os.path.join(folder_path, f)
        for f in os.listdir(folder_path)
        if f.startswith("part-") and f.endswith(".csv")
    ]

    if not files:
        return pd.DataFrame()

    return pd.concat((pd.read_csv(f) for f in files), ignore_index=True)


# ---------------------------
# Load Data
# ---------------------------
defect_df = read_spark_csv(DEFECT_DIR)
source_df = read_spark_csv(RAW_DIR)
target_df = read_spark_csv(TARGET_DIR)

if defect_df.empty:
    st.error("❌ No defect data found. Run validation first.")
    st.stop()

# ---------------------------
# Metrics
# ---------------------------
col1, col2, col3, col4 = st.columns(4)

col1.metric("Source Rows", len(source_df))
col2.metric("Target Rows", len(target_df))
col3.metric("Total Defects", len(defect_df))
col4.metric(
    "Critical Defects",
    len(defect_df[defect_df["severity"] == "critical"])
)

st.divider()

# ---------------------------
# Tabs
# ---------------------------
tab1, tab2 = st.tabs(["🚨 Defects", "📊 Summary"])

with tab1:
    st.subheader("Full Defect Report (Record Level)")

    st.dataframe(
        defect_df.sort_values(
            ["severity", "customer_id"],
            ascending=[False, True]
        ),
        use_container_width=True
    )

    st.download_button(
        "⬇ Download Defect CSV",
        defect_df.to_csv(index=False),
        file_name="dq_defect_report.csv",
        mime="text/csv"
    )

with tab2:
    st.subheader("Defect Summary")

    summary = (
        defect_df
        .groupby(["defect_type", "severity"])
        .size()
        .reset_index(name="count")
    )

    st.dataframe(summary, use_container_width=True)

import streamlit as st
import pandas as pd
import os

st.set_page_config(
    page_title="Data Quality Dashboard",
    layout="wide"
)

st.title("📊 Data Quality Dashboard")

DATA_DIR = "data"


# --------------------------------------------------
# Utility: Safe CSV reader
# --------------------------------------------------
def read_csv_safe(folder_name):
    path = os.path.join(DATA_DIR, folder_name)
    if not os.path.exists(path):
        return pd.DataFrame()

    files = [f for f in os.listdir(path) if f.endswith(".csv")]
    if not files:
        return pd.DataFrame()

    return pd.read_csv(os.path.join(path, files[0]))


# --------------------------------------------------
# Load datasets (IMPORTANT CHANGE HERE)
# --------------------------------------------------
source_df = read_csv_safe("raw_source_data.csv")      # ✅ MAIN SOURCE
target_df = read_csv_safe("target_data.csv")
defect_df = read_csv_safe("dq_defect_report.csv")


# --------------------------------------------------
# KPI Section
# --------------------------------------------------
col1, col2, col3, col4, col5 = st.columns(5)

col1.metric("Source Rows", len(source_df))
col2.metric("Target Rows", len(target_df))
col3.metric("Total Defects", len(defect_df))

# Duplicate defects
if "defect_type" in defect_df.columns:
    dup_count = defect_df["defect_type"].str.contains(
        "DUPLICATE", case=False, na=False
    ).sum()
else:
    dup_count = 0

col4.metric("Duplicate Defects", dup_count)

# Null-related defects
if "issues" in defect_df.columns:
    null_count = defect_df["issues"].str.contains(
        "null", case=False, na=False
    ).sum()
else:
    null_count = 0

col5.metric("Null Issues", null_count)

st.divider()


# --------------------------------------------------
# Tabs
# --------------------------------------------------
tab1, tab2, tab3, tab4 = st.tabs(
    ["Defects", "Source Sample", "Target Sample", "Summary"]
)


# ---------------- Defects ----------------
with tab1:
    st.subheader("Full Defect Report")

    if defect_df.empty:
        st.info("No defects available.")
    else:
        st.dataframe(defect_df, use_container_width=True)

        st.download_button(
            "⬇ Download Defect CSV",
            defect_df.to_csv(index=False),
            "dq_defect_report.csv",
            mime="text/csv"
        )


# ---------------- Source ----------------
with tab2:
    st.subheader("Raw Source Sample (Before Cleaning)")
    if source_df.empty:
        st.warning("Raw source data not found.")
    else:
        st.dataframe(source_df, use_container_width=True)


# ---------------- Target ----------------
with tab3:
    st.subheader("Target Sample (After Transformations)")
    if target_df.empty:
        st.warning("Target data not found.")
    else:
        st.dataframe(target_df, use_container_width=True)


# ---------------- Summary ----------------
with tab4:
    st.subheader("Defect Summary")

    if defect_df.empty or "severity" not in defect_df.columns:
        st.info("No summary data available.")
    else:
        st.bar_chart(defect_df["severity"].value_counts())

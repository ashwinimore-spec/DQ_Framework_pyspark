import os
import pandas as pd

DATA_PATH = "data"
DEFECT_FILE = os.path.join(DATA_PATH, "dq_defect_report.csv")


def test_defect_report_exists():
    assert os.path.exists(DEFECT_FILE), "dq_defect_report.csv not found"


def test_defects_present():
    df = pd.read_csv(DEFECT_FILE)
    assert len(df) > 0, "No defects found in defect report"


def test_customer_id_present():
    df = pd.read_csv(DEFECT_FILE)
    assert "customer_id" in df.columns, "customer_id column missing"
    assert df["customer_id"].notnull().any(), "customer_id values missing"


def test_critical_defects_present():
    df = pd.read_csv(DEFECT_FILE)
    assert "severity" in df.columns
    assert (df["severity"] == "critical").any(), "No critical defects found"


def test_defect_description_present():
    df = pd.read_csv(DEFECT_FILE)
    assert "description" in df.columns
    assert df["description"].notnull().all(), "Some defect descriptions are empty"

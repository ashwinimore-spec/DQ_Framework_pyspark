from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, count

def validate_data_quality():
    spark = SparkSession.builder.appName("DQ Validation").getOrCreate()

    # -----------------------------
    # Read raw source & target
    # -----------------------------
    src = spark.read.option("header", True).option("inferSchema", True) \
        .csv("data/raw_source_data.csv")

    tgt = spark.read.option("header", True).option("inferSchema", True) \
        .csv("data/target_data.csv")

    defects = []

    # -----------------------------
    # 1. Duplicate PK in target
    # -----------------------------
    dup_df = tgt.groupBy("customer_id").agg(count("*").alias("cnt")) \
        .filter(col("cnt") > 1)

    for row in dup_df.collect():
        defects.append((
            row["customer_id"],
            "DUPLICATE_PK",
            "Duplicate customer_id found in target",
            "critical"
        ))

    # -----------------------------
    # 2. Missing records in target
    # -----------------------------
    missing_df = src.select("customer_id").subtract(tgt.select("customer_id"))

    for row in missing_df.collect():
        defects.append((
            row["customer_id"],
            "MISSING_RECORD",
            "Record present in source but missing in target",
            "high"
        ))

    # -----------------------------
    # 3. Extra records in target
    # -----------------------------
    extra_df = tgt.select("customer_id").subtract(src.select("customer_id"))

    for row in extra_df.collect():
        defects.append((
            row["customer_id"],
            "EXTRA_RECORD",
            "Extra record found in target not present in source",
            "high"
        ))

    # -----------------------------
    # 4. Mandatory field checks
    # -----------------------------
    mandatory_cols = ["name", "age", "email"]

    for col_name in mandatory_cols:
        bad_rows = tgt.filter(
            col(col_name).isNull() | (trim(col(col_name)) == "")
        ).select("customer_id").distinct()

        for row in bad_rows.collect():
            defects.append((
                row["customer_id"],
                "MANDATORY_FIELD_MISSING",
                f"Null or blank value found in column: {col_name}",
                "high"
            ))

    # -----------------------------
    # 5. Age business rule
    # -----------------------------
    age_violations = tgt.filter(
        (col("age") < 18) | (col("age") > 60)
    ).select("customer_id")

    for row in age_violations.collect():
        defects.append((
            row["customer_id"],
            "AGE_RULE_VIOLATION",
            "Age value outside allowed range (18–60)",
            "high"
        ))

    # -----------------------------
    # 6. Email rule
    # -----------------------------
    email_issues = tgt.filter(
        col("email").isNull() | (trim(col("email")) == "")
    ).select("customer_id")

    for row in email_issues.collect():
        defects.append((
            row["customer_id"],
            "EMAIL_RULE_VIOLATION",
            "Email is null or blank in target",
            "medium"
        ))

    # -----------------------------
    # Create defect DataFrame
    # -----------------------------
    defect_columns = ["customer_id", "defect_type", "description", "severity"]
    defect_df = spark.createDataFrame(defects, defect_columns)

    defect_df.coalesce(1).write.mode("overwrite").option("header", True) \
        .csv("data/dq_defect_report.csv")

    print(f"DQ Validation completed. Total defects: {defect_df.count()}")

    spark.stop()

if __name__ == "__main__":
    validate_data_quality()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, trim, count

def validate_data_quality():
    spark = SparkSession.builder \
        .appName("Validate Data Quality") \
        .getOrCreate()

    expected_df = spark.read.option("header", True).option("inferSchema", True) \
        .csv("data/cleaned_source_data.csv")

    target_df = spark.read.option("header", True).option("inferSchema", True) \
        .csv("data/target_data.csv")

    defects = []

    # -------------------------------
    # 1. Record count validation
    # -------------------------------
    expected_count = expected_df.count()
    target_count = target_df.count()

    if expected_count != target_count:
        defects.append(("COUNT_MISMATCH", "Record count mismatch", "critical"))

    # -------------------------------
    # 2. Duplicate primary key check
    # -------------------------------
    dup_df = target_df.groupBy("customer_id").agg(count("*").alias("cnt")) \
        .filter(col("cnt") > 1)

    if dup_df.count() > 0:
        defects.append(("DUPLICATE_PK", "Duplicate customer_id in target", "critical"))

    # -------------------------------
    # 3. Mandatory field checks
    # -------------------------------
    mandatory_columns = ["customer_id", "name", "age", "email"]

    for column in mandatory_columns:
        null_count = target_df.filter(
            col(column).isNull() | (trim(col(column)) == "")
        ).count()

        if null_count > 0:
            defects.append((
                "MANDATORY_NULL",
                f"Null or blank values found in {column}",
                "high"
            ))

    # -------------------------------
    # 4. Age business rule
    # -------------------------------
    age_violation_count = target_df.filter(
        (col("age") < 18) | (col("age") > 60)
    ).count()

    if age_violation_count > 0:
        defects.append((
            "AGE_RULE_VIOLATION",
            "Age not in range 18 to 60",
            "high"
        ))

    # -------------------------------
    # 5. Email rule
    # -------------------------------
    email_violation_count = target_df.filter(
        col("email").isNull() | (trim(col("email")) == "")
    ).count()

    if email_violation_count > 0:
        defects.append((
            "EMAIL_RULE_VIOLATION",
            "Email is null or blank",
            "medium"
        ))

    # -------------------------------
    # Create defect report DataFrame
    # -------------------------------
    defect_schema = ["defect_type", "description", "severity"]

    defect_df = spark.createDataFrame(defects, defect_schema)

    defect_df.write.mode("overwrite").option("header", True) \
        .csv("data/dq_defect_report.csv")

    print("Data Quality validation completed. Defect report generated.")

    spark.stop()

if __name__ == "__main__":
    validate_data_quality()

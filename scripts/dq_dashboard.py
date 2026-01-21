from pyspark.sql import SparkSession
from pyspark.sql.functions import count

def dq_dashboard():
    spark = SparkSession.builder \
        .appName("Data Quality Dashboard") \
        .getOrCreate()

    defect_df = spark.read.option("header", True).option("inferSchema", True) \
        .csv("data/dq_defect_report.csv")

    print("\n==============================")
    print(" DATA QUALITY DASHBOARD ")
    print("==============================\n")

    # Total defects
    total_defects = defect_df.count()
    print(f"Total Defects Found: {total_defects}\n")

    # Defects by severity
    print("Defects by Severity:")
    defect_df.groupBy("severity").agg(count("*").alias("count")) \
        .show(truncate=False)

    # Defects by defect type
    print("Defects by Defect Type:")
    defect_df.groupBy("defect_type").agg(count("*").alias("count")) \
        .show(truncate=False)

    spark.stop()

if __name__ == "__main__":
    dq_dashboard()

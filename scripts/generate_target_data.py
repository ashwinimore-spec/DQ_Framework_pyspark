from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

def generate_target_data():
    spark = SparkSession.builder \
        .appName("Generate Buggy Target Data") \
        .getOrCreate()

    # Read expected (clean source) data
    expected_df = spark.read.option("header", True).option("inferSchema", True) \
        .csv("data/cleaned_source_data.csv")

    # Introduce developer bugs intentionally

    # 1. Age rule broken
    target_df = expected_df.withColumn(
        "age",
        col("age") + 15   # pushes some ages beyond 60
    )

    # 2. Email corruption
    target_df = target_df.withColumn(
        "email",
        lit(None)
    )

    # 3. Duplicate primary key
    dup_row = target_df.limit(1)
    target_df = target_df.union(dup_row)

    # 4. Missing mandatory column (salary)
    target_df = target_df.drop("salary")

    target_df.write.mode("overwrite").option("header", True) \
        .csv("data/target_data.csv")

    print("Buggy target data generated (developer defects simulated)")

    spark.stop()

if __name__ == "__main__":
    generate_target_data()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, when


def clean_source_data():
    spark = (
        SparkSession.builder
        .appName("Clean Source Data")
        .config("spark.hadoop.fs.permissions.enabled", "false")
        .getOrCreate()
    )

    raw_df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv("data/raw_source_data.csv")
    )

    print(f"Raw source record count: {raw_df.count()}")

    # --------------------------------------------------
    # Step 1: Fix mandatory fields instead of dropping
    # --------------------------------------------------

    clean_df = raw_df \
        .filter(col("customer_id").isNotNull()) \
        .filter(col("name").isNotNull())

    # Replace null / blank email with dummy value
    clean_df = clean_df.withColumn(
        "email",
        when(
            col("email").isNull() | (trim(col("email")) == ""),
            "unknown@email.com"
        ).otherwise(col("email"))
    )

    # Replace null age with default valid age
    clean_df = clean_df.withColumn(
        "age",
        when(col("age").isNull(), 30).otherwise(col("age"))
    )

    print(f"After mandatory field handling: {clean_df.count()}")

    # --------------------------------------------------
    # Step 2: Fix age instead of removing rows
    # --------------------------------------------------

    clean_df = clean_df.withColumn(
        "age",
        when(col("age") < 18, 18)
        .when(col("age") > 60, 60)
        .otherwise(col("age"))
    )

    print(f"After age correction: {clean_df.count()}")

    # --------------------------------------------------
    # Step 3: Deduplicate primary key
    # --------------------------------------------------

    clean_df = clean_df.dropDuplicates(["customer_id"])

    print(f"After removing duplicate customer_id: {clean_df.count()}")

    # --------------------------------------------------
    # Write cleaned source (EXPECTED DATASET)
    # --------------------------------------------------

    clean_df.write \
        .mode("overwrite") \
        .option("header", True) \
        .csv("data/cleaned_source_data.csv")

    print("Cleaned source data created (expected dataset)")

    spark.stop()


if __name__ == "__main__":
    clean_source_data()

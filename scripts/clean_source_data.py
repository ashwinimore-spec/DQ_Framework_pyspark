from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim

spark = SparkSession.builder.master("local[1]").appName("clean").getOrCreate()

df = spark.read.option("header", True).csv("data/raw_source_data.csv")

df = df.filter(
    col("customer_id").isNotNull() &
    col("name").isNotNull() &
    col("age").isNotNull() &
    col("email").isNotNull() &
    (trim(col("email")) != "")
)

df = df.filter((col("age") >= 18) & (col("age") <= 60))
df = df.dropDuplicates(["customer_id"])

df.coalesce(1).write.mode("overwrite").option("header", True) \
    .csv("data/cleaned_source_data.csv")

print("✔ cleaned_source_data.csv created")

spark.stop()

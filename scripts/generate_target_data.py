from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder.master("local[1]").appName("target").getOrCreate()

schema = StructType([
    StructField("customer_id", IntegerType()),
    StructField("name", StringType()),
    StructField("age", IntegerType()),
    StructField("email", StringType()),
    StructField("salary", IntegerType()),
])

data = [
    (101, "JOHN", 25, "john@example.com", None),
    (102, "Alice", 17, "alice@example.com", 60000),
    (103, "Bob", 65, None, 70000),
    (104, "Mike", 30, "mike@example.com", 40000),
    (104, "Mike", 30, "mike@example.com", 40000),
    (999, "Ghost", 45, "ghost@example.com", 90000),
]

df = spark.createDataFrame(data, schema)

df.coalesce(1).write.mode("overwrite").option("header", True) \
    .csv("data/target_data.csv")

print("✔ target_data.csv created")

spark.stop()

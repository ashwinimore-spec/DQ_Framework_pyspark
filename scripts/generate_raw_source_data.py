from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = (
    SparkSession.builder
    .appName("raw_source")
    .master("local[1]")
    .config("spark.sql.shuffle.partitions", "1")
    .config("spark.hadoop.security.authentication", "simple")
    .getOrCreate()
)

schema = StructType([
    StructField("customer_id", IntegerType()),
    StructField("name", StringType()),
    StructField("age", IntegerType()),
    StructField("email", StringType()),
    StructField("salary", IntegerType()),
])

data = [
    (101, "John", 25, "john@example.com", 50000),
    (102, "Alice", 17, "alice@example.com", 60000),
    (103, "Bob", 65, "bob@example.com", 70000),
    (104, None, 30, "mike@example.com", 40000),
    (105, "Sara", 28, None, 55000),
    (106, "Tom", 40, "", 65000),
    (107, "Eva", None, "eva@example.com", 72000),
    (101, "John Dup", 25, "john@example.com", 50000),
    (108, "Raj", 35, "raj@example.com", 58000),
    (109, "Neha", 22, "neha@example.com", 48000),
]

df = spark.createDataFrame(data, schema)

df.coalesce(1).write.mode("overwrite").option("header", True) \
    .csv("data/raw_source_data.csv")

print("✔ raw_source_data.csv created")

spark.stop()

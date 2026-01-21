from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

spark = SparkSession.builder.master("local[1]").appName("dq").getOrCreate()

src = spark.read.option("header", True).csv("data/cleaned_source_data.csv")
tgt = spark.read.option("header", True).csv("data/target_data.csv")

defects = []

if src.count() != tgt.count():
    defects.append((None, "COUNT_MISMATCH", "Source vs Target count mismatch", "critical"))

dup = tgt.groupBy("customer_id").agg(count("*").alias("c")).filter(col("c") > 1)
for r in dup.collect():
    defects.append((r["customer_id"], "DUPLICATE_PK", "Duplicate PK in target", "critical"))

missing = src.select("customer_id").subtract(tgt.select("customer_id"))
for r in missing.collect():
    defects.append((r["customer_id"], "MISSING_RECORD", "Missing in target", "high"))

bad_age = tgt.filter((col("age") < 18) | (col("age") > 60))
for r in bad_age.collect():
    defects.append((r["customer_id"], "AGE_RULE", "Age out of range", "high"))

df = spark.createDataFrame(
    defects,
    ["customer_id", "defect_type", "description", "severity"]
)

df.coalesce(1).write.mode("overwrite").option("header", True) \
    .csv("data/dq_defect_report.csv")

print("✔ dq_defect_report.csv created")

spark.stop()

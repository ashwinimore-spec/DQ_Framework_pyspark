from pyspark.sql import SparkSession

def generate_raw_source_data():
    spark = SparkSession.builder.appName("Generate Raw Source Data").getOrCreate()

    data = [
        (101, "John", 25, "john@example.com", 50000, "2023-10-01"),
        (102, "Alice", 17, "alice@example.com", 60000, "2023-10-02"),
        (103, "Bob", 65, "bob@example.com", 70000, "2023-10-03"),
        (104, None, 30, "mike@example.com", None, "2023-10-04"),
        (105, "Sara", 28, None, 55000, None),
        (106, "Tom", 40, "", 65000, "2023-10-06"),
        (101, "John Duplicate", 25, "john@example.com", 50000, "2023-10-01"),
        (107, "Eva", None, "eva@example.com", 72000, "2023-10-07"),
        (108, "Raj", 35, "raj@example.com", 58000, "2023-10-08"),
        (109, "Neha", 22, "neha@example.com", 48000, "2023-10-09"),
        (110, "Amit", 45, "amit@example.com", 80000, "2023-10-10"),
    ]

    columns = ["customer_id", "name", "age", "email", "salary", "last_updated"]
    df = spark.createDataFrame(data, columns)

    df.coalesce(1).write.mode("overwrite").option("header", True).csv("data/raw_source_data.csv")

    print("Raw source data generated")

    spark.stop()

if __name__ == "__main__":
    generate_raw_source_data()

from pyspark.sql import SparkSession

def generate_target_data():
    spark = SparkSession.builder.appName("Generate Target Data").getOrCreate()

    data = [
        (101, "JOHN", 70, None, 50000),
        (102, "Alice", 17, "alice@example.com", 60000),
        (103, "Bob", 65, "bob@example.com", None),
        (104, None, 30, "mike@example.com", None),
        (101, "JOHN DUP", 25, "john@example.com", 50000),
        (999, "Ghost", 40, "ghost@example.com", 40000)
    ]

    columns = ["customer_id", "name", "age", "email", "salary"]
    df = spark.createDataFrame(data, columns)

    df.coalesce(1).write.mode("overwrite").option("header", True).csv("data/target_data.csv")

    print("Buggy target data generated")

    spark.stop()

if __name__ == "__main__":
    generate_target_data()

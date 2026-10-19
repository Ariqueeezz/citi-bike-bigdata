from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("CitiBike-HDFS-CSV-Test")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

path = "hdfs://namenode:9000/raw/2026/01/*.csv"

try:
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .option("mode", "FAILFAST")
        .csv(path)
    )

    print("\n=== KOLOM DATASET ===")
    print(df.columns)

    print("\n=== JUMLAH BARIS ===")
    print(df.count())

    print("\n=== CONTOH DATA ===")
    df.show(5, truncate=False)

    print("HDFS_CSV_TEST=SUCCESS")

finally:
    spark.stop()

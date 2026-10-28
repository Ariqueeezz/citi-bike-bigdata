from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("CitiBike-HDFS-Integration-Test")
    .getOrCreate()
)

sc = spark.sparkContext

path = "hdfs://namenode:9000/test/hdfs-test.txt"

try:
    lines = sc.textFile(path).collect()
    print("=== HASIL BACA HDFS ===")
    print("\n".join(lines))
    print("HDFS_READ_TEST=SUCCESS")
except Exception as exc:
    print("HDFS_READ_TEST=FAILED")
    print(repr(exc))
    raise
finally:
    spark.stop()

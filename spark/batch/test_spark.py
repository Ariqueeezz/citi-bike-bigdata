from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("CitiBike-Spark-Test")
    .master("spark://spark-master:7077")
    .getOrCreate()
)

data = [
    ("member", 10),
    ("member", 15),
    ("casual", 20),
    ("casual", 25),
]

df = spark.createDataFrame(
    data,
    ["member_type", "duration"]
)

print("=== DATA ===")
df.show()

print("=== SUMMARY ===")
df.groupBy("member_type").sum("duration").show()

spark.stop()

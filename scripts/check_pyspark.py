"""Test PySpark — uruchamiaj w kontenerze (spark-submit), nie na gołym Windows."""

from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.master("local[*]")
    .appName("lab9-check")
    .getOrCreate()
)

df = spark.range(10)
print("OK — wierszy:", df.count())
df.show()

spark.stop()

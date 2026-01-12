# Import PySpark
from pyspark.sql import SparkSession

# Create a SparkSession
spark = (
    SparkSession.builder.appName("MySparkApplication")
    .config("spark.executor.memory", "2g")
    .config("spark.sql.schuffle.partitions", "4")
    .getOrCreate()
)

spark

# Shut down the current active SparkSession
spark.stop()

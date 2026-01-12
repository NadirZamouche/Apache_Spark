# Import PySpark
from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("MySparkApplication").getOrCreate()

# Create a SparkContext object
sc = spark.sparkContext
sc

# Shut down the current active SparkSession
sc.stop()  # or spark.stop()

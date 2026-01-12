"""Parquet files are columnar storage format which are highly optimized for high analytics workloads"""

from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("DataFrame-DEMO").getOrCreate()

"""Saving dataframe as parquet"""
df = spark.read.csv("./data/products.csv", header=True, inferSchema=True)
# desplaying data type of each column
df.printSchema()
# Display 5 rows
df.show(5)
# saving the dataframe as parquet file
df.write.parquet("./data/products.parquet")

"""Reading parquet to dataframe"""
df = spark.read.parquet("./data/products.parquet")
# desplaying data type of each column
df.printSchema()
# Display 5 rows
df.show(5)

spark.stop()

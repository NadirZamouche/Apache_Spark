# Import PySpark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc

# Create a SparkSession
spark = SparkSession.builder.appName("DataFrame-Operations").getOrCreate()

"""Dataframe from CSV file"""
# Create a dataframe from a text file
df = spark.read.csv("./data/stocks.txt", header=True, inferSchema=True)
# desplaying data type of each column
df.printSchema()
# Display 5 rows
df.show(5)

"""Select: Choose specific columns."""
df.select("id", "name", "category").show(5)

"""Filter: Apply conditions to filter rows."""
print("Unfiltered data count: ", df.count())
print("Filtered data count: ", df.filter(df.quantity > 20).count())
df.filter(df.quantity > 20).show(5)

"""GrouBy: Group data based on specific columns."""
"""Aggregations: Perform functions like sum, average, etc., on grouped data."""
df.groupBy("category").agg({"quantity": "sum", "price": "avg"}).show(5)

"""Join: Combine multiple DataFrames based on specific columns."""
df2 = df.select("id", "category").limit(6)
df3 = df.join(df2, "id", "inner")
df3.show()

"""Sort: Arrange rows based on one or more columns."""
df.orderBy("price").show(5)
# or soprt by a column desc
df.orderBy(col("price").desc(), col("id").desc()).show(5)

"""Distinct: Get unique rows."""
df.select("category").distinct().show(5)

"""Drop: Remove specified columns."""
df.drop("category", "quantity").show(5)

"""withColumn: Add new calculated columns."""
df.withColumn("revenue", df.quantity * df.price).show(5)

"""alias: Rename columns for better readablity."""
df.withColumnRenamed("price", "product_price").show(5)

spark.stop()

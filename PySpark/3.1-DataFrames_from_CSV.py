# Import PySpark
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
)

# Create a SparkSession
spark = SparkSession.builder.appName("DataFrame-DEMO").getOrCreate()

"""Dataframe from CSV file"""
# Create a dataframe from a text file
df = spark.read.csv("./data/products.csv", header=True)
# desplaying data type of each column
df.printSchema()
# Display 5 rows
df.show(5)

"""Manually defined schema"""
schema = StructType(
    [
        StructField(name="id", dataType=IntegerType(), nullable=True),
        StructField(name="name", dataType=StringType(), nullable=True),
        StructField(name="category", dataType=StringType(), nullable=True),
        StructField(name="quantity", dataType=IntegerType(), nullable=True),
        StructField(name="price", dataType=DoubleType(), nullable=True),
    ]
)

# Schema Enforcement
df = spark.read.csv("./data/products.csv", header=True, schema=schema)
# desplaying data type of each column
df.printSchema()
# Display 5 rows
df.show(5)

"""inferSchema (letting Spark guess the data type of each column based on row values of each column i suppose)"""
df = spark.read.csv("./data/products.csv", header=True, inferSchema=True)
# desplaying data type of each column
df.printSchema()
# Display 5 rows
df.show(5)

spark.stop()

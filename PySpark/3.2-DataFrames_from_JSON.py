"""JSON or Java Script Object Notation which is a popular semi-structured data format used to store data in a human-readable format"""

from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("DataFrame-DEMO").getOrCreate()

"""Single line JSON file"""
df = spark.read.json("./data/products_singleline.json")
# desplaying data type of each column
df.printSchema()
# Display 5 rows
df.show(5)

"""Multi line JSON file"""
df = spark.read.json("./data/products_multiline.json", multiLine=True)
# desplaying data type of each column
df.printSchema()
# Display 5 rows
df.show(5)

spark.stop()

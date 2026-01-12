# Import PySpark
from pyspark.sql import SparkSession
from pyspark.sql.functions import desc

# Create a SparkSession
spark = SparkSession.builder.appName("DataFrame-DEMO").getOrCreate()

"""RDD"""
# create an RDD from a text file
rdd = spark.sparkContext.textFile("./data/ApacheSpark.txt")
result_rdd = (
    rdd.flatMap(lambda line: line.split(" "))
    .map(lambda word: (word, 1))
    .reduceByKey(lambda a, b: a + b)
    .sortBy(lambda x: x[1], ascending=False)
)

result_rdd.take(10)

"""Dataframe"""
# Create a dataframe from a text file
df = spark.read.text("./data/ApacheSpark.txt")
df = (
    df.selectExpr("explode(split(value, ' ')) as word")
    .groupBy("word")
    .count()
    .orderBy(desc("count"))
)
df.take(10)

# Shut down the current active SparkSession
spark.stop()

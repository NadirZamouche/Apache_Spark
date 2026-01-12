# Import PySpark
from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("RDD-DEMO").getOrCreate()


# Create an RDD from a list of tuples
data = [("Alice", 25), ("Bob", 30), ("Charlie", 35), ("Alice", 40)]
rdd = spark.sparkContext.parallelize(data)

"""RDDs Operation: Actions----------------------------------------"""
# collect action: to retrieve all elements of the RDD
print("All elements of the rdd: ", rdd.collect())

# count Action: to count the number of elelemnts in the RDD
print("The total number of elements in the rdd: ", rdd.count())

# first action: to retrieve the first element of the RDD
print("The first element in the rdd: ", rdd.first())

# take action: to retrieve the first n elements of the RDD
print("The first two elements in the rdd: ", rdd.take(2))

# foreach action: to print each element of the RDD
rdd.foreach(lambda x: print(x))


"""RDDs Operation: Transformations----------------------------------------"""
# Remember transformations are lazy it means they won't execute until an action is called
# map transformation:convert  name to uppercase
mapped_rdd = rdd.map(lambda x: (x[0].upper(), x[1]))
print("rdd with uppercase name: ", mapped_rdd.collect())

# filter transformation: to filter records
filtered_rdd = rdd.filter(lambda x: x[1] > 30)
print("rdd with elements older than 30: ", filtered_rdd.collect())

# reduceByKey transformation: to do aggregation by same element
reduced_rdd = rdd.reduceByKey(lambda x, y: x + y)
print("aggregated rdd: ", reduced_rdd.collect())

# sortBy transformation: to sort an RDD
sorted_rdd = rdd.sortBy(lambda x: x[1], ascending=False)
print("aggregated rdd: ", sorted_rdd.collect())


"""Save/read RDDs from/to text file----------------------------------------"""
# save action: to save the RDD to a text file
rdd.saveAsTextFile(r"C:\Users\Nadir\Desktop\PySpark\RDD_output")

# create an RDD from a text file
rdd2 = spark.sparkContext.textFile("./RDD_output")
rdd2.collect()

# Shut down the current active SparkSession
spark.stop()

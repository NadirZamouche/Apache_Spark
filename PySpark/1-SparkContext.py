# Note: this won't work since my spark version is v3.5.1 and this one is for older version 1.x

from pyspark import SparkContext

# Create a SparkContext object
sc = SparkContext(appName="MySparkApplication")
sc

# Shut down the current active SparkContext
sc.stop()

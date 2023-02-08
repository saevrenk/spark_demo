import os

# Create SparkContext
from pyspark import SparkContext

sc = SparkContext("local", "Spark_Example_App")
print(sc.appName)

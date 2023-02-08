import findspark

findspark.init()

import pyspark
from pyspark import SparkContext, SparkConf

sc = SparkContext.getOrCreate()
# remember to change the path/location of file
airports = sc.textFile(
    "/Users/sae/vs_projects/spark_demo/spark_files/airports.txt.text"
)
print(airports.take(9))

sc.stop()

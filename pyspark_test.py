from pyspark import SparkContext
from pyspark.sql import Row, SparkSession

sc = SparkContext.getOrCreate()
# sc.setLogLevel(newLevel)

words = sc.parallelize (
   ["scala", 
   "java", 
   "hadoop", 
   "spark", 
   "akka",
   "spark vs hadoop", 
   "pyspark",
   "pyspark and spark"]
)

counts = words.count()
print ("Number of elements in RDD ->", counts)
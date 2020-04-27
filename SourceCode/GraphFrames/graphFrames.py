import os

import pyspark.sql.functions as f
from graphframes import *
from pyspark.sql import SparkSession


os.environ["SPARK_HOME"] = "C:\Installations\spark-2.4.5-bin-hadoop2.7"
os.environ["HADOOP_HOME"] = "C:\Installations\Hadoop"
os.environ["PYSPARK_SUBMIT_ARGS"]  = ("--packages  graphframes:graphframes:0.5.0-spark2.0-s_2.11 pyspark-shell")

spark = SparkSession \
    .builder \
    .appName("graphFrame") \
    .getOrCreate()

#1
#Reading two datasets directly into dataFrames
station_df = spark.read.csv(r"201508_station_data.csv", header=True)
trip_df = spark.read.csv(r"201508_trip_data.csv", header=True)
station_df.show()
trip_df.show()

#2
#concatenating two cols into one col separated by comma
concat_df = station_df.select("station_id", f.concat("lat", f.lit(","), "long"))
concat_df.show()

#3
#Dropping duplicates
station_df = station_df.drop_duplicates()
station_df.show()

#4&5
#renaming lat col to latitude and long col to longitude
station_df = station_df.withColumnRenamed("lat","latitude")
station_df = station_df.withColumnRenamed("long","longitude")
station_df.show()

#6
station_vertices = station_df.withColumnRenamed("name", "id").select("id").distinct()
station_vertices.show()

trip_edges = trip_df.withColumnRenamed("Start Station", "src").withColumnRenamed("End Station", "dst").select("src", "dst")
trip_edges.show()

gf = GraphFrame(station_vertices, trip_edges)

#7
#Display vertices
gf.vertices.show()

#8
#Display edges
gf.edges.show()

#9
#Display vertex in-degree
inDeg = gf.inDegrees
inDeg.show()

#10
#Display vertax out-degree
outDeg = gf.outDegrees
outDeg.show()

#11
gf.find("(a)-[b]->(e)").show()

#bonus

#1
gf.degrees.show()

#2
topTrips = gf \
  .edges \
  .groupBy("src", "dst") \
  .count() \
  .orderBy("count", ascending=False) \
  .limit(10)

topTrips.show()

#3
degreeRatio = inDeg.join(outDeg, inDeg.id == outDeg.id) \
.drop(outDeg.id) \
.withColumn("degreeRatio", (inDeg.inDegree/outDeg.outDegree)) \
.select("id", "degreeRatio")

degreeRatio.cache()

degreeRatio.orderBy("degreeRatio", ascending=False).limit(10).show()

#4
# Save vertices and edges as Parquet to some location.
gf.vertices.write.parquet("vertices")
gf.edges.write.parquet("edges")
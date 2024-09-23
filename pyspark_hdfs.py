# Import SparkSession

# import os
# os.environ['PYSPARK_SUBMIT_ARGS'] = """--name job_name --master local --conf spark.dynamicAllocation.enabled=true pyspark-shell"""

from pyspark.sql import SparkSession

#Create SparkSession

spark = SparkSession.builder\
                    .master("yarn")\
                    .appName("JOB DF READ")\
                    .getOrCreated() 

# path_hdfs="hdfs://namenode:9000/spark_data/toto.txt" 
#from pyspark.sql.types import *
df = spark.read.csv("hdfs://namenode:9000/raw_avocado/avocado_chunk1.csv", header=True)
df_final = df.withColumn("Day", F.split('Date', '-').getItem(2)).withColumn("month", F.split('Date', '-').getItem(1)).show()
#./spark-submit --driver-memory 1g --executor-memory 1g --executor-cores 1 /partage/xml_spark/read_hive.py
#./spark-submit --driver-memory 1g --executor-memory 1g --executor-cores 1 /partage/xml_spark/pyspark_hdfs.py

spark.stop()

#Pour executer la commande
#cd /spark/bin
#./spark-submit /partage/xml_spark/read

#(
    #df.write.format("org.neo4j.spark.DataSource")
    #.mode("Overwrite")
    #.option("labels", "Person")
    #.option("node.keys", "name,surname")
    #.save()
#)

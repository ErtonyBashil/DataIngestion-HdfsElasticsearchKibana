# Import SparkSession
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

#Create SparkSession

spark = SparkSession.builder \
	.master("yarn")\
	.appName("JOB DF READ")\
	.getOrCreated() 
	#from pyspark.sql.types import *

#TASK I: SPLIT THE DATA in 5 chunks files
path_file = "hdfs://namenode:9000/avocado.csv"
data = spark.read.csv(path_file, header=True)
chunks = [ 0.2, 0.2, 0.2, 0.2, 0.2]
data_ = data.randomSplit(chunks, seed=42)
#save_path_data = "hdfs:///raw_avocado/avocado"
for idx, chunk in enumerate(data_):
	chunk.coalesce(1).write.csv(f"hdfs:///raw_avocado/avocado_{idx+1}.csv", header=True)
     

#Get the data from the HDFS, clean an save in stagging directory

read_path = "hdfs://namenode:9000/raw_avocado/avocado_1.csv"
df = spark.read.csv(read_path, header=True)
df_ = df.withColumn("Day", F.split('Date', '-').getItem(2)).withColumn("month", F.split('Date', '-').getItem(1))
save_path = "hdfs://namenode:9000/stagging_avocado/avocado_cleaned_1.csv"
df_.write.save(save_path, format='csv', mode = 'append', header=True)

# Delete the file 

#Get system file in hdfs
hdp = spark._jsc.hadoopConfiguration()
filesys = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hdp)

# Delete the raw data in the directory
path = spark._jvm.org.apache.hadoop.fs.Path(read_path)
filesys.delete(path, True)

##TASK 3: READ THE CLEANED DATA IN stagging directory 

#Read the cleaned file from hdfs

df_ = spark.read.option("header", True).option("inferSchema", "true").csv(r"/stagging_avocado/avocado_cleaned_1.csv")

#Index the file in ELasticserch 
df.write.format("org.elasticsearch.spark.sql").option("es.net.http.auth.user", "ertony").option("es.net.http.auth.pass", "bashil").option("es.nodes", "http://elasticsearch:9200").option("es.nodes.discovery", "false").option("es.nodes.wan.only", "true").option("es.index.auto.create", "true").mode("append").save("avocado_sales")



# Move the file indexed in cleaned avocado
path ="/test/salle01.csv"
path_ = "data/"
p1 = spark._jvm.org.apache.hadoop.fs.Path(path)
p2 = spark._jvm.org.apache.hadoop.fs.Path(path_)

filesys.mv(p1, p2,True )

p1 = spark._jvm.org.apache.hadoop.fs.Path(path)
p2 = spark._jvm.org.apache.hadoop.fs.Path(path_)

p2 = "hdfs://namenode:9000/data/"




# ==============================================
val file_target = new Path("toLocation")
fs.mkdirs(file_target)
fs.rename(file_source, file_target)


spark.stop()

#Pour executer la commande
#cd /spark/bin
#./spark-submit /partage/xml_spark/read



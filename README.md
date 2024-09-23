# Data Ingestion HDFS SPARK Elasticsearch Kibana

Ce projet vise à créer un cluster connectant HDFS, SPARK, ELASTICSEARCH et KIBANA pour permettre le stockage,
la transformation et la visualisation de données. Notre cluster utilise Hadoop HDFS comme couche de stockage 
pour les grands ensembles de données et Apache Spark comme environnement de programmation et d'exécution pour
le calcul sur des grands ensembles de données, elasticsearch intervient pour l’indexation des document et KIBANA 
nous aider à créer des graphes pour la visualisation. Pour la plupart des tâches Spark nous effectueront des calculs sur de grands ensembles de données.  L'objectif est de fournir un pipeline robuste pour le stockage,
l'analyse et la visualisation de données.

## I. HDFS 
     
### Se conecter sur HDFS


- Créer les répertoires HDFS ‘/raw_avocado’, ‘/stagging_avocado’ , ‘/cleaned_avocado’ 

```python
docker exec -it namenode bash

hdfs dfs -ls
hdfs dfs -mkdir raw avocado, stagging_avocado, /cleaned_avocado
hdfs dfs -ls /
```

Scinder le fichier avocado.csv en 5 fichiers (avocado_1.csv, avocado_2.csv, avocado_3.csv, 
avocado_4.csv, avocado_5.csv) avec un script python

```python
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

```


    
## II. SPARK
    
Écrire un script PYSPARK qui récupère le fichier csv dans ‘/raw_avocado’ et pour chaque ligne du 
fichier, rajoute les colonnes 'jour' et 'mois' qui seront des extractions du champ 'date'. Ensuite 
sauvegardez le nouveau résultat dans un fichier csv (avocado_cleaned_1.csv, ...) et le stocker dans 
‘/stagging_avocado’, ensuite supprimer le fichier ('avocado_1.csv', ...) du répertoire ‘/raw_avocado’ - 
Écrire un script qui récupère le fichier (avocado_cleaned_1.csv, ...) csv du répertoire 
‘/stagging_avocado’ et l'indexe dans ELASTICSEARCH dans un indexe nommé 'avocado' ensuite le 
déplace le fichier csv dans le répertoire ‘/cleaned_avocado’

### Création d'une session spark

```python
./pyspark --master=yarn --driver-memory 1g --executor-memory 1g --executor-cores 1 --num-executors 2

```
### Lire le fichier 

```python
import pyspark.sql.functions as F
path_data = "hdfs://namenode:900/raw_avocado/avocado.csv"
df = spark.read.csv(path_data, header = True)
df.show()
```

### Pre-processing

- Prendre la colonne Anné" splitter et ajouter la colonne mois et jours

```python
path_data = "hdfs://namenode:9000/raw_avocado/avocado_1.csv"
df = spark.read.csv(path_data, header=True)
df = df.withColumn("Day", F.split('Date', '-').getItem(2)).withColumn("month", F.split('Date', '-').getItem(1))
```

### Enregistré le dataframe

Une fois les transformations effectuées, nous devons enregistrer notre
dataset dans le répertoire Stagging-avocado dans HDFS
```python
save_path = "hdfs://namenode:9000/stagging_avocado/avocado_cleaned_1.csv"
df.write.save(save_path, format='csv', mode = 'append', header=True)

```
### Suppression du fichier brut

Une fois les données traitées et enregistrées dans HDFS nous supprimons 
le fichier brut dans le raw-avocado dans HDFS.

```python
hdp = spark._jsc.hadoopConfiguration()
filesys = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hdp)
delete_path = "hdfs://namenode:9000/raw_avocado/avocado_2.csv"
del_path = spark._jvm.org.apache.hadoop.fs.Path(delete_path)
filesys.delete(del_path, True)
```

```python
hdp = spark._jsc.hadoopConfiguration()
filesys = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hdp)
path_src = "hdfs://namenode:9000/stagging_avocado/avocado_cleaned_1.csv"
path_dest = "hdfs://namenode:9000/cleaned_avocado/avocado_cleaned_1.csv"
file_source = spark._jvm.org.apache.hadoop.fs.Path(path_src)
file_target = spark._jvm.org.apache.hadoop.fs.Path(path_dest)
filesys.rename(file_source, file_target)

hdp = spark._jsc.hadoopConfiguration()
filesys = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hdp)
delete_path = "hdfs://namenode:9000/raw_avocado/avocado_2.csv"
del_path = spark._jvm.org.apache.hadoop.fs.Path(delete_path)
filesys.delete(del_path, True)

```
![](logo_elasticSearch.png)

**Écrire un script qui récupère le fichier (avocado_cleaned_1.csv, ...) csv du répertoire 
‘/stagging_avocado’ et l'indexe dans ELASTICSEARCH dans un indexe nommé 'avocado' ensuite le 
déplace le fichier csv dans le répertoire ‘/cleaned_avocado’**

Elasticsearch permet à chaque document d'avoir ses propres métadonnées. Grâce aux différentes 
options de mappage, on peut personnaliser ces paramètres afin que leurs valeurs soient extraites de leur document d'appartenance. De plus, 
on peut même inclure/exclure les parties des données qui sont renvoyées à Elasticsearch. 

### Seconnecter à Elastic search

docker exec -it elasticsearch bash

```python
### Authentification des utilisateurs:
```
### Création de super user

```python
# Create user in Elastic seach
bin/elasticsearch-users useradd ertony -p bashil -r superuser

# List users 
bin/elasticsearch-users
```
En tant que super utilisateur, nous disposons des privilèges pour créer et gérer les index 

Nous allons maintenant insérer une donnée dans notre index , 
nous lui précisons le nom de notre index et le ID de l’observation


### Création des index sur ElasticSearch

L’objectif est de parvenir à indexer les données dans Elasticsearch. Nous allons créer notre index avocado. Ici nous précisons : 
-	Settings de l’index
-	Mapping parameters
-	Les noms des attributs
-	Les types des attributs
-	Le nombre de shards


```python
{ "index":{} }
{ "Date":"2015-04-10", "Volume":102021.49, "AveragePrice":1.4, 
"type":"conventional", "region":"Altanta","Day":"10","month":"04","year":"2015"}
```

```python
./pyspark --master yarn --driver-memory 1g --executor-memory 1g --executor-cores 1 --jars /partage/jars/elasticsearch-spark-30_2.12-8.12.2.jar
df = spark.read.option("header", True).option("inferSchema", "true").csv(r"/stagging_avocado/avocado_cleaned_1.csv")
df = spark.read.csv(path_data, header=True)
df.write.format("org.elasticsearch.spark.sql").option("es.net.http.auth.user", "ertony").option("es.net.http.auth.pass", "bashil").option("es.nodes", "http://elasticsearch:9200").option("es.nodes.discovery", "false").option("es.nodes.wan.only", "true").option("es.index.auto.create", "true").mode("append").save("avocado")

```

## KIBANA

KIBANNA est un outil qui permet de lancer des analyses à grande échelle pour bénéficier de l'observabilité,
cet outil nous aide à explorer et à visualiser nos données, nous nous réservons cette audace de citer la liste exhaustive des fonctionnalités de KiBANA. L’objectif étant de faire le monitoring idéal pour comprendre nos données,
nous allons donc créer des graphes et configurer la table de bord


Etape 1: 
Nous allons commencer par créer un tableau de bord, on va dans  >> Stack Management >>data view >> On crée le dashboard >>une visualisation
- Création de Dataview et Tableau de bord

Etape 2: Ingestion des données dans Elasticsearch

```python
df = spark.read.option("header", True).option("inferSchema", "true").csv(r"/stagging_avocado/avocado_cleaned_1.csv")
df = spark.read.csv(path_data, header=True)
df.write.format("org.elasticsearch.spark.sql").option("es.net.http.auth.user", "ertony").option("es.net.http.auth.pass", "bashil").option("es.nodes", "http://elasticsearch:9200").option("es.nodes.discovery", "false").option("es.nodes.wan.only", "true").option("es.index.auto.create", "true").mode("append").save("avocado")
```















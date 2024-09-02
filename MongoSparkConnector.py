from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.\
        builder.\
        appName("MongoSparkConnector").\
        config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.13:10.4.0').\
        getOrCreate()

connection_uri = 'mongodb+srv://paulacaires:paula@paula.d7teqh5.mongodb.net/?retryWrites=true&w=majority&appName=Paula'
database = "ReceitasDatabase"
collection = "Receitas"

query = (spark.readStream.format("mongodb").\
        option('spark.mongodb.connection.uri', connection_uri)
    	.option('spark.mongodb.database', database) \
    	.option('spark.mongodb.collection', collection) \
        .option('spark.mongodb.change.stream.publish.full.document.only','true') \
    	.option("forceDeleteTempCheckpointLocation", "true") \
    	.load())

# Dados para um data frame
df = spark.read.format("mongodb").option("database", database).option("spark.mongodb.connection.uri", connection_uri).option("collection", collection).load()

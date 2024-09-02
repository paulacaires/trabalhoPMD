from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.\
        builder.\
        appName("MongoSparkConnector").\
        config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.13:10.4.0').\
        getOrCreate()

query=(spark.readStream.format("mongodb")
.option('spark.mongodb.connection.uri', 'mongodb+srv://paulacaires:paula@paula.d7teqh5.mongodb.net/?retryWrites=true&w=majority&appName=Paula')
    	.option('spark.mongodb.database', 'ReceitasDatabase') \
    	.option('spark.mongodb.collection', 'Receitas') \
.option('spark.mongodb.change.stream.publish.full.document.only','true') \
    	.option("forceDeleteTempCheckpointLocation", "true") \
    	.load())

query.printSchema()

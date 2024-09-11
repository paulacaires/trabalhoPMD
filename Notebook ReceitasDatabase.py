# Databricks notebook source
# MAGIC %md
# MAGIC # Conexão com o Spark
# MAGIC
# MAGIC Instalamos a versão `org.mongodb.spark:mongo-spark-connector_2.12:3.0.1`.

# COMMAND ----------

connection_uri = 'mongodb+srv://paulacaires:paula@paula.d7teqh5.mongodb.net/?retryWrites=true&w=majority&appName=Paula'
database = "ReceitasDatabase"
collection = "Receitas"

df_receitas = spark.read.format("mongo").option("database", database).option("spark.mongodb.input.uri", connection_uri).option("collection",collection).load()

# COMMAND ----------

# MAGIC %md
# MAGIC # Explorando o o data set

# COMMAND ----------

# Os campos e seus respectivos tipos
df_receitas.show(1, vertical=True)

df_receitas.dtypes

# COMMAND ----------

# MAGIC %md
# MAGIC # Consulta das receitas sem campo
# MAGIC
# MAGIC Na coleção, dentre todos os campos existem aqueles de proteína (protein), sódio (sodium), calorias (calories), gordura (fat). No entanto, nem todas as receitas tem essas informações preenchidas, pois tem o valor "null". 
# MAGIC
# MAGIC Essa consulta tem como objetivo preencher esses campos com base nas informações das outras receitas que têm essa categoria.

# COMMAND ----------

df_receitas_nulo = df_receitas.filter(df_receitas.calories.isNull())

print(f"\nA coleção de receitas tem {df_receitas_nulo.count()} receitas com o campo de caloria igual a null. Exemplo:\n")
df_receitas_nulo.show(1, vertical=True)


# COMMAND ----------

# MAGIC %md
# MAGIC Para executar essa consulta, será muito útil ter um dataframe que tem todas as categorias 
# MAGIC e quais receitas que têm essas categorias. 
# MAGIC
# MAGIC Categorias é do tipo ArrayColumn, por isso usar a função explode.

# COMMAND ----------

import pyspark.sql.functions as F

# Cada categoria separada
df_categorias_receitas = df_receitas.withColumn('categories', F.explode(F.col('categories'))).\
                         select('_id', 'calories', 'fat', 'protein', 'sodium' ,'categories')

df_categorias_receitas.show(5, vertical=True)

# Agrupar 
df_categorias_agrupado = df_categorias_receitas.groupBy('categories') \
                                              .agg(F.collect_list('_id').alias('ids_das_receitas'),
                                                   F.count('*').alias('quantidade_de_receitas'),
                                                   F.avg('calories').alias('media_calorias'),
                                                   F.avg('fat').alias('media_gordura'),
                                                   F.avg('protein').alias('media_proteina'),
                                                   F.avg('sodium').alias('media_sodio'))

df_categorias_agrupado.show(vertical=True)

# COMMAND ----------

# MAGIC %md
# MAGIC Com todas as categorias e as receitas com aquela categoria, selecionar as categorias da receita alvo. 

# COMMAND ----------

# Pegar a primeira ocorrencia do nulo
row_receita_alvo = df_receitas_nulo.take(1)[0]

# Categorias da receita alvo
categorias_alvo = row_receita_alvo['categories']


# COMMAND ----------

# MAGIC %md
# MAGIC Selecionar todas as receitas que tem pelo menos uma categoria da receita alvo.

# COMMAND ----------

# De todas as categorias das receitas, aquelas que são do alvo
df_receitas_categoria_alvo = df_categorias_agrupado.filter(df_categorias_agrupado.categories.isin(categorias_alvo))


# COMMAND ----------

# MAGIC %md
# MAGIC Agora, calcular a média das informações de todas as receitas 

# COMMAND ----------

# Documentação: PySpark DataFrame is lazily evaluated and simply selecting a column does not trigger the computation but it returns a Column instance.

# Calorias
df_receitas_categoria_alvo.createOrReplaceTempView("tableInfos")

# O Collect é necessário para retornar o valor
resultado = spark.sql("SELECT ROUND(avg(media_calorias), 2) as media_calorias FROM tableInfos").collect()
media_calorias = resultado[0][0]
print(media_calorias)

# Gordura
df_receitas_categoria_alvo.createOrReplaceTempView("tableGordura")

resultado = spark.sql("SELECT ROUND(avg(media_gordura), 2) as media_gordura FROM tableInfos").collect()
media_gordura = resultado[0][0]
print(media_gordura)

# Proteina
df_receitas_categoria_alvo.createOrReplaceTempView("tableProteina")

resultado = spark.sql("SELECT ROUND(avg(media_proteina), 2) as media_proteina FROM tableInfos").collect()
media_proteina = resultado[0][0]
print(media_proteina)

# Sodio
df_receitas_categoria_alvo.createOrReplaceTempView("tableSodio")

resultado = spark.sql("SELECT ROUND(avg(media_sodio), 2) as media_sodio FROM tableInfos").collect()
media_sodio = resultado[0][0]
print(media_sodio)

# COMMAND ----------

# MAGIC %md
# MAGIC Fazer a mudança direto no dataframe

# COMMAND ----------

# Para buscar no dataframe original
titulo_alvo = df_receitas_nulo.select("title").first()[0]
df_filtrado = df_receitas.filter(df_receitas.title == titulo_alvo)

# Atualizando as colunas sem informações
df_atualizado = df_filtrado.withColumn("calories", F.when(df_receitas["title"] == titulo_alvo, media_calorias))
df_atualizado = df_atualizado.withColumn("sodium", F.when(df_receitas["title"] == titulo_alvo, media_sodio))
df_atualizado = df_atualizado.withColumn("protein", F.when(df_receitas["title"] == titulo_alvo, media_proteina))
df_atualizado = df_atualizado.withColumn("fat", F.when(df_receitas["title"] == titulo_alvo, media_gordura))

df_atualizado.show(vertical=True)


# COMMAND ----------

# MAGIC %md
# MAGIC Fazendo a escrita no Mongo

# COMMAND ----------

# Gravando o documento atualizado de volta no MongoDB
uri = "mongodb+srv://paulacaires:paula@paula.d7teqh5.mongodb.net/?retryWrites=true&w=majority&appName=Paula"


df_atualizado.write.format("mongo") \
    .mode("append") \
    .option("database", database) \
    .option("collection", collection) \
    .option("spark.mongodb.output.uri", uri) \
    .save()


# COMMAND ----------

# MAGIC %md
# MAGIC Mostrando como ficou o documento da receita

# COMMAND ----------

df_filtrado = df_receitas.filter(df_receitas.title == titulo_alvo)
df_filtrado.show(1, vertical=True)

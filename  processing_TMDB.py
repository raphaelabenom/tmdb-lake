# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### Step 1: Set the data location and type

# COMMAND ----------

# Env Scope
storage_account_name = "dfstoragelab"
storage_account_access_key = dbutils.secrets.get(scope="databricks", key="key1")

# Folder
file_location = "wasbs://raw@dfstoragelab.blob.core.windows.net/*.json"

# Config options
spark.conf.set(
  "fs.azure.account.key."+storage_account_name+".blob.core.windows.net",
  storage_account_access_key)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Step 2: Read the data

# COMMAND ----------

# Lendo o arquivo JSON no DataFrame do Spark
df = (spark.read
  .format("json")
  .option("header", "true")
  .option("inferSchema", "true")
  .load(file_location)
)

# Estrutura do JSON
# page:long
#   results:array
#     element:struct
#     adult:boolean
#     backdrop_path:string
#     genre_ids:array
#       element:long
#     id:long
#     original_language:string
#     original_title:string
#     overview:string
#     popularity:double
#     poster_path:string
#     release_date:string
#     title:string
#     video:boolean
#     vote_average:double
#     vote_count:long
# total_pages:long
# total_results:long

# COMMAND ----------

df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import *

# Explodindo o array 'results' em novas linhas
df = df.select("*", explode_outer(df.results).alias("result"))

# É uma boa prática usar explode_outer em vez de explode ao trabalhar com JSON aninhado. A função explode_outer difere da função 'explode' no sentido de que, se a coluna especificada for nula ou conter um array vazio, a função 'explode' excluirá a linha, enquanto a função explode_outer manterá a linha e retornará nulo para todos os outros campos.
#Então, ao trabalhar com JSON aninhado onde pode haver campos nulos ou arrays vazios, explode_outer geralmente é uma opção mais segura para garantir que você não perca linhas.
# REF: https://sparkbyexamples.com/spark/explode-spark-array-and-map-dataframe-column/?expand_article=1

# Acessando os campos dentro do objeto 'result'

df = df.select("page", 
              "total_pages", 
              "total_results", 
              "result.adult", 
              "result.backdrop_path", 
              "result.genre_ids", 
              "result.id", 
              "result.original_language", 
              "result.original_title", 
              "result.overview", 
              "result.popularity", 
              "result.poster_path", 
              "result.release_date", 
              "result.title", 
                "result.video", 
              "result.vote_average", 
              "result.vote_count")

df.display()

# COMMAND ----------

# Dicionário com todos os gêneros

from pyspark.sql import *

genres_dict = {
"genres":[
  {"id":28,"category":"Action"},
  {"id":12,"category":"Adventure"},
  {"id":16,"category":"Animation"},
  {"id":35,"category":"Comedy"},
  {"id":80,"category":"Crime"},
  {"id":99,"category":"Documentary"},
  {"id":18,"category":"Drama"},
  {"id":10751,"category":"Family"},
  {"id":14,"category":"Fantasy"},
  {"id":36,"category":"History"},
  {"id":27,"category":"Horror"},
  {"id":10402,"category":"Music"},
  {"id":9648,"category":"Mystery"},
  {"id":10749,"category":"Romance"},
  {"id":878,"category":"Science Fiction"},
  {"id":10770,"category":"TV Movie"},
  {"id":53,"category":"Thriller"},
  {"id":10752,"category":"War"},
  {"id":37,"category":"Western"}
    ]
  }

# Converter o dicionário em uma lista de Rows
genres_list = [Row(**x) for x in genres_dict["genres"]] # List Comprehension

# Criar o DataFrame
category_df = spark.createDataFrame(genres_list)

# Renomear a coluna do dataframe genres_df Tabela de categorias
category_df = category_df.withColumnRenamed("id", "id_cat")

category_df.display()

# COMMAND ----------

from pyspark.sql.functions import *

# "Explodir" a coluna genre_ids
exploded_genre_ids = df.select("*", explode_outer(df.genre_ids).alias("genre_id"))

# Join dos dataframes category_df, genre_id
joined_df = exploded_genre_ids.join(category_df, exploded_genre_ids.genre_id == category_df.id_cat, how='left_outer')

# Agrupe pelas colunas originais e coletar os nomes dos gêneros como uma lista
tmdb_df = (joined_df \
    .groupBy(df.columns) \
    .agg(collect_list("category").alias("genre_array")))

# Concatenar os nomes dos gêneros em uma única string, com concat_ws
tmdb_df = tmdb_df.withColumn("genre", concat_ws(", ", tmdb_df.genre_array)).drop("genre_array")

tmdb_df = tmdb_df.withColumn("genre", when(tmdb_df["genre"] == "", "empty").otherwise(tmdb_df["genre"]))

tmdb_df.display()

# COMMAND ----------

# TODO: Atache the full link to poster_path and backdrop_path

# Dropar colunas que não são necessárias
tmdb_df = tmdb_df.drop("page","total_pages","total_results", "adult", "video")

# Passar os valores do array do genre_ids para string fazendo o concat entre elas
tmdb_df = tmdb_df.withColumn("genre_ids", concat_ws(", ", tmdb_df.genre_ids))

# Concatenar o prefixo do link da imagem
def concat_prefix(df, prefix, column_name):
    return df.withColumn(column_name, concat(lit(prefix), col(column_name)))
  
tmdb_df = concat_prefix(tmdb_df, "www.themoviedb.org/t/p/w600_and_h900_bestv2", "backdrop_path")
tmdb_df = concat_prefix(tmdb_df, "www.themoviedb.org/t/p/w600_and_h900_bestv2", "poster_path")

#Tratar null values
def replace_null_value(df, column_name, replacement_value):
    return df.withColumn(column_name, when(col(column_name).isNull(), replacement_value).otherwise(col(column_name)))

tmdb_df = replace_null_value(tmdb_df, "backdrop_path", "empty")
tmdb_df = replace_null_value(tmdb_df, "poster_path", "empty")

tmdb_df = tmdb_df.withColumn("genre_ids", when(tmdb_df["genre_ids"] == "", "empty").otherwise(tmdb_df["genre_ids"]))
tmdb_df = tmdb_df.withColumn("overview", when(tmdb_df["overview"] == "", "empty").otherwise(tmdb_df["overview"]))

# Remover linhas duplicadas com base na coluna "id"
tmdb_df = tmdb_df.dropDuplicates(["id"])


tmdb_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Step 3: Query the data

# COMMAND ----------

# Encontrar e contar todos os valores que são None, NULL & Empty String Literal Values
missing_values = tmdb_df.select([count(when(col(column).contains('None') | \
                            col(column).contains('NULL') | \
                            (col(column) == '' ) | \
                            col(column).isNull() | \
                            isnan(column), column 
                           )).alias(column)
                    for column in tmdb_df.columns])

missing_values.display()

# COMMAND ----------

# Explorando os tipos de dados das colunas usando dtypes

for column_name, column_type in tmdb_df.dtypes:
    print(f"\n A coluna {column_name} é do tipo {column_type}")

# COMMAND ----------

for column_name, column_type in tmdb_df.dtypes:
  print(column_name)

# COMMAND ----------

tmdb_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Step 4: Create a view or table

# COMMAND ----------

tmdb_df.createOrReplaceTempView("tmdb")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT original_title
# MAGIC FROM tmdb
# MAGIC GROUP BY original_title
# MAGIC HAVING COUNT(*) > 1;
# MAGIC
# MAGIC
# MAGIC -- SELECT * FROM tmdb WHERE original_title = "Mangas";

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT DISTINCT id AS unique_id
# MAGIC FROM tmdb;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT genre, COUNT(*) AS COUNT 
# MAGIC FROM tmdb
# MAGIC GROUP BY ROLLUP(genre)
# MAGIC HAVING COUNT > 1
# MAGIC ORDER BY COUNT DESC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Creating a table and ingesting it into the Azure data lake

# COMMAND ----------

tmdb_df.write.format("parquet").saveAsTable("TABLE_TMDB")

# COMMAND ----------

# Exporting for Azure Datalake

container_name = 'dfstoragelab'
storage_name = 'bronze'
mount_name = '/mnt/bronze/' 
sas_key = dbutils.secrets.get(scope="databricks", key="sas1")

output_container_path = "wasbs://%s@%s.blob.core.windows.net" % (storage_name, container_name)
output_blob_folder = "%s/tmdb" % output_container_path

# gravar o dataframe como um único arquivo para armazenamento de blob

(tmdb_df
 .coalesce(1)
 .write
 .mode("overwrite")
 .option("header", "true")
 .format("csv")
 .save(output_blob_folder))

# # Obtenha o nome do arquivo CSV dos dados que acabou de ser salvo no armazenamento de blob do Azure (começa com 'part-')

files = dbutils.fs.ls(output_blob_folder)
output_file = [x for x in files if x.name.startswith("part-")]

dbutils.fs.mv(output_file[0].path, "%s/tmdb-transform-output.csv" % output_container_path)

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

dbutils.fs.unmount('/mnt/bronze/')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC This table will persist across cluster restarts and allow various users across different notebooks to query this data.

# COMMAND ----------

# df.write.saveAsTable("table_name")

# CREATE TABLE table_name AS SELECT * FROM another_table

# CREATE TABLE table_name (field_name1 INT, field_name2 STRING)

# df.write.option("path", "/path/to/empty/directory").saveAsTable("table_name")

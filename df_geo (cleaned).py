# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC SET spark.databricks.delta.formatCheck.enabled=false

# COMMAND ----------

file_location = "/mnt/mounted_bucket/topics/0e33e87dfa09.geo/partition=0/*.json" 
file_type = "json"
infer_schema = "true"
df_geo = spark.read.format(file_type).option("inferSchema", infer_schema).load(file_location)
display(df_geo)

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Restore the dataframe from this point:

# COMMAND ----------

df = df_geo

# COMMAND ----------

df = df.withColumn("ind", df["ind"].cast("int"))

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##Create a new column coordinates that contains an array based on the latitude and longitude columns

# COMMAND ----------

df = df.withColumn("latitude", df["latitude"].cast("float"))
df = df.withColumn("longitude", df["longitude"].cast("float"))

# COMMAND ----------

display(df.filter(col("latitude").isNull()))

# COMMAND ----------

df = df.withColumn("coordinates", array("latitude", "longitude"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Drop the latitude and longitude columns from the DataFrame
# MAGIC

# COMMAND ----------

df = df.drop("latitude", "longitude")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Convert the timestamp column from a string to a timestamp data type

# COMMAND ----------

df = df.withColumn("timestamp", to_timestamp("timestamp", "yyyy-MM-dd HH:mm:ss"))

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# Filter rows with null elements in the array
df.filter(expr("exists(coordinates, x -> x IS NULL)")).show()

# Count rows with null elements in the array
df.withColumn(
    "num_nulls",
    size(filter(df.coordinates, lambda x: x.isNull()))
).filter("num_nulls > 0").show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## REORDER THE DATAFRAME TO IND, COUNTRY, COORDINATES, TIMESTAMP

# COMMAND ----------

df = df.select("ind", "country", "coordinates", "timestamp")
df = df.orderBy("ind")

# COMMAND ----------

display(df)

# COMMAND ----------

df_length = df.count()
unique_col = df.select("ind").distinct().count()
print(df_length == unique_col)

# COMMAND ----------

df = df.dropDuplicates()

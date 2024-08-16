# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC SET spark.databricks.delta.formatCheck.enabled=false

# COMMAND ----------

file_location = "/mnt/mounted_bucket/topics/0e33e87dfa09.user/partition=0/*.json" 
file_type = "json"
infer_schema = "true"
df_user = spark.read.format(file_type).option("inferSchema", infer_schema).load(file_location)

display(df_user)

# COMMAND ----------

df = df_user

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df = df.dropDuplicates()

# COMMAND ----------

df = df.orderBy("ind")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Convert the date_joined column from a string to a timestamp data type

# COMMAND ----------

df = df.withColumn("date_joined", to_timestamp("date_joined", "yyyy-MM-dd HH:mm:ss"))

# COMMAND ----------

# MAGIC %md
# MAGIC * ## Create a new column user_name that concatenates the information found in the first_name and last_name columns
# MAGIC * ## Drop the first_name and last_name columns from the DataFrame

# COMMAND ----------

df = df.withColumn("user_name", concat("first_name", lit(" "), "last_name"))
df = df.drop("first_name", "last_name")
display(df)

# COMMAND ----------

df = df.select("ind", "user_name", "age", "date_joined")

# COMMAND ----------

display(df)

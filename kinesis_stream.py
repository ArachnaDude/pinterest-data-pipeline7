# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *
import urllib

# Define the path to the Delta table
delta_table_path = "dbfs:/user/hive/warehouse/authentication_credentials"

# Read the Delta table to a Spark DataFrame
aws_keys_df = spark.read.format("delta").load(delta_table_path)

# COMMAND ----------

# Get the AWS access key and secret key from the spark dataframe
ACCESS_KEY = aws_keys_df.select('Access key ID').collect()[0]['Access key ID']
SECRET_KEY = aws_keys_df.select('Secret access key').collect()[0]['Secret access key']
# Encode the secrete key
ENCODED_SECRET_KEY = urllib.parse.quote(string=SECRET_KEY, safe="")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Disable format checks during the reading of Delta tables
# MAGIC SET spark.databricks.delta.formatCheck.enabled=false

# COMMAND ----------

df_pin = spark \
.readStream \
.format('kinesis') \
.option('streamName','streaming-0e33e87dfa09-pin') \
.option('initialPosition','earliest') \
.option('region','us-east-1') \
.option('awsAccessKey', ACCESS_KEY) \
.option('awsSecretKey', SECRET_KEY) \
.load()

# COMMAND ----------

df_geo = spark \
.readStream \
.format('kinesis') \
.option('streamName','streaming-0e33e87dfa09-geo') \
.option('initialPosition','earliest') \
.option('region','us-east-1') \
.option('awsAccessKey', ACCESS_KEY) \
.option('awsSecretKey', SECRET_KEY) \
.load()

# COMMAND ----------

df_user = spark \
.readStream \
.format('kinesis') \
.option('streamName','streaming-0e33e87dfa09-user') \
.option('initialPosition','earliest') \
.option('region','us-east-1') \
.option('awsAccessKey', ACCESS_KEY) \
.option('awsSecretKey', SECRET_KEY) \
.load()

# COMMAND ----------

df_pin = df_pin.selectExpr("CAST(data as STRING)")
df_geo = df_geo.selectExpr("CAST(data as STRING)")
df_user = df_user.selectExpr("CAST(data as STRING)")

# COMMAND ----------

display(df_pin)

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

# MAGIC %md
# MAGIC ### schemas

# COMMAND ----------

pin_schema = StructType([
    StructField("index", IntegerType(), True),
    StructField("unique_id", StringType(), True),
    StructField("title", StringType(), True),
    StructField("description", StringType(), True),
    StructField("poster_name", StringType(), True),
    StructField("follower_count", StringType(), True),
    StructField("tag_list", StringType(), True),
    StructField("is_image_or_video", StringType(), True),
    StructField("image_src", StringType(), True),
    StructField("downloaded", StringType(), True),
    StructField("save_location", StringType(), True),
    StructField("category", StringType(), True)
])

# COMMAND ----------

geo_schema = StructType([
    StructField("ind", IntegerType(), True),
    StructField("latitude", FloatType(), True),
    StructField("longitude", FloatType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("country", StringType(), True)
])

# COMMAND ----------

user_schema = StructType({
    StructField("ind", IntegerType(), True),
    StructField("date_joined", TimestampType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("age", IntegerType(), True)
})

# COMMAND ----------

# MAGIC %md
# MAGIC ### df_pin cleaning

# COMMAND ----------

df_pin = df_pin.withColumn("parsed_data", from_json(col("data"), pin_schema))
df_pin = df_pin.select("parsed_data.*")

# COMMAND ----------

df_pin = df_pin.withColumn("description", when(col("description").rlike("(No description available|Untitled)"), None).otherwise(col("description")))
df_pin = df_pin.withColumn("follower_count", when(col("follower_count").rlike("User Info Error"), None).otherwise(col("follower_count")))
df_pin = df_pin.withColumn("image_src", when(col("image_src").rlike("Image src error."), None).otherwise(col("image_src")))
df_pin = df_pin.withColumn("poster_name", when(col("poster_name").rlike("User Info Error"), None).otherwise(col("poster_name")))
df_pin = df_pin.withColumn("tag_list", when(col("tag_list").rlike("N,o, ,T,a,g,s, ,A,v,a,i,l,a,b,l,e"), None).otherwise(col("tag_list")))
df_pin = df_pin.withColumn("title", when(col("title").rlike("(^$|No Title Data Available)"), None).otherwise(col("title")))

# COMMAND ----------

df_pin = df_pin.withColumn(
    "follower_count",
    when(col("follower_count").endswith("k"), regexp_replace("follower_count", "k", "000"))
    .when(col("follower_count").endswith("M"), regexp_replace("follower_count", "M", "000000"))
    .otherwise(col("follower_count"))
)

df_pin = df_pin.withColumn("follower_count", df_pin["follower_count"].cast("int"))

# COMMAND ----------

df_pin = df_pin.withColumn("index", df_pin["index"].cast("int"))
df_pin = df_pin.withColumnRenamed("index", "ind")

# COMMAND ----------

df_pin = df_pin.withColumn("save_location", regexp_replace("save_location", "Local save in ", ""))

# COMMAND ----------

df_pin = df_pin.withColumn("tag_list", split(df_pin["tag_list"], ",", limit=-1))

# COMMAND ----------

df_pin = df_pin.withColumn("downloaded", df_pin["downloaded"].cast("boolean"))

# COMMAND ----------

df_pin = df_pin.select("ind", "unique_id", "title", "description", "follower_count", "poster_name", "tag_list", "is_image_or_video", "image_src", "save_location", "downloaded", "category")

# COMMAND ----------

# MAGIC %md
# MAGIC ### df_geo cleaning

# COMMAND ----------

df_geo = df_geo.withColumn("parsed_data", from_json(col("data"), geo_schema))
df_geo = df_geo.select("parsed_data.*")

# COMMAND ----------

df_geo = df_geo.withColumn("coordinates", array("latitude", "longitude"))
df_geo = df_geo.drop("latitude", "longitude")

# COMMAND ----------

df_geo = df_geo.select("ind", "country", "coordinates", "timestamp")

# COMMAND ----------

# MAGIC %md
# MAGIC ### df_user cleaning

# COMMAND ----------

df_user = df_user.withColumn("parsed_data", from_json(col("data"), user_schema))
df_user = df_user.select("parsed_data.*")

# COMMAND ----------

df_user = df_user.withColumn("user_name", concat("first_name", lit(" "), "last_name"))
df_user = df_user.drop("first_name", "last_name")

# COMMAND ----------

df_user = df_user.select("ind", "user_name", "age", "date_joined")

# COMMAND ----------

# MAGIC %md
# MAGIC ### writing to sinks

# COMMAND ----------

df_pin.writeStream \
    .format("delta")\
    .outputMode("append")\
    .option("checkpointLocation", "/tmp/kinesis/_checkpoints/")\
    .table("0e33e87dfa09_pin_table")

df_geo.writeStream \
    .format("delta")\
    .outputMode("append")\
    .option("checkpointLocation", "/tmp/kinesis/_checkpoints/")\
    .table("0e33e87dfa09_geo_table")

df_user.writeStream \
    .format("delta")\
    .outputMode("append")\
    .option("checkpointLocation", "/tmp/kinesis/_checkpoints/")\
    .table("0e33e87dfa09_user_table")


# COMMAND ----------

# MAGIC %md
# MAGIC ### run the following command before running writeStream commands:
# MAGIC
# MAGIC `dbutils.fs.rm("/tmp/kinesis/_checkpoints/", True)`

# COMMAND ----------

# dbutils.fs.rm("/tmp/kinesis/_checkpoints/", True)

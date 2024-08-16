# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC SET spark.databricks.delta.formatCheck.enabled=false

# COMMAND ----------

file_location = "/mnt/mounted_bucket/topics/0e33e87dfa09.pin/partition=0/*.json"
file_type = "json"
infer_schema = "true"
df_pin = spark.read.format(file_type).option("inferSchema", infer_schema).load(file_location)

display(df_pin)

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC If we need to reset the dataframe, do so here

# COMMAND ----------

df = df_pin

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Replace empty entries and entries with no relevant data in each column with None
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Check all the unique descriptions in the data

# COMMAND ----------

display(df.select("description").distinct())

# COMMAND ----------

# MAGIC %md
# MAGIC Replace the missing values in "description" with None

# COMMAND ----------

# MAGIC %md
# MAGIC Missing value substitutions for `description` - 159 nulls, `follower_count` - 15 nulls, `image_src` - 45 nulls, `poster_name` - 15 nulls, `tag_list` - 27 nulls, `title` - 16 nulls.

# COMMAND ----------

df = df.withColumn("description", when(col("description").rlike("(No description available|Untitled)"), None).otherwise(col("description")))
df = df.withColumn("follower_count", when(col("follower_count").rlike("User Info Error"), None).otherwise(col("follower_count")))
df = df.withColumn("image_src", when(col("image_src").rlike("Image src error."), None).otherwise(col("image_src")))
df = df.withColumn("poster_name", when(col("poster_name").rlike("User Info Error"), None).otherwise(col("poster_name")))
df = df.withColumn("tag_list", when(col("tag_list").rlike("N,o, ,T,a,g,s, ,A,v,a,i,l,a,b,l,e"), None).otherwise(col("tag_list")))
df = df.withColumn("title", when(col("title").rlike("(^$|No Title Data Available)"), None).otherwise(col("title")))


# COMMAND ----------

# MAGIC %md
# MAGIC All columns above have Null values inserted correctly

# COMMAND ----------

# MAGIC %md
# MAGIC ## Perform the necessary transformations on the follower_count to ensure every entry is a number. Make sure the data type of this column is an int

# COMMAND ----------

# MAGIC %md
# MAGIC If the string ends with k, replace with 000,<br>
# MAGIC if the string ends with M, replace with 000000<br>
# MAGIC else, do nothing

# COMMAND ----------

df = df.withColumn(
    "follower_count",
    when(col("follower_count").endswith("k"), regexp_replace("follower_count", "k", "000"))
    .when(col("follower_count").endswith("M"), regexp_replace("follower_count", "M", "000000"))
    .otherwise(col("follower_count"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC Cast follower_count column to numeric type

# COMMAND ----------

df = df.withColumn("follower_count", df["follower_count"].cast("int"))

# COMMAND ----------

# MAGIC %md
# MAGIC Column successfully converted to integer

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ensure that each column containing numeric data has a numeric data type

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Columns with numeric values - index

# COMMAND ----------

df = df.withColumn("index", df["index"].cast("integer"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Clean the data in the save_location column to include only the save location path

# COMMAND ----------

df = df.withColumn("save_location", regexp_replace("save_location", "Local save in ", ""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Rename index column to ind

# COMMAND ----------

df = df.withColumnRenamed("index", "ind")

# COMMAND ----------

display(df)

# COMMAND ----------

total_count = df.count()
distinct_count = df.select("ind").distinct().count()

print(total_count == distinct_count)

# COMMAND ----------

# MAGIC %md
# MAGIC ## EXPERIMENTAL CLEANING - DROP DUPLICATES

# COMMAND ----------

df = df.dropDuplicates()

# COMMAND ----------

# MAGIC %md
# MAGIC ## EXPERIMENTAL CLEANING - SORT BY IND

# COMMAND ----------

df = df.orderBy("ind", ascending=True)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## EXPERIMENTAL CLEANING - SPLIT TAGS TO ARRAY

# COMMAND ----------

df = df.withColumn("tag_list", split(df["tag_list"], ",", limit=-1))
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## EXPERIMENTAL CLEANING - CAST DOWNLOADED TO BOOLEAN

# COMMAND ----------

df = df.withColumn("downloaded", df["downloaded"].cast("boolean"))

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## REORDER DATAFRAME

# COMMAND ----------

df = df.select("ind", "unique_id", "title", "description", "follower_count", "poster_name", "tag_list", "is_image_or_video", "image_src", "save_location", "category", "downloaded")

# COMMAND ----------

# MAGIC %md
# MAGIC ## EXPERIMENTAL CLEANING - DROP ROWS WITH 6 NULL VALUES

# COMMAND ----------

# df_null_count = df

# COMMAND ----------

# # Initialize a column expression to accumulate the sum of null counts
# null_counts_expr = col(df_null_count.columns[0]).isNull().cast("int")

# # Add expressions for each column in the DataFrame
# for column in df_null_count.columns[1:]:
#     null_counts_expr += col(column).isNull().cast("int")

# # Add a new column 'num_nulls' with the computed null counts
# df_with_null_counts = df_null_count.withColumn("num_nulls", null_counts_expr)

# # Display the resulting DataFrame in Databricks
# display(df_with_null_counts)



# COMMAND ----------

# cleaned_df = df_with_null_counts.filter(col("num_nulls") != 6)
# display(cleaned_df)

# COMMAND ----------

# cleaned_df = cleaned_df.drop("num_nulls")

# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC SET spark.databricks.delta.formatCheck.enabled=false

# COMMAND ----------

file_location = "/mnt/mounted_bucket/topics/0e33e87dfa09.pin/partition=0/*.json"
file_type = "json"
infer_schema = "true"
df_pin = spark.read.format(file_type).option("inferSchema", infer_schema).load(file_location)

# COMMAND ----------

file_location = "/mnt/mounted_bucket/topics/0e33e87dfa09.geo/partition=0/*.json"
file_type = "json"
infer_schema = "true"
df_geo = spark.read.format(file_type).option("inferSchema", infer_schema).load(file_location)

# COMMAND ----------

file_location = "/mnt/mounted_bucket/topics/0e33e87dfa09.user/partition=0/*.json"
file_type = "json"
infer_schema = "true"
df_user = spark.read.format(file_type).option("inferSchema", infer_schema).load(file_location)

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### df_pin cleaning

# COMMAND ----------

df_pin = df_pin.dropDuplicates()

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
df_pin = df_pin.orderBy("ind")

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

df_geo = df_geo.dropDuplicates()

# COMMAND ----------

df_geo = df_geo.withColumn("ind", df_geo["ind"].cast("int"))
df_geo = df_geo.orderBy("ind")

# COMMAND ----------

df_geo = df_geo.withColumn("latitude", df_geo["latitude"].cast("float"))
df_geo = df_geo.withColumn("longitude", df_geo["longitude"].cast("float"))
df_geo = df_geo.withColumn("coordinates", array("latitude", "longitude"))
df_geo = df_geo.drop("latitude", "longitude")

# COMMAND ----------

df_geo = df_geo.withColumn("timestamp", to_timestamp("timestamp", "yyyy-MM-dd HH:mm:ss"))

# COMMAND ----------

df_geo = df_geo.select("ind", "country", "coordinates", "timestamp")


# COMMAND ----------

# MAGIC %md
# MAGIC ### df_user cleaning

# COMMAND ----------

df_user = df_user.dropDuplicates()

# COMMAND ----------

df_user = df_user.orderBy("ind")

# COMMAND ----------

df_user = df_user.withColumn("date_joined", to_timestamp("date_joined", "yyyy-MM-dd HH:mm:ss"))

# COMMAND ----------

df_user = df_user.withColumn("ind", df_user["ind"].cast("int"))
df_user = df_user.withColumn("age", df_user["age"].cast("int"))

# COMMAND ----------

df_user = df_user.withColumn("user_name", concat("first_name", lit(" "), "last_name"))
df_user = df_user.drop("first_name", "last_name")

# COMMAND ----------

df_user = df_user.select("ind", "user_name", "age", "date_joined")

# COMMAND ----------

display(df_pin)

# COMMAND ----------

display(df_geo)

# COMMAND ----------

display(df_user)

# COMMAND ----------

df_pin.printSchema()

# COMMAND ----------

df_geo.printSchema()

# COMMAND ----------

df_user.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Questions

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC Task 4: Find the most popular category in each country
# MAGIC
# MAGIC Find the most popular pintrest category people post to, based on their country.
# MAGIC Your query should return a DataFrame that contains the following columns:
# MAGIC
# MAGIC * country
# MAGIC * category
# MAGIC * category_count  - a new column containing the desired query output

# COMMAND ----------

task_4_joined_df = df_pin.join(df_geo, df_pin["ind"] == df_geo["ind"], how="left")
task_4_select_df = task_4_joined_df.select("country", "category")
task_4_grouped_df = task_4_select_df.groupBy("country", "category").count()
task_4_renamed_df = task_4_grouped_df.withColumnRenamed("count", "category_count")
task_4_sorted_df = task_4_renamed_df.orderBy("country", col("category_count").desc())
display(task_4_sorted_df)

# COMMAND ----------

task_4_df = (df_pin.join(df_geo, df_pin["ind"] == df_geo["ind"], how="left")
    .select("country", "category")
    .groupBy("country", "category").count()
    .withColumnRenamed("count", "category_count")
    .orderBy("country", col("category_count").desc())
)

window = Window.partitionBy("country").orderBy("country", col("category_count").desc())
task_4_df = task_4_df.withColumn("rank", rank().over(window)).filter(col("rank") == 1).drop("rank")
display(task_4_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Task 5: Find which was the most popular category each year 
# MAGIC
# MAGIC Find out how many posts each category had between 2018 and 2022
# MAGIC Your query should return a DataFrame that contains the following columns:
# MAGIC
# MAGIC   * post_year, a new column that contains only the year from the timestamp column
# MAGIC   * category
# MAGIC   * category_count, a new column containing the desired query output

# COMMAND ----------

task_5_df = ( df_pin.join(df_geo, on="ind", how="left")
             .select(year("timestamp").alias("post_year"), "category")
             .filter(col("post_year").between(2018, 2022))
             .groupBy("post_year", "category").count()
             .withColumnRenamed("count", "category_count")
             .orderBy(col("post_year").asc(), col("category_count").desc())
)
             

window = Window.partitionBy("post_year").orderBy(col("post_year").asc(), col("category_count").desc())
task_5_df = task_5_df.withColumn("category_rank", rank().over(window)).filter(col("category_rank") == 1).drop("category_rank")
display(task_5_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Task 6: Find the user with the most followers in each country:
# MAGIC
# MAGIC Step 1: For each country find the user with the most followers.
# MAGIC
# MAGIC Your query should return a DataFrame that contains the following columns:
# MAGIC
# MAGIC * country
# MAGIC * poster_name
# MAGIC * follower_count
# MAGIC
# MAGIC Step 2: Based on the above query, find the country with the user with most followers.
# MAGIC
# MAGIC
# MAGIC Your query should return a DataFrame that contains the following columns:
# MAGIC
# MAGIC * country
# MAGIC * follower_count
# MAGIC This DataFrame should have only one entry.

# COMMAND ----------

task_6_df = ( df_pin.join(df_geo, on="ind", how="left")
             .select("country", "poster_name", "follower_count")
             .groupBy("country", "poster_name")
             .agg(max("follower_count").alias("follower_count"))
             .orderBy("country", col("follower_count").desc())
)

window = Window.partitionBy("country").orderBy(col("follower_count").desc())
task_6_df = (task_6_df.withColumn("follower_rank", rank().over(window))
             .filter(col("follower_rank") == 1)
             .drop("follower_rank")
             )


task_6_pt_2 = task_6_df.orderBy("follower_count", ascending=False).limit(1).select("country", "follower_count")
display(task_6_pt_2)

# COMMAND ----------

# MAGIC %md
# MAGIC Task 7: Find the most popular category for different age groups
# MAGIC
# MAGIC What is the most popular category people post to based on the following age groups:
# MAGIC
# MAGIC * 18-24
# MAGIC * 25-35
# MAGIC * 36-50
# MAGIC * +50
# MAGIC
# MAGIC Your query should return a DataFrame that contains the following columns:
# MAGIC
# MAGIC * age_group, a new column based on the original age column
# MAGIC * category
# MAGIC * category_count, a new column containing the desired query output

# COMMAND ----------

task_7_df = (df_pin.join(df_user, on="ind", how="left")
             .withColumn("age_group", 
                         when((col("age") >= 18) & (col("age") <= 24), "18-24" )
                         .when((col("age") >= 25) & (col("age") <= 35), "25-35")
                         .when((col("age") >= 36) & (col("age") <= 50), "36-50")
                         .otherwise("50+")
                         )
             .groupBy("age_group", "category").count()
             .withColumnRenamed("count", "category_count")
             )

# task_7_df = (task_7_df.withColumn("custom_rank",
#                                  when(col("age_group") == "18-24", 1)
#                                  .when(col("age_group") == "25-35", 2)
#                                  .when(col("age_group") == "36-50", 3)
#                                  .when(col("age_group") == "+50", 4)
#                                  )
#             )

window = Window.partitionBy("age_group").orderBy(col("category_count").desc())
task_7_df = (task_7_df.withColumn("rank", rank().over(window))
             .filter(col("rank") == 1)
             .drop("rank")
            #  .orderBy(col("custom_rank").asc())
            #  .drop("custom_rank")
            )
display(task_7_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Task 8: Find the median follower count for different age groups
# MAGIC
# MAGIC
# MAGIC What is the median follower count for users in the following age groups:
# MAGIC
# MAGIC * 18-24
# MAGIC * 25-35
# MAGIC * 36-50
# MAGIC * +50
# MAGIC
# MAGIC Your query should return a DataFrame that contains the following columns:
# MAGIC
# MAGIC * age_group, a new column based on the original age column
# MAGIC * median_follower_count, a new column containing the desired query output

# COMMAND ----------

task_8_df = ( df_pin.join(df_user, on="ind", how="left")
             .withColumn("age_group",
                         when((col("age") >= 18) & (col("age") <=24), "18-24")
                         .when((col("age") >= 25) & (col("age") <= 35), "25-35")
                         .when((col("age") >= 36) & (col("age") <= 50), "36-50")
                         .otherwise("50+")
                        )
             .groupBy("age_group")
             .agg(percentile_approx("follower_count", 0.5).alias("median_follower_count"))
             .orderBy("age_group")
)

# task_8_df = (task_8_df.withColumn("custom_rank",
#                                  when(col("age_group") == "18-24", 1)
#                                  .when(col("age_group") == "25-35", 2)
#                                  .when(col("age_group") == "36-50", 3)
#                                  .when(col("age_group") == "+50", 4)
#                                  )
#              .orderBy("custom_rank")
#              .drop("custom_rank")
#             )

display(task_8_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Task 9: Find how many users have joined between 2015 and 2020
# MAGIC
# MAGIC Find how many users have joined between 2015 and 2020.
# MAGIC
# MAGIC Your query should return a DataFrame that contains the following columns:
# MAGIC
# MAGIC * post_year, a new column that contains only the year from the timestamp column
# MAGIC * number_users_joined, a new column containing the desired query output

# COMMAND ----------

task_9_df = ( df_pin.join(df_user, on="ind", how="left")
             .withColumn("post_year", year(col("date_joined")))
             .filter((col("post_year") >= 2015) & (col("post_year") <= 2020))
             .groupBy("post_year")
             .count().alias("number_users_joined")          
             )
display(task_9_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Task 10: Find the median follower count of users based on their joining year
# MAGIC
# MAGIC
# MAGIC Find the median follower count of users have joined between 2015 and 2020.
# MAGIC
# MAGIC Your query should return a DataFrame that contains the following columns:
# MAGIC * post_year, a new column that contains only the year from the timestamp column
# MAGIC * median_follower_count, a new column containing the desired query output

# COMMAND ----------

task_10_df = ( df_pin.join(df_user, on="ind", how="left")
              .withColumn("post_year", year(col("date_joined")))
              .filter((col("post_year") >= 2015) & (col("post_year") <= 2020))
              .groupBy("post_year")
              .agg(percentile_approx("follower_count", 0.5).alias("median_follower_count"))
              .orderBy(col("post_year").asc())
)

display(task_10_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Task 11: Find the median follower count of users based on their joining year and age group
# MAGIC
# MAGIC Find the median follower count of users that have joined between 2015 and 2020, based on which age group they are part of.
# MAGIC
# MAGIC Your query should return a DataFrame that contains the following columns:
# MAGIC
# MAGIC * age_group, a new column based on the original age column
# MAGIC * post_year, a new column that contains only the year from the timestamp column
# MAGIC * median_follower_count, a new column containing the desired query output

# COMMAND ----------

task_11_df = ( df_pin.join(df_user, on="ind", how="left")
              .withColumn("age_group",
                          when((col("age") >= 18) & (col("age") <= 24), "18-24")
                          .when((col("age") >= 25) & (col("age") <= 35), "25-35")
                          .when((col("age") >= 36) & (col("age") <= 50), "36-50")
                          .otherwise("50+")
              )
              .withColumn("post_year", year(col("date_joined")))
              .filter((col("post_year") >= 2015) & (col("post_year") <= 2020))
              .groupBy("age_group", "post_year")
              .agg(percentile_approx("follower_count", 0.5).alias("median_follower_count"))
              .orderBy("age_group", "post_year")

)

display(task_11_df)

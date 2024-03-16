# Databricks notebook source
# MAGIC %fs ls dbfs:/FileStore/tables/formula1/
# MAGIC

# COMMAND ----------

df=spark.read.csv("dbfs:/FileStore/tables/formula1/circuits.csv",header=True,inferSchema=True)

# COMMAND ----------

display(df)

# COMMAND ----------

df.select("circuitId","name").display()

# COMMAND ----------

df.select("circuitId".alias("circuit_id"),"name").display()

# COMMAND ----------

df.select(col("circuitId").alias("circuit_id"),"name").display()

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df.select(col("circuitId").alias("circuit_id"),"name").display()

# COMMAND ----------

df.select(concat("location",lit(" & "),"country").alias("location&county")).display()

# COMMAND ----------

df.withColumnRenamed("circuitId","circuit_id").display()

# COMMAND ----------

df2.withColumn("ingestion_date",current_date()).display()


# COMMAND ----------

df.columns


# COMMAND ----------

df.show()

# COMMAND ----------

df.display()

# COMMAND ----------

df.withColumnRenamed("circuitId","circuit_id").withColumnRenamed("circuitRef","circuit_ref").display()

# COMMAND ----------

df2.withColumn("country",upper("country")).display()

# COMMAND ----------

df2\
.withColumn("location&country",concat("location", lit(" "),"country"))\
.drop("country","location")\
.display()

# COMMAND ----------

df2=df1.drop("url")

# COMMAND ----------

df1=df.toDF(*new_column)

# COMMAND ----------

df.column


# COMMAND ----------

df.select(concat("location",lit(" & "),"country").alias("location&county")).display()

# COMMAND ----------

df.withColumnRenamed("circuitId","circuit_id").display()


# COMMAND ----------

new_column=['circuit_id',
 'circuit_ref',
 'name',
 'location',
 'country',
 'latitude',
 'longitute',
 'altitude',
 'url']

# COMMAND ----------

df1=df.toDF(*new_column)

# COMMAND ----------

df2=df1.drop("url")

# COMMAND ----------

df2.withColumn("ingestion_date",current_date()).display()

# COMMAND ----------

df2.withColumn("newcolumn",lit("formula1")).display()

# COMMAND ----------

df2\
.withColumn("location&country",concat("location", lit(" "),"country"))\
.drop("country","location")\
.display()

# COMMAND ----------

(df2
.withColumn("location&country",concat("location", lit(" "),"country"))
.drop("country","location")
.display())

# COMMAND ----------

df2.withColumn("country",upper("country")).display()

# COMMAND ----------

df2.withColumn("ingestion_date",current_timestamp())

# COMMAND ----------

df2.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #making change in DB
# MAGIC # Making changes in Github
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # learning Github integration
# Practicing more

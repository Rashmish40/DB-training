# Databricks notebook source
df=spark.read.option("header",True).option("inferSchema",True).json("dbfs:/FileStore/tables/raw/iot.json")

# COMMAND ----------

df=spark.read.json("dbfs:/FileStore/tables/raw/iot1.json")

# COMMAND ----------

df.show()

# COMMAND ----------

df=spark.read.json(f"{input_path}iot1.json")

# COMMAND ----------

input_path

# COMMAND ----------

# MAGIC %run "/Workspace/Users/naval@cloudthat.net/day 1/includes"

# COMMAND ----------

input_path

# COMMAND ----------

df=spark.read.json(f"{input_path}iot1.json")

# COMMAND ----------

df=spark.read.json(f"{input_path}emp.csv")

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/tables/raw/

# COMMAND ----------



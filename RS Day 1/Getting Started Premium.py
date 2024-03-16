# Databricks notebook source
print("Hello")


# COMMAND ----------

# MAGIC %sql
# MAGIC create schema Rashmi

# COMMAND ----------

data=([(1,'a',30),(2,'b',34)])
schema="id int, name string, age int"
df=spark.createDataFrame(data,schema)

# COMMAND ----------

df.show()

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/tables/raw/
# MAGIC

# COMMAND ----------

df=spark.read.option("header",True).option("inferSchema",True).csv("dbfs:/FileStore/tables/raw/emp.csv")

# COMMAND ----------

df.show()

# COMMAND ----------

df.display()


# COMMAND ----------

df.printSchema()


# COMMAND ----------

df.write.saveAsTable("rashmi.emp_demp")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from rashmi.emp_demp

# COMMAND ----------



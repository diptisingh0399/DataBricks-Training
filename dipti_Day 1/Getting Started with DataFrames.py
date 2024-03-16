# Databricks notebook source
# MAGIC %md
# MAGIC # Python
# MAGIC ## Python
# MAGIC ### Python

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema dipti

# COMMAND ----------

data=([(1,'a',30),(2,'b',34)])
schema="id int, name string, age int"
df=spark.createDataFrame(data,schema)

# COMMAND ----------

df.show()

# COMMAND ----------

dbutils.help()

# COMMAND ----------

# MAGIC %fs ls

# COMMAND ----------

df=spark.read.option("header",True).option("inferSchema",True).csv("dbfs:/FileStore/tables/raw/emp.csv")

# COMMAND ----------

df.show()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.write.saveAsTable("dipti.emp_demo")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dipti.emp_demo

# COMMAND ----------



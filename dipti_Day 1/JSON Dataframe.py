# Databricks notebook source
df=spark.read.json("dbfs:/FileStore/tables/raw/iot1.json")

# COMMAND ----------

df.show()

# COMMAND ----------

df.display()

# COMMAND ----------

df.write.saveAsTable("dipti.iot1")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dipti.iot1

# COMMAND ----------

df=spark.read.json(f"{input_path}iot1.json")

# COMMAND ----------

# MAGIC %run "/Workspace/Users/dipti.singh@marsh.com/Day 1/includes"

# COMMAND ----------

df=spark.read.json(f"{input_path}iot1.json")

# COMMAND ----------



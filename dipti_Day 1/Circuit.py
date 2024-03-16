# Databricks notebook source
# MAGIC %run "/Workspace/Users/dipti.singh@marsh.com/Day 1/includes"

# COMMAND ----------

print(input_path)

# COMMAND ----------

print(input_formula1_path)

# COMMAND ----------

df=spark.read.csv(f"{input_formula1_path}circuits.csv",header=True,inferSchema=True)

# COMMAND ----------

df.select("circuitId","name").display()

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df.select(col("circuitId").alias("circuit_id"),"name").display()

# COMMAND ----------

df.select("circuitId",col("name"),df.location,df["country"]).display()

# COMMAND ----------

df.select(concat("location",lit(" & "),"country").alias("location & country")).display()

# COMMAND ----------

df.withColumnRenamed("circuitId","circuit_id").display()

# COMMAND ----------

df.columns

# COMMAND ----------

# MAGIC %md
# MAGIC #dipti_new_change

# Databricks notebook source
output = "/temp/dbricklight/output"

# COMMAND ----------

df = spark.createDataFrame([(i, "Test data") for i in range(1000)], ("key", "value"))
df.show()

# COMMAND ----------

df.write.parquet(output)

# COMMAND ----------

# dbutils.fs.rm(output, True)
dbutils.fs.ls(output)

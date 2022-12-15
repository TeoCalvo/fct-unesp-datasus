# Databricks notebook source
import pandas as pd
from pyspark.sql import functions as F

# COMMAND ----------

df = spark.read.csv("/mnt/datalake/liga_unesp/raw/SIHSUS/RD/csv/",sep=';', header=True)
df = df.withColumn("DT_REFERENCIA", F.make_date( "ANO_CMPT", "MES_CMPT", F.lit(1) ))
df.write.mode("overwrite").format("delta").partitionBy("DT_REFERENCIA").saveAsTable("bronze_datasus.sihsus_rd")

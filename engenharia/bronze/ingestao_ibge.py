# Databricks notebook source
df = spark.read.csv("/mnt/datalake/liga_unesp/raw/ibge/ibge_codigo_municipio.csv", header=True, sep=";", encoding='latin1')

columns = [i for i in df.columns if not i.startswith("_c")]
df = (df.select(*columns)
        .withColumnRenamed("UF","uf")
        .withColumnRenamed("Nome_UF","nome_uf")
        .withColumnRenamed("Região Geográfica Intermediária","regiao_geografica_intermediaria")
        .withColumnRenamed("Nome Região Geográfica Intermediária","nome_regiao_geografica_intermediaria")
        .withColumnRenamed("Região Geográfica Imediata","regiao_geografica_imediata")
        .withColumnRenamed("Nome Região Geográfica Imediata","nome_regiao_geografica_imediata")
        .withColumnRenamed("Mesorregião Geográfica", "mesorregiao_geografica")
        .withColumnRenamed("Nome_Mesorregião", "nome_mesorregiao")
        .withColumnRenamed("Microrregião Geográfica", "microrregiao_geografica")
        .withColumnRenamed("Nome_Microrregião", "nome_microrregiao")
        .withColumnRenamed("Município", "municipio")
        .withColumnRenamed("Código Município Completo", "codigo_municipio_completo")
        .withColumnRenamed("Nome_Município", "nome_municipio")
        .filter("uf is not null")
)

df.display()

# COMMAND ----------

df.write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable("bronze_datasus.ibge_uf")

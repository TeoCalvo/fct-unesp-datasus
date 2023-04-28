# Databricks notebook source
def ingestao_table(path):

    table_name = (path.split("/")[-1]
                            .split(".")[0]
                            .lower())
    print(table_name)

    df = (spark.read
            .format('csv')
            .option('sep', ';')
            .option('header', 'true')
            .load(path))

    (df.coalesce(1)
       .write
       .format('delta')
       .mode('overwrite')
       .saveAsTable(f'bronze.datasus.{table_name}'))

path = '/mnt/datalake/liga_unesp/raw/SIHSUS/DEPARA/'
paths = [i.path for i in dbutils.fs.ls(path)]

for i in paths:
    ingestao_table(i)

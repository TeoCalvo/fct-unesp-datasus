# Databricks notebook source
# MAGIC %pip install tqdm

# COMMAND ----------

import os
import datetime
from dateutil.relativedelta import relativedelta
import urllib.request

from tqdm import tqdm

def date_range(date_start, date_stop):

    dt_start = datetime.datetime.strptime(date_start, "%Y-%m-%d")
    dt_stop = datetime.datetime.strptime(date_stop, "%Y-%m-%d")

    dates = []
    while dt_start <= dt_stop:
        dates.append( dt_start.strftime("%y%m") )
        dt_start += relativedelta(months=1)

    return dates

def import_file(fonte,tipo_arquivo, uf, ano_mes):

    url = f"ftp://ftp.datasus.gov.br/dissemin/publicos/{fonte}/200801_/Dados/{tipo_arquivo}{uf}{ano_mes}.dbc"
    nome_arquivo = f"{fonte}_{tipo_arquivo}{uf}{ano_mes}.dbc"

    urllib.request.urlretrieve(url, f"/dbfs/mnt/datalake/liga_unesp/{nome_arquivo}")
    return True


def import_file_dates(fonte, tipo_arquivo, uf, dates):
    for ano_mes in tqdm(dates):
        print(ano_mes)
        import_file(fonte, tipo_arquivo, uf, ano_mes)
    return True

# COMMAND ----------

date_start = "2021-06-01"
date_stop = "2022-09-01"

fonte = "SIHSUS"
tipo_arquivo = "RD"
uf = "AC"
ano_meses = date_range(date_start, date_stop)

import_file_dates(fonte, tipo_arquivo, uf, ano_meses)

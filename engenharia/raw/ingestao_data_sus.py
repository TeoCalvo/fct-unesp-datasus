# Databricks notebook source
# MAGIC %pip install tqdm lxml

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
    file_name = f"{uf}_{ano_mes}.dbc"
    folder_name = f"/dbfs/mnt/datalake/liga_unesp/raw/{fonte}/{tipo_arquivo}/dbc/"

    try:
        urllib.request.urlretrieve(url, f"{folder_name}{file_name}")
    
    except urllib.error.URLError as err:
        print(file_name, err)

    return True


def import_file_dates(fonte, tipo_arquivo, uf, dates):
    for ano_mes in tqdm(dates):
        print(ano_mes)
        import_file(fonte, tipo_arquivo, uf, ano_mes)
    return True

def import_file_ufs(fonte, tipo_arquivo, ufs, dates):
    for u in tqdm(ufs):
        import_file_dates(fonte, tipo_arquivo, u, dates)

# COMMAND ----------

date_start = "2021-06-01"
date_stop = "2022-09-01"

fonte = "SIHSUS"
tipo_arquivo = "RD"
ufs = [  'AC',
         'AL',
         'AP',
         'AM',
         'BA',
         'CE',
         'DF',
         'ES',
         'GO',
         'MA',
         'MT',
         'MS',
         'MG',
         'PA',
         'PB',
         'PR',
         'PE',
         'PI',
         'RJ',
         'RN',
         'RS',
         'RO',
         'RR',
         'SC',
         'SP',
         'SE',
         'TO']


ano_meses = date_range(date_start, date_stop)

import_file_ufs(fonte, tipo_arquivo, ufs, ano_meses)

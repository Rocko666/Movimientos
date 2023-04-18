#!/usr/bin/env python
import sys
reload(sys)
sys.setdefaultencoding('latin1')
from pyspark.sql import SparkSession
import pandas as pd
from datetime import datetime
from pyspark.sql import functions as F, Window
from pyspark.sql.functions import date_format
import re
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--rutain', required=True, type=str)
parser.add_argument('--tablaout', required=True, type=str)
parser.add_argument('--tipo', required=True, type=str)
parametros = parser.parse_args()
vPathExcel=parametros.rutain
vTablaOut=parametros.tablaout
vTipo=parametros.tipo

timestart = datetime.now()
vRegExpUnnamed=r"unnamed*"
vApp="AJUSTES MOVIMIENTOS"
dfExcel = pd.read_excel(vPathExcel, dtype=str)

def getColumnName(vColumn=str):
    a=vColumn.lower()
    x=a.replace(' ','_')
    y=x.replace(':','')
    return y

spark = SparkSession\
    .builder\
    .appName(vApp)\
    .master("local")\
    .enableHiveSupport()\
    .getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")

df0 = spark.createDataFrame(dfExcel.astype(str))
df1 = df0
vColumns=df0.columns
vColumnsOk=[]
for vColumn in vColumns:
    if (re.match( vRegExpUnnamed,vColumn.lower())):
        print("Columna rechazada {}".format(vColumn.lower()))
    else:
        vColumnOK=getColumnName(vColumn)
        vColumnsOk.append(getColumnName(vColumn))
        df1 = df1.withColumnRenamed(
            vColumn,
            getColumnName(vColumn)
        )

df2 = df1.select(*vColumnsOk)
df2.printSchema()
df2 = df2.withColumn('fecha_proceso',date_format(df2.fecha_proceso,'yyyyMMdd'))
df2.repartition(1).write.mode(vTipo).saveAsTable(vTablaOut)

spark.stop()
timeend = datetime.now()
duracion = timeend - timestart 
print("Duracion {}".format(duracion))

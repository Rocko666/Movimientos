#!/usr/bin/env python
import sys
reload(sys)
sys.setdefaultencoding('latin1')
#sys.setdefaultencoding('utf-8')
from pyspark.sql import SparkSession
import pandas as pd
from datetime import datetime
from pyspark.sql import functions as F, Window
import re
import argparse

# pip install xlrd==1.2.0

parser = argparse.ArgumentParser()
parser.add_argument('--rutain', required=True, type=str)
parser.add_argument('--part', required=True, type=str)
parser.add_argument('--tablaout', required=True, type=str)
parser.add_argument('--tipo', required=True, type=str)
parametros = parser.parse_args()

vPart=parametros.part
vPathExcel=parametros.rutain
vTablaOut=parametros.tablaout
vTipo=parametros.tipo

timestart = datetime.now()
vRegExpUnnamed=r"unnamed*"
vApp="PERIMETRO PARA MOVIMIENTOS"
dfExcel = pd.read_excel(vPathExcel)

def getColumnName(vColumn=str):
    a=vColumn.lower()
    x=a.replace(' ','_')
    y=x.replace(':','')
    return y

spark = SparkSession\
    .builder\
    .appName(vApp)\
    .config("hive.exec.dynamic.partition", "true")\
    .config("hive.exec.dynamic.partition.mode", "nonstrict")\
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
df3 = df2.withColumn("fecha_carga", F.lit(datetime.now()))
df3 = df3.withColumn("pt_fecha", F.lit(vPart))

## Se escriben los  datos en una tabla en HIVE 
df3.printSchema()
print ("==== Guardando los datos en tabla "+vTablaOut+" ====")
query_truncate = "ALTER TABLE "+ vTablaOut +" DROP IF EXISTS PARTITION (PT_FECHA = "+str(vPart)+") purge"
spark.sql(query_truncate)
print("Truncate Exitoso de la tabla "+vTablaOut+" particion "+str(vPart))
df3.repartition(1).write.mode("append").insertInto(vTablaOut)
print("Escritura Exitosa de la tabla "+ vTablaOut)

spark.stop()
timeend = datetime.now()
duracion = timeend - timestart 
print("Duracion {}".format(duracion))

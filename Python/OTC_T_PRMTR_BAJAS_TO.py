#!/usr/bin/env python
import sys
reload(sys)
#sys.setdefaultencoding('utf-8-sig')
#sys.setdefaultencoding('latin1')
#sys.setdefaultencoding('windows-1252')
sys.setdefaultencoding('utf-8')
from pyspark.sql import SparkSession
import pandas as pd
from datetime import datetime
from pyspark.sql import functions as F, Window
import re
import argparse
import os

# Establece la codificacion UTF-8 en el entorno de Python
os.environ['PYTHONIOENCODING'] = 'UTF-8'

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
vApp="PERIMETRO PARA BAJAS Y TRANSFER OUT"
dfExcel = pd.read_excel(vPathExcel, encoding='utf-8')
print (dfExcel)

def getColumnName(vColumn=str):
    a=vColumn.lower()
    x=a.replace(' ','_')
    y=x.replace(':','')
    return y

spark = SparkSession\
    .builder\
    .appName(vApp)\
    .master("local")\
    .config("hive.exec.dynamic.partition", "true")\
    .config("hive.exec.dynamic.partition.mode", "nonstrict")\
    .config("spark.sql.sessionEncoding", "UTF-8")\
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
df3.printSchema()

print ("==== Guardando los datos en tabla "+vTablaOut+" ====")
query_truncate = "ALTER TABLE "+ vTablaOut +" DROP IF EXISTS PARTITION (PT_FECHA = "+str(vPart)+") purge"
spark.sql(query_truncate)
print("Truncate Exitoso de la tabla "+vTablaOut+" particion "+str(vPart))
df3.repartition(1).write.mode("append").insertInto(vTablaOut)
df3.write.mode('overwrite').format('parquet').saveAsTable('db_desarrollo2021.tmp_prmt_b_to')
print("Escritura Exitosa de la tabla "+ vTablaOut)

spark.stop()
timeend = datetime.now()
duracion = timeend - timestart 
print("Duracion {}".format(duracion))

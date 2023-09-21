import sys
reload(sys)
sys.setdefaultencoding('latin1')
from pyspark.sql import SparkSession
import pandas as pd
from datetime import datetime
from pyspark.sql import functions as F, Window
import re
import argparse

# pip install xlrd==1.2.0

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
vApp="BASE CARGA DESCUENTO NO PYMES"
dfExcel = pd.read_excel(vPathExcel)

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
    .config("hive.exec.dynamic.partition.mode", "nonstrict") \
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

df3 =  df2.withColumnRenamed('min', 'telefono')
df3 = df3.withColumn("fecha_carga", F.lit(datetime.now()))
df3 = df3.withColumn("detalle", F.lit('NO_PYMES'))

df3.repartition(1).write.mode(vTipo).saveAsTable(vTablaOut)

spark.stop()
timeend = datetime.now()
duracion = timeend - timestart 
print("Duracion {}".format(duracion))

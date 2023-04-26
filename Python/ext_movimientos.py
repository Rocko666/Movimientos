from datetime import datetime
from pyspark.sql import functions as F, Window
from pyspark.sql import SparkSession
from pyspark.sql import HiveContext
from pyspark.sql.types import StructType, DoubleType, DateType, StringType, FloatType, TimestampType, StructField, IntegerType, BooleanType
from pyspark import SQLContext
import argparse
import time
import sys
import os
# Genericos
sys.path.insert(1,'/var/opt/tel_spark')
from messages import *
from functions import *

timestart = datetime.now()
print(etq_info("Inicio del proceso en PySpark..."))
#Parametros definidos
VStp='Paso [1]: Cargando parametros desde la Shell..'
print(lne_dvs())
try:
    ts_step = datetime.now() 
    print(etq_info(VStp))
    print(lne_dvs())
    parser = argparse.ArgumentParser()
    parser.add_argument('--vPart', required=True, type=str)
    parser.add_argument('--vTMain', required=True, type=str)
    parser.add_argument('--vFechExt', required=True, type=str)
    parser.add_argument('--vTAjust', required=True, type=str)
    parser.add_argument('--vRutaArchXtrct', required=True, type=str)
    parametros = parser.parse_args()
    vPart=parametros.vPart
    vTMain=parametros.vTMain
    vFechExt=parametros.vFechExt
    vTAjust=parametros.vTAjust
    vRutaArchXtrct=parametros.vRutaArchXtrct
    print(etq_info(log_p_parametros("vPart",vPart)))
    print(etq_info(log_p_parametros("vTMain",vTMain)))
    print(etq_info(log_p_parametros("vFechExt",vFechExt)))
    print(etq_info(log_p_parametros("vTAjust",vTAjust)))
    print(etq_info(log_p_parametros("vRutaArchXtrct",vRutaArchXtrct)))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))

print(lne_dvs())

VStp='Paso [2]: Cargando session en PySpark..'
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    spark = SparkSession\
        .builder\
        .enableHiveSupport() \
        .config("spark.sql.broadcastTimeout", "36000") \
        .config("hive.exec.dynamic.partition", "true") \
        .config("hive.exec.dynamic.partition.mode", "nonstrict") \
        .config("spark.yarn.queue", "capa_semantica") \
        .config("hive.enforce.bucketing", "false")\
	    .config("hive.enforce.sorting", "false")\
        .getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    app_id = spark._sc.applicationId
    print(etq_info("Importando librerias personalizadas..."))
    sys.path.insert(1,'/home/nae108834/Movimientos/Python/Querys')
    from ext_movimientos_query import *
    print(lne_dvs())
    print(etq_info("INFO: Mostrar application_id => {}".format(str(app_id))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))

print(lne_dvs())

VStp='Paso [3]: Generando logica de negocio '
print(etq_info(VStp))
print(lne_dvs())
VStp='Paso [3.01]: Se obtienen los campos necesarios para el extractor de movimientos de la tabla'
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(vTMain)))
    vSQL=qry_xtrct_movs(vPart,vTAjust)
    print(etq_sql(vSQL))
    df01 = spark.sql(vSQL)
    df01 = df01.withColumn('aux',F.concat(df01.fecha_proceso.substr(7,4), df01.fecha_proceso.substr(4,2)))
    df01 = df01.withColumn('id_hash', (F.md5(F.concat(df01.telefono, df01.account_num, df01.aux).alias('id_hash'))))
    df01 = df01.withColumn('id_hash',F.upper(df01.id_hash))
    df01 = df01.filter(df01.orden == "1")
    df01 = df01.filter(df01.t_filter != "borrar")
    df01 = df01.drop("t_filter","orden","aux")
    if df01.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df01'))))
    else: 
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(vTMain)))
            query_truncate = "ALTER TABLE {} DROP IF EXISTS PARTITION (pt_fecha = {}) purge".format(vTMain,str(vFechExt))
            print(etq_info(query_truncate))
            hc=HiveContext(spark)
            hc.sql(query_truncate)
            df01.write.mode('append').insertInto(vTMain)
            df01.printSchema() 
            print(etq_info(msg_t_total_registros_hive(vTMain,str(df01.count())))) 
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(vTMain,vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive(vTMain,str(e))))
    print(etq_info("Eliminar dataframe [{}]".format('df01')))
    del df01
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))

print(lne_dvs())
VStp='Paso [3.02]: Se escribe el archivo CSV para el extractor '
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(vTMain)))
    vSQL=qry_xtrct_csv(vTMain,vFechExt)
    print(etq_sql(vSQL))
    df02=spark.sql(vSQL)
    df02 = df02.drop("pt_fecha")
    if df02.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df02'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(vTMain)))
            pandas_df = df02.toPandas()
            pandas_df = pandas_df.round(2)
            pandas_df.to_csv(vRutaArchXtrct, sep='|',index=False, encoding='utf-8-sig')
            df02.printSchema()
            print(etq_info(msg_t_total_registros_hive(vTMain,str(df02.count())))) 
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(vTMain,vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive(vTMain,str(e))))
    print(etq_info("Eliminar dataframe [{}]".format('df02')))
    del df02
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))



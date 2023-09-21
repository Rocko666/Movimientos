from datetime import datetime
from pyspark.sql import functions as F, Window
from pyspark.sql import SparkSession
from pyspark.sql import HiveContext
from pyspark.sql.types import StructType, DoubleType, DateType, StringType, FloatType, TimestampType, StructField, IntegerType, BooleanType
from pyspark.sql.functions import lit
from pyspark.sql.functions import col
from pyspark.sql.functions import concat
from pyspark.sql.functions import to_date
from pyspark import SQLContext
from pyspark_llap import HiveWarehouseSession
import argparse
import time
import sys
import os

# Genericos otc_t_360_movimientos_parque
sys.path.insert(1,'/var/opt/tel_spark')
from messages import *
from functions import *

timestart = datetime.now()
#Parametros definidos
VStp='[Paso inicial]: Cargando parametros desde la Shell:'
print(lne_dvs())
try:
    ts_step = datetime.now() 
    print(etq_info(VStp))
    print(lne_dvs())
    parser = argparse.ArgumentParser()
    parser.add_argument('--vSEntidad', required=True, type=str)
    parser.add_argument('--vSchTmp', required=True, type=str)
    parser.add_argument('--vSchRep', required=True, type=str)
    parser.add_argument('--vTAltBajHist', required=True, type=str)
    parser.add_argument('--vTAltBI', required=True, type=str)
    parser.add_argument('--vTBajBI', required=True, type=str)
    parser.add_argument('--vTTransfHist', required=True, type=str)
    parser.add_argument('--vTTrInBI', required=True, type=str)
    parser.add_argument('--vTTrOutBI', required=True, type=str)
    parser.add_argument('--vTCPHist', required=True, type=str)
    parser.add_argument('--vTCPBI', required=True, type=str)
    parser.add_argument('--vTNRCSA', required=True, type=str)
    parser.add_argument('--vTCatPosUsr', required=True, type=str)
    parser.add_argument('--vTCatPreUsr', required=True, type=str)
    parser.add_argument('--vTPivotParq', required=True, type=str)
    parser.add_argument('--f_inicio', required=True, type=str)
    parser.add_argument('--fecha_proceso', required=True, type=str)
    parser.add_argument('--fecha_movimientos', required=True, type=str)
    parser.add_argument('--fecha_movimientos_cp', required=True, type=str)
    parser.add_argument('--fecha_mes_ant_cp', required=True, type=str)
    parser.add_argument('--fecha_mes_ant', required=True, type=str)
    parser.add_argument('--f_inicio_abr', required=True, type=str)
    parser.add_argument('--f_fin_abr', required=True, type=str)
    parser.add_argument('--f_efectiva', required=True, type=str)
    parametros = parser.parse_args()
    vSEntidad=parametros.vSEntidad
    vSchTmp=parametros.vSchTmp
    vSchRep=parametros.vSchRep
    vTAltBajHist=parametros.vTAltBajHist
    vTAltBI=parametros.vTAltBI
    vTBajBI=parametros.vTBajBI
    vTTransfHist=parametros.vTTransfHist
    vTTrInBI=parametros.vTTrInBI
    vTTrOutBI=parametros.vTTrOutBI
    vTCPHist=parametros.vTCPHist
    vTCPBI=parametros.vTCPBI
    vTNRCSA=parametros.vTNRCSA
    vTCatPosUsr=parametros.vTCatPosUsr
    vTCatPreUsr=parametros.vTCatPreUsr
    vTPivotParq=parametros.vTPivotParq
    f_inicio=parametros.f_inicio
    fecha_proceso=parametros.fecha_proceso
    fecha_movimientos=parametros.fecha_movimientos
    fecha_movimientos_cp=parametros.fecha_movimientos_cp
    fecha_mes_ant_cp=parametros.fecha_mes_ant_cp
    fecha_mes_ant=parametros.fecha_mes_ant
    f_inicio_abr=parametros.f_inicio_abr
    f_fin_abr=parametros.f_fin_abr
    f_efectiva=parametros.f_efectiva
    print(etq_info(log_p_parametros("vSEntidad",vSEntidad)))
    print(etq_info(log_p_parametros("vSchTmp",vSchTmp)))
    print(etq_info(log_p_parametros("vSchTmp",vSchRep)))
    print(etq_info(log_p_parametros("vTAltBajHist",vTAltBajHist)))
    print(etq_info(log_p_parametros("vTAltBI",vTAltBI)))
    print(etq_info(log_p_parametros("vTBajBI",vTBajBI)))
    print(etq_info(log_p_parametros("vTTransfHist",vTTransfHist)))
    print(etq_info(log_p_parametros("vTTrInBI",vTTrInBI)))
    print(etq_info(log_p_parametros("vTTrOutBI",vTTrOutBI)))
    print(etq_info(log_p_parametros("vTCPHist",vTCPHist)))
    print(etq_info(log_p_parametros("vTCPBI",vTCPBI)))
    print(etq_info(log_p_parametros("vTNRCSA",vTNRCSA)))
    print(etq_info(log_p_parametros("vTCatPosUsr",vTCatPosUsr)))
    print(etq_info(log_p_parametros("vTCatPreUsr",vTCatPreUsr)))
    print(etq_info(log_p_parametros("vTPivotParq",vTPivotParq)))
    print(lne_dvs())
    print(etq_info(log_p_parametros("f_inicio",f_inicio)))
    print(etq_info(log_p_parametros("fecha_proceso",fecha_proceso)))
    print(etq_info(log_p_parametros("fecha_movimientos",fecha_movimientos)))
    print(etq_info(log_p_parametros("fecha_movimientos_cp",fecha_movimientos_cp)))
    print(etq_info(log_p_parametros("fecha_mes_ant_cp",fecha_mes_ant_cp)))
    print(etq_info(log_p_parametros("fecha_mes_ant",fecha_mes_ant)))
    print(etq_info(log_p_parametros("f_inicio_abr",f_inicio_abr)))
    print(etq_info(log_p_parametros("f_fin_abr",f_fin_abr)))
    print(etq_info(log_p_parametros("f_efectiva",f_efectiva)))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))

print(lne_dvs())

VStp='Paso [1]: Configuracion del Spark Session:'
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    spark = SparkSession\
        .builder\
        .enableHiveSupport() \
        .config("hive.exec.dynamic.partition.mode", "nonstrict") \
        .getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    hive_hwc = HiveWarehouseSession.session(spark).build()
    app_id = spark._sc.applicationId
    print(etq_info("INFO: Mostrar application_id => {}".format(str(app_id))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))

print(lne_dvs())

VStp='Paso [2]: Cargando configuraciones y nombre de tablas:'

try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs()) 
    print(etq_info("Inicio del proceso en PySpark...")) 
    print(lne_dvs())
    print(etq_info("Importando librerias personalizadas..."))
    sys.path.insert(1,'/home/nae108834/RGenerator/reportes/Cliente360/Python/Configuraciones')
    from otc_t_360_movimientos_parque_config import *
    sys.path.insert(1,'/home/nae108834/RGenerator/reportes/Cliente360/Python/Querys')
    from otc_t_360_movimientos_parque_query import *
    print(lne_dvs())
    print(etq_info("Tablas termporales del proceso..."))
    print(lne_dvs())
    vTMP06=nme_tbl_otc_t_360_movimientos_parque_06(vSchTmp)
    vTMP07=nme_tbl_otc_t_360_movimientos_parque_07(vSchTmp)
    vTMP08=nme_tbl_otc_t_360_movimientos_parque_08(vSchTmp)
    vTMP09=nme_tbl_otc_t_360_movimientos_parque_09(vSchTmp)
    vTMP10=nme_tbl_otc_t_360_movimientos_parque_10(vSchTmp)
    vTMP11=nme_tbl_otc_t_360_movimientos_parque_11(vSchTmp)
    vTMP12=nme_tbl_otc_t_360_movimientos_parque_12(vSchTmp)
    vTMP13=nme_tbl_otc_t_360_movimientos_parque_13(vSchTmp)
    vTMP14=nme_tbl_otc_t_360_movimientos_parque_14(vSchTmp)
    vTMP15=nme_tbl_otc_t_360_movimientos_parque_15(vSchTmp)
    vTMP16=nme_tbl_otc_t_360_movimientos_parque_16(vSchTmp)
    vTMP17=nme_tbl_otc_t_360_movimientos_parque_17(vSchTmp)
    vTMP18=nme_tbl_otc_t_360_movimientos_parque_18(vSchTmp)
    vTMP19=nme_tbl_otc_t_360_movimientos_parque_19(vSchTmp)
    vTMP20=nme_tbl_otc_t_360_movimientos_parque_20(vSchTmp)
    vTMP21=nme_tbl_otc_t_360_movimientos_parque_21(vSchTmp)
    vTMP22=nme_tbl_otc_t_360_movimientos_parque_22(vSchTmp)
    vTMP23=nme_tbl_otc_t_360_movimientos_parque_23(vSchTmp)
    print(etq_info(log_p_parametros('vTMP06',vTMP06)))
    print(etq_info(log_p_parametros('vTMP07',vTMP07)))
    print(etq_info(log_p_parametros('vTMP08',vTMP08)))
    print(etq_info(log_p_parametros('vTMP09',vTMP09)))
    print(etq_info(log_p_parametros('vTMP10',vTMP10)))
    print(etq_info(log_p_parametros('vTMP11',vTMP11)))
    print(etq_info(log_p_parametros('vTMP12',vTMP12)))
    print(etq_info(log_p_parametros('vTMP13',vTMP13)))
    print(etq_info(log_p_parametros('vTMP14',vTMP14)))
    print(etq_info(log_p_parametros('vTMP15',vTMP15)))
    print(etq_info(log_p_parametros('vTMP16',vTMP16)))
    print(etq_info(log_p_parametros('vTMP17',vTMP17)))
    print(etq_info(log_p_parametros('vTMP18',vTMP18)))
    print(etq_info(log_p_parametros('vTMP19',vTMP19)))
    print(etq_info(log_p_parametros('vTMP20',vTMP20)))
    print(etq_info(log_p_parametros('vTMP21',vTMP21)))
    print(etq_info(log_p_parametros('vTMP22',vTMP22)))
    print(etq_info(log_p_parametros('vTMP23',vTMP23)))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))

print(lne_dvs())

VStp='Paso [3]: Generando logica de negocio '
print(etq_info(VStp))

print(lne_dvs())
vStp='Paso [3.01]: Se inserta las altas del mes de la tabla [{}] en la tabla [{}] '.format(vTAltBI, vTAltBajHist)
try:
    ts_step = datetime.now()
    print(etq_info(vStp))
    print(lne_dvs())
    vTablaTmpAlta=vSchTmp+".tmp_mvprq_alta_baja_hist_a"
    print(etq_info("Se guardan los datos de altas en la tabla[{}]".format(vTablaTmpAlta)))
    VSQLinsrt=qry_insrt_otc_t_abh_alta(vTAltBajHist,vTAltBI,fecha_movimientos_cp, f_inicio,fecha_proceso)
    print(etq_sql(VSQLinsrt))
    df_del = spark.sql(VSQLinsrt)
    df_del.repartition(1).write.mode("overwrite").saveAsTable(vTablaTmpAlta)
    print(etq_info(msg_t_total_registros_hive(vTablaTmpAlta,str(df_del.count()))))     
    #hive_hwc.executeUpdate(VSQLinsrt)
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vStp,str(e))))


print(lne_dvs())
vStp='Paso [3.02]: Se inserta las bajas del mes de la tabla [{}] en la tabla [{}] '.format(vTBajBI, vTAltBajHist)
try:
    ts_step = datetime.now()
    print(etq_info(vStp))
    print(lne_dvs())
    vTablaTmpBaja=vSchTmp+".tmp_mvprq_alta_baja_hist_b"
    print(etq_info("Se guardan los datos de bajas en la tabla[{}]".format(vTablaTmpBaja)))    
    VSQLinsrt=qry_insrt_otc_t_abh_baja(vTAltBajHist,vTBajBI,fecha_movimientos_cp, f_inicio,fecha_proceso)
    print(etq_sql(VSQLinsrt))
    df_delb = spark.sql(VSQLinsrt)
    df_delb.repartition(1).write.mode("overwrite").saveAsTable(vTablaTmpBaja)
    print(etq_info(msg_t_total_registros_hive(vTablaTmpBaja,str(df_delb.count()))))
    print(lne_dvs())    
    print(etq_info("Se actualizan los datos en la tabla[{}]".format(vTAltBajHist)))
    VSQLovwrt = qry_ovwrt_altas_bajas(vTAltBajHist,vTablaTmpAlta,vTablaTmpBaja)
    print(etq_sql(VSQLovwrt))
    hive_hwc.executeUpdate(VSQLovwrt)
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vStp,str(e))))


print(lne_dvs())
vStp='Paso [3.03]: Se inserta los transfer_in del mes de la tabla [{}] en la tabla [{}] '.format(vTTrInBI, vTTransfHist)
try:
    ts_step = datetime.now()
    print(etq_info(vStp))
    print(lne_dvs())
    print(etq_info("Se eliminan los transfer_in  preexistentes de la tabla[{}]".format(vTTransfHist)))
    VSQLdelete=qry_dlt_otc_t_th_pre_pos(vTTransfHist, f_inicio, fecha_proceso)
    print(etq_sql(VSQLdelete))
    hive_hwc.executeUpdate(VSQLdelete)
    print(lne_dvs())
    print(etq_info(msg_i_insert_hive(vTTransfHist)))
    VSQLinsrt=qry_insrt_otc_t_th_pre_pos(vTTransfHist, vTTrInBI, fecha_movimientos_cp)
    hive_hwc.executeUpdate(VSQLinsrt)
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vStp,str(e))))
    

print(lne_dvs())
vStp='Paso [3.04]: Se inserta los transfer_out del mes de la tabla [{}] en la tabla [{}] '.format(vTTrOutBI, vTTransfHist)
try:
    ts_step = datetime.now()
    print(etq_info(vStp))
    print(lne_dvs())
    print(etq_info("Se eliminan los transfer_out preexistentes de la tabla[{}]".format(vTTransfHist)))
    VSQLdelete=qry_dlt_otc_t_th_pos_pre(vTTransfHist, f_inicio, fecha_proceso)
    print(etq_sql(VSQLdelete))
    hive_hwc.executeUpdate(VSQLdelete)
    print(lne_dvs())
    print(etq_info(msg_i_insert_hive(vTTransfHist)))
    VSQLinsrt=qry_insrt_otc_t_th_pos_pre(vTTransfHist, vTTrOutBI, fecha_movimientos_cp)
    hive_hwc.executeUpdate(VSQLinsrt)
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vStp,str(e))))
    

print(lne_dvs())
vStp='Paso [3.05]: Se inserta los cambios de plan del mes de la tabla [{}] en la tabla [{}] '.format(vTCPBI, vTCPHist)
try:
    ts_step = datetime.now()
    print(etq_info(vStp))
    print(lne_dvs())
    print(etq_info("Se eliminan los cambios de plan preexistentes de la tabla[{}]".format(vTCPHist)))
    VSQLdelete=qry_dlt_otc_t_cph(vTCPHist, f_inicio, fecha_proceso)
    print(etq_sql(VSQLdelete))
    hive_hwc.executeUpdate(VSQLdelete)
    print(lne_dvs())
    print(etq_info(msg_i_insert_hive(vTCPHist)))
    VSQLinsrt=qry_insrt_otc_t_cph(vTCPHist, vTCPBI, fecha_movimientos_cp)
    hive_hwc.executeUpdate(VSQLinsrt)
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vStp,str(e))))

print(lne_dvs())
VStp='Paso [3.06]: Insertado en comisiones  BIGD-60-para incluir movimientos [no recliclable]'
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(vTMP06)))
    vSQL=qry_otc_t_mp_no_reciclable_tmp(vTNRCSA,fecha_movimientos)
    print(etq_sql(vSQL))
    df06=spark.sql(vSQL)
    if df06.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df06'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(vTMP06)))
            df06.repartition(1).write.mode('overwrite').saveAsTable(vTMP06)
            df06.printSchema()
            print(etq_info(msg_t_total_registros_hive(vTMP06,str(df06.count())))) 
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(vTMP06,vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive(vTMP06,str(e))))
    print(etq_info("Eliminar dataframe [{}]".format('df06')))
    del df06
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))

print(lne_dvs())
VStp='Paso [3.07]: Tabla que almacena las altas de todos los dias (particiones) de un determinado mes de analisis:'
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(vTMP07)))
    vSQL=qry_tmp_mp_altas_ttls_mes(vTAltBI,f_inicio_abr,f_fin_abr)
    print(etq_sql(vSQL))
    df07=spark.sql(vSQL)
    if df07.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df07'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(vTMP07)))
            df07.repartition(1).write.mode('overwrite').saveAsTable(vTMP07)
            df07.printSchema()
            print(etq_info(msg_t_total_registros_hive(vTMP07,str(df07.count())))) 
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(vTMP07,vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive(vTMP07,str(e))))
    print(etq_info("Eliminar dataframe [{}]".format('df07')))
    del df07
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))

print(lne_dvs())
VStp='Paso [3.08]: Almacena los telefonos de:altas, t. in, t out, cambios de plan, a mes caido,(fecha eje +1) se tendra una informacion real a mes caido (yyyymm01)'
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(vTMP08)))
    vSQL=qry_tmp_mp_movs_efctvs(vTAltBI, vTTrInBI,vTTrOutBI,vTCPBI,fecha_movimientos_cp)
    print(etq_sql(vSQL))
    df08=spark.sql(vSQL)
    if df08.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df08'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(vTMP08)))
            df08.repartition(1).write.mode('overwrite').saveAsTable(vTMP08)
            df08.printSchema()
            print(etq_info(msg_t_total_registros_hive(vTMP08,str(df08.count())))) 
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(vTMP08,vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive(vTMP08,str(e))))
    print(etq_info("Eliminar dataframe [{}]".format('df08')))
    del df08
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))

print(lne_dvs())
VStp='Paso [3.09]: Se obtienen las altas bajas reproceso'
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(vTMP09)))
    vSQL=qry_otc_t_mp_alta_baja_rep(vTMP07, vTMP08)
    print(etq_sql(vSQL))
    df09=spark.sql(vSQL)
    if df09.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df09'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(vTMP09)))
            df09.repartition(1).write.mode('overwrite').saveAsTable(vTMP09)
            df09.printSchema()
            print(etq_info(msg_t_total_registros_hive(vTMP09,str(df09.count())))) 
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(vTMP09,vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive(vTMP09,str(e))))
    print(etq_info("Eliminar dataframe [{}]".format('df09')))
    del df09
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))

print(lne_dvs())
VStp='Paso [3.10]: Se obtiene el ultimo evento del alta en toda la historia hasta la fecha de proceso'
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(vTMP10)))
    vSQL=qry_otc_t_alta_hist_unic(vTAltBajHist, fecha_movimientos)
    print(etq_sql(vSQL))
    df10=spark.sql(vSQL)
    if df10.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df10'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(vTMP10)))
            df10.repartition(1).write.mode('overwrite').saveAsTable(vTMP10)
            df10.printSchema()
            print(etq_info(msg_t_total_registros_hive(vTMP06,str(df10.count())))) 
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(vTMP06,vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive(vTMP06,str(e))))
    print(etq_info("Eliminar dataframe [{}]".format('df10')))
    del df10
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))

print(lne_dvs())
VStp='Paso [3.11]: Se obtiene el ultimo evento de las bajas en toda la historia hasta la fecha de proceso'
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(vTMP11)))
    vSQL=qry_otc_t_baja_hist_unic(vTAltBajHist, fecha_movimientos)
    print(etq_sql(vSQL))
    df11=spark.sql(vSQL)
    if df11.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df11'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(vTMP11)))
            df11.repartition(1).write.mode('overwrite').saveAsTable(vTMP11)
            df11.printSchema()
            print(etq_info(msg_t_total_registros_hive(vTMP11,str(df11.count())))) 
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(vTMP11,vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive(vTMP11,str(e))))
    print(etq_info("Eliminar dataframe [{}]".format('df11')))
    del df11
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))
    
print(lne_dvs())
VStp='Paso [3.12]: Se obtiene el ultimo evento de las transferencias out en toda la historia hasta la fecha de proceso'
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(vTMP12)))
    vSQL=qry_otc_t_pos_pre_hist_unic(vTTransfHist, fecha_movimientos)
    print(etq_sql(vSQL))
    df12=spark.sql(vSQL)
    if df12.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df12'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(vTMP12)))
            df12.repartition(1).write.mode('overwrite').saveAsTable(vTMP12)
            df12.printSchema()
            print(etq_info(msg_t_total_registros_hive(vTMP12,str(df12.count())))) 
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(vTMP12,vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive(vTMP12,str(e))))
    print(etq_info("Eliminar dataframe [{}]".format('df12')))
    del df12
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))

print(lne_dvs())
VStp='Paso [3.13]: Se obtiene el ultimo evento de las transferencias in  en toda la historia hasta la fecha de proceso'
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(vTMP13)))
    vSQL=qry_otc_t_pre_pos_hist_unic(vTTransfHist, fecha_movimientos)
    print(etq_sql(vSQL))
    df13=spark.sql(vSQL)
    if df13.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df13'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(vTMP13)))
            df13.repartition(1).write.mode('overwrite').saveAsTable(vTMP13)
            df13.printSchema()
            print(etq_info(msg_t_total_registros_hive(vTMP13,str(df13.count())))) 
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(vTMP13,vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive(vTMP13,str(e))))
    print(etq_info("Eliminar dataframe [{}]".format('df13')))
    del df13
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))

print(lne_dvs())
VStp='Paso [3.14]: Se obtiene el ultimo evento de los cambios de plan en toda la historia hasta la fecha de proceso'
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(vTMP14)))
    vSQL=qry_otc_t_cambio_plan_hist_unic(vTCPHist, fecha_movimientos)
    print(etq_sql(vSQL))
    df14=spark.sql(vSQL)
    if df14.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df14'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(vTMP14)))
            df14.repartition(1).write.mode('overwrite').saveAsTable(vTMP14)
            df14.printSchema()
            print(etq_info(msg_t_total_registros_hive(vTMP14,str(df14.count())))) 
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(vTMP14,vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive(vTMP14,str(e))))
    print(etq_info("Eliminar dataframe [{}]".format('df14')))
    del df14
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))

print(lne_dvs())
VStp='Paso [3.15]: Se obtiene el ultimo evento no reciclable en toda la historia hasta la fecha de proceso'
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(vTMP15)))
    vSQL=qry_otc_t_mp_no_reciclable_tmp_unic(vTMP06, fecha_movimientos)
    print(etq_sql(vSQL))
    df15=spark.sql(vSQL)
    if df15.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df15'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(vTMP15)))
            df15.repartition(1).write.mode('overwrite').saveAsTable(vTMP15)
            df15.printSchema()
            print(etq_info(msg_t_total_registros_hive(vTMP15,str(df15.count())))) 
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(vTMP15,vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive(vTMP15,str(e))))
    print(etq_info("Eliminar dataframe [{}]".format('df15')))
    del df15
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))

print(lne_dvs())
VStp='Paso [3.16]: Tabla temporal para calculo de dias en parque prepago:'
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(vTMP16)))
    vSQL=qry_otc_t_360_no_parque_trnsfr_tmp(vTPivotParq)
    print(etq_sql(vSQL))
    df16=spark.sql(vSQL)
    if df16.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df16'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(vTMP16)))
            df16.repartition(1).write.mode('overwrite').saveAsTable(vTMP16)
            df16.printSchema()
            print(etq_info(msg_t_total_registros_hive(vTMP16,str(df16.count())))) 
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(vTMP16,vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive(vTMP16,str(e))))
    print(etq_info("Eliminar dataframe [{}]".format('df16')))
    del df16
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))

print(lne_dvs())
VStp='Paso [3.17]: Se realiza el cruce con cada tabla usando [{}] (tabla resultante de pivot_parque) y agregando los campos de cada tabla renombrandolos de acuerdo al movimiento que corresponda'.format(vTPivotParq)
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(vTMP17)))
    vSQL=qry_otc_t_360_parque_1_tmp_t_mov(vTPivotParq, vTMP10, vTMP12, vTMP13, vTMP14, vTAltBI, fecha_mes_ant_cp, vTTrOutBI, vTMP11, vTMP15, vTMP09, vTMP16)
    print(etq_sql(vSQL))
    df17=spark.sql(vSQL)
    if df17.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df17'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(vTMP17)))
            df17.repartition(1).write.mode('overwrite').saveAsTable(vTMP17)
            df17.printSchema()
            print(etq_info(msg_t_total_registros_hive(vTMP17,str(df17.count())))) 
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(vTMP17,vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive(vTMP17,str(e))))
    print(etq_info("Eliminar dataframe [{}]".format('df17')))
    del df17
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))

print(lne_dvs())
VStp='Paso [3.18]: Tabla temporal union para obtener ultimo movimiento del mes por num_telefono'
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(vTMP18)))
    vSQL=qry_otc_t_360_parque_1_mov_mes_tmp_2(vTMP14,vTMP12,vTMP13,vTMP11,vTMP15,vTMP09,vTMP10,f_inicio,fecha_proceso)
    print(etq_sql(vSQL))
    df18=spark.sql(vSQL)
    if df18.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df18'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(vTMP18)))
            df18.repartition(1).write.mode('overwrite').saveAsTable(vTMP18)
            df18.printSchema()
            print(etq_info(msg_t_total_registros_hive(vTMP18,str(df18.count())))) 
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(vTMP18,vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive(vTMP18,str(e))))
    print(etq_info("Eliminar dataframe [{}]".format('df18')))
    del df18
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))

print(lne_dvs())
VStp='Paso [3.19]: Tabla temporal para el catalogo de USUARIOS pospago'
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(vTMP19)))
    vSQL=qry_tmp_otc_t_ctl_pos_usr_nc(vTCatPosUsr, fecha_proceso)
    print(etq_sql(vSQL))
    df19=spark.sql(vSQL)
    if df19.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df19'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(vTMP19)))
            df19.repartition(1).write.mode('overwrite').saveAsTable(vTMP19)
            df19.printSchema()
            print(etq_info(msg_t_total_registros_hive(vTMP19,str(df19.count())))) 
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(vTMP19,vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive(vTMP19,str(e))))
    print(etq_info("Eliminar dataframe [{}]".format('df19')))
    del df19
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))

print(lne_dvs())
VStp='Paso [3.20]: Tabla temporal para el catalogo de USUARIOS prepago'
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(vTMP20)))
    vSQL=qry_tmp_otc_t_ctl_pre_usr_nc(vTCatPreUsr)
    print(etq_sql(vSQL))
    df20=spark.sql(vSQL)
    if df20.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df20'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(vTMP20)))
            df20.repartition(1).write.mode('overwrite').saveAsTable(vTMP20)
            df20.printSchema()
            print(etq_info(msg_t_total_registros_hive(vTMP20,str(df20.count())))) 
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(vTMP20,vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive(vTMP20,str(e))))
    print(etq_info("Eliminar dataframe [{}]".format('df20')))
    del df20
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))

print(lne_dvs())
VStp='Paso [3.21]: Se crea la tabla temp union final con datos del catalogo de usuarios'
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(vTMP21)))
    vSQL=qry_otc_t_360_parque_1_mov_mes_tmp(vTMP18,vTMP19,vTMP20)
    print(etq_sql(vSQL))
    df21=spark.sql(vSQL)
    if df21.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df21'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(vTMP21)))
            df21.repartition(1).write.mode('overwrite').saveAsTable(vTMP21)
            df21.printSchema()
            print(etq_info(msg_t_total_registros_hive(vTMP21,str(df21.count())))) 
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(vTMP21,vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive(vTMP21,str(e))))
    print(etq_info("Eliminar dataframe [{}]".format('df21')))
    del df21
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))

print(lne_dvs())
VStp='Paso [3.22]: Se crea la tabla para segmentos'
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(vTMP22)))
    vSQL=qry_otc_t_360_parque_1_mov_seg_tmp(vTMP12, vTMP13, vTMP10)
    print(etq_sql(vSQL))
    df22=spark.sql(vSQL)
    if df22.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df22'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(vTMP22)))
            df22.repartition(1).write.mode('overwrite').saveAsTable(vTMP22)
            df22.printSchema()
            print(etq_info(msg_t_total_registros_hive(vTMP22,str(df22.count())))) 
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(vTMP22,vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive(vTMP22,str(e))))
    print(etq_info("Eliminar dataframe [{}]".format('df22')))
    del df22
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))

print(lne_dvs())
VStp='Paso [3.23]: Se crea la ultima tabla del proceso'
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(vTMP23)))
    vSQL=qry_otc_t_360_parque_1_tmp_t_mov_mes(vTPivotParq, vTMP21)
    print(etq_sql(vSQL))
    df23=spark.sql(vSQL)
    if df23.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df23'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(vTMP23)))
            df23.repartition(1).write.mode('overwrite').saveAsTable(vTMP23)
            df23.printSchema()
            print(etq_info(msg_t_total_registros_hive(vTMP23,str(df23.count())))) 
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(vTMP23,vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive(vTMP23,str(e))))
    print(etq_info("Eliminar dataframe [{}]".format('df23')))
    del df23
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))
    

print(lne_dvs())
spark.stop()
timeend = datetime.now()
print(etq_info(msg_d_duracion_ejecucion(vSEntidad,vle_duracion(timestart,timeend))))
print(lne_dvs())
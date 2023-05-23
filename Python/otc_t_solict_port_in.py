import sys
reload(sys)
sys.setdefaultencoding('utf8')
from pyspark.sql import SparkSession, DataFrame
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.functions import col
from pyspark.sql.window import Window
from pyspark.sql.functions import udf
import argparse
sys.path.append('/var/opt/tel_spark')
from messages import *
from functions import *
#from query import *

timestart = datetime.now()

#------------------------------------------------------------------------ vStp00 --------------------------------------------------------------------------------#
print(lne_dvs())
vStp00 ='Paso [0]: Iniciacion de variables y parametros => Registros en variables PySaprk...'
print(etq_info(vStp00))
print(lne_dvs())

parser = argparse.ArgumentParser()
parser.add_argument('--vSEntidad', required=True, type=str, help='Entidad del Proceso')
parser.add_argument('--vTDClass', required=True, type=str, help='Clase java del conector JDBC')
parser.add_argument('--vJdbcUrl', required=True, type=str, help='URL del conector JDBC Ejm: jdbc:mysql://localhost:3306/base')
parser.add_argument('--vTDUser', required=True, type=str, help='Usuario de la base de datos')
parser.add_argument('--vTDPass', required=True, type=str, help='Clave de la base de datos')
parser.add_argument('--vTable', required=True, type=str, help='Nombre de la tabla en hive bd.tabla')
#parser.add_argument('--vtipocarga', required=True, type=str, help='Tipo de carga overwrite/append - carga completa/incremental')
#parser.add_argument('--vtdboracle', required=True, type=str,help='Parametro fecha final del traslado de datos')
#parser.add_argument('--vttbloracle', required=True, type=str,help='Parametro fecha final del traslado de datos')
#parser.add_argument('--vFuncion', required=True, type=int, help='Numero de funcion a llamar para las diferentes cargas')

parametros = parser.parse_args()
vSEntidad=parametros.vSEntidad
vTDClass=parametros.vTDClass
vJdbcUrl=parametros.vJdbcUrl
vTDUser=parametros.vTDUser
vTDPass=parametros.vTDPass
vTable=parametros.vTable
#vTipoCarga=parametros.vtipocarga
#vTdboracle=parametros.vtdboracle
#vTtbloracle=parametros.vttbloracle
#vFuncion=parametros.vFuncion

print(etq_info("Parametros del Proceso....."))
print(lne_dvs())
print(etq_info(log_p_parametros("vSEntidad",vSEntidad)))
print(etq_info(log_p_parametros("vTDClass",vTDClass)))
print(etq_info(log_p_parametros("vJdbcUrl",vJdbcUrl)))
print(etq_info(log_p_parametros("vTDUser",vTDUser)))
print(etq_info(log_p_parametros("vTDPass",vTDPass)))
#print(etq_info(log_p_parametros("vtipocarga",vTipoCarga)))
print(etq_info(log_p_parametros("vTable",vTable)))
#print(etq_info(log_p_parametros("vTipoCarga",vTipoCarga)))
#print(etq_info(log_p_parametros("vTdboracle",vTdboracle)))
#print(etq_info(log_p_parametros("vTtbloracle",vTtbloracle)))

print(lne_dvs())

#------------------------------------------------------------------------ vStp01 --------------------------------------------------------------------------------#

vStp01 ='Paso [1]: Inicio de sesion en Spark Session...'
print(etq_info(vStp01))
print(lne_dvs())

ts_step = datetime.now()

spark = SparkSession. \
    builder. \
    enableHiveSupport() \
    .config("spark.sql.broadcastTimeout", "36000") \
    .config("hive.exec.dynamic.partition", "true") \
    .config("hive.exec.dynamic.partition.mode", "nonstrict") \
    .config("spark.yarn.queue", "default") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
sc = spark.sparkContext
sc.setLogLevel("ERROR")
app_id = spark._sc.applicationId

print(etq_info("INFO: Mostrar aplication_id => {}".format(str(app_id))))
print(lne_dvs())

te_step = datetime.now()
print(etq_info(msg_d_duracion_ejecucion(vStp01,vle_duracion(ts_step,te_step))))
print(lne_dvs())

#------------------------------------------------------------------------ vStp02 --------------------------------------------------------------------------------#
vStp02 ='Paso [2]: Creacion de Dataframe con lectura desde ORACLE...'
print(etq_info(vStp02))
print(lne_dvs())

ts_step = datetime.now()

try:
    print(etq_info("Recuperando datos de la tabla de Oracle ..."))
    print(lne_dvs())
    vSQL =  ''' 
    (select   case when cr.cust_acct_number is null then cb.cust_acct_number else cr.cust_acct_number end as CustomerAccountNumber,
    case when cr.doc_number is null then cb.doc_number else cr.doc_number end doc_number,
    case when (select y.value from nc_list_values y where y.list_value_id = cb.doc_type) is null then
    (select y.value from nc_list_values y where y.list_value_id = cr.doc_type)  else
    (select y.value from nc_list_values y where y.list_value_id = cb.doc_type) end as doc_type,
    ctg.name as CustomerCategory,
    to_char( a.object_id) as PortinCommonOrderID,
    (select lv.value from nc_list_values lv where lv.list_value_id = a.request_status) as RequestStatus,
    (select lv.localized_value from vw_list_values lv where lv.list_value_id = a.ascp_response) as estado,
    to_char(a.fvc,'DD-MM-YYYY') as fvc,
    donor.name as operadora,
    to_char(a.donor_account_type) as donor_account_type1,
    (select lv.value from nc_list_values lv where lv.list_value_id = a.donor_account_type) as LN_Origen,
    u.name as AssignedCSR,
    a.created_when as created_when,
    to_char(o.object_id) as SalesOrderID,
    (select lv.value from nc_list_values lv where lv.list_value_id = o.sales_ord_status) as SalesOrderStatus,
    o.processed_when as SalesOrderProcessedDate,
    ri.name as telefono,
    substr(sim.name,1,19) as AssociatedSIMICCID,
    oi.tariff_plan_name as PlanDestino,
    a.ascp_rejection_comment  as motivo_rechazo
    from R_OM_PORTIN_CO a
    left join PROXTOMSREP_RDB.R_OM_PORTIN_CO_MPN_SR mpn on mpn.object_id = a.object_id
    left join R_RI_MOBILE_PHONE_NUMBER ri on  mpn.value = ri.object_id
    left join R_USR_USERS u on a.assigned_csr = u.object_id
    left join R_PMGT_STORE t on t.object_id = u.current_location
    left join R_CIM_RES_CUST_ACCT cr on a.customer_account = cr.object_id
    left join R_CIM_BSNS_CUST_ACCT cb on a.customer_account = cb.object_id
    left join R_PIM_CUST_CATEGORY ctg on ctg.object_id = nvl(cb.cust_category, cr.cust_category)
    left join R_RI_NUMBER_OWNER donor on donor.object_id = a.donor_operator
    left join R_AM_SIM sim on sim.object_id = ri.assoc_sim_iccid
    left join R_EH_ERROR_RECORD err on err.failed_order = a.object_id
    join R_BOE_SALES_ORD o on o.object_id = a.sales_order    
    left join R_USR_USERS u1 on u1.object_id = o.submitted_by
    left join R_BOE_ORD_ITEM oi on oi.parent_id = a.sales_order and oi.phone_number = ri.object_id
    where a.created_when >= to_date('010123','ddmmyy') and  a.created_when < to_date('010423','ddmmyy') )
    '''
    ## where a.created_when >= to_date('010322','ddmmyy') and  a.created_when < to_date('311222','ddmmyy')
    print(etq_info("Query de la tabla de Oracle a exportar a Hive ..."))    
    print(lne_dvs())
    df_solict_port_in = spark.read.format("jdbc")\
			.option("url",vJdbcUrl)\
			.option("driver",vTDClass)\
			.option("user",vTDUser)\
			.option("password",vTDPass)\
			.option("fetchsize",1000)\
			.option("dbtable",vSQL).load()
    print(etq_info(msg_t_total_registros_hive("Total registros recuperados de ORACLE: ",str( df_solict_port_in.count()))))
    df_solict_port_in.printSchema()
    # Grabando en tabla temporal de Hive 
    df_solict_port_in.write.mode('overwrite').format('parquet').saveAsTable(vTable)
    print(lne_dvs())
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vStp02,vle_duracion(ts_step,te_step))))
    print(lne_dvs())
    ts_step = datetime.now()
    print(lne_dvs())
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vTable+" Grabando los datos =>".format(''),str(e))))
te_step = datetime.now()
print(etq_info(msg_d_duracion_ejecucion(vStp02,vle_duracion(ts_step,te_step))))
print(lne_dvs())

timeend = datetime.now()
spark.stop()
print(etq_info(msg_d_duracion_ejecucion(" DURACION FINAL DE TODO EL PROCESO DE LA ENTIDAD : " + vSEntidad,vle_duracion(timestart,timeend ))))
print(lne_dvs())
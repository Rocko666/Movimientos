set -e
#!/bin/bash
##########################################################################
# Script que realiza las siguientes tareas:
#
# -Carga una particion especificada por parametro(YYYYmmDD) en esta shell
# de la tabla db_desarrollo2021.otc_t_360_general  (PySpark)    
#        
# -PySpark convierte la tabla a CSV con separador ' | ' y lo deposita 
# en la ruta de desarrollo
##########################################################################
# PARAMETROS DEL SHELL                           
# $1: Parametro de Fecha de particion de la tabla YYYYmmDD
# $2: Parametro de control de que ETAPA ejecutar 		
# _______________________________________________________________________#
# Creado 2022/22/06  (LC) Version 1.0                                    #
# Las tildes han sido omitidas intencionalmente en el script             #
#------------------------------------------------------------------------#
#																		 #
# Desarrollado por: Cristian Ortiz (NAE108834) SOFTCONSULTING            #
# Fecha de modificacion:   2022/23/06                                    #
#																		 #
#------------------------------------------------------------------------#
#																		 #
# Modificado por: XXXXXXXXXXXXX (XXXXXXXXX) SOFTCONSULTING               #
# Fecha de modificacion:   2022/07/16                                    #
##########################################################################

# sh -x /home/nae105215/RAW/TUENTI/OTC_T_TUENTI_MSISDN_BY_ACCOUNT/bin/OTC_T_TUENTI_MSISDN_BY_ACCOUNT.sh 20230704 && sh -x /home/nae105215/RAW/TUENTI/OTC_T_TUENTI_ACCOUNT/bin/OTC_T_TUENTI_ACCOUNT.sh 20230704 && sh -x /home/nae105215/ALTAS_APP_TUENTI/bin/OTC_T_ALTAS_APP_TUENTI.sh 20230704 


## sh -x /home/nae108834/Movimientos/Bin/OTC_T_360_PIVOT_PARQUE.sh 20230831  && sh -x /home/nae108834/Movimientos/Bin/OTC_T_DESC_NO_PYMES.sh && sh -x  /home/nae108834/Movimientos/Bin/OTC_T_SOLICIT_PORT_IN.sh 20230901 && sh -x /home/nae108834/Movimientos/Bin/OTC_T_COMBO_INICIAL.sh && sh -x /home/nae108834/Movimientos/Bin/OTC_T_AJUSTES_MOVIMIENTOS.sh && sh -x /home/nae108834/Movimientos/Bin/OTC_T_MOVIMIENTOS_PARQUE.sh 20230904 && sh -x /home/nae108834/Movimientos/Bin/OTC_T_360_GENERAL.sh 20230904 && sh -x /home/nae108834/Movimientos/Bin/OTC_T_EXT_MOVIMIENTOS.sh 20230731

## NUEVA EJECUCION
# sh -x /home/nae108834/Movimientos/Bin/OTC_T_360_PIVOT_PARQUE.sh 20230731 && sh -x /home/nae108834/RGenerator/reportes/Cliente360/Bin/OTC_T_360_MOVIMIENTOS_PARQUE.sh 20230731 && sh -x /home/nae108834/RGenerator/reportes/Cliente360/Bin/OTC_T_360_GENERAL.sh 20230731 && sh -x /home/nae108835/EXT_MOVIMIENTOS_TEST/Ext_Movimientos/bin/OTC_T_EXT_MOVIMIENTOS.sh 20230906

#PARAMETROS ESTATICOS
ENTIDAD=EXT_MVMNTS0010

###########################################################################################################################################################
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: Parametros definidos en la tabla params" 
###########################################################################################################################################################
ETAPA=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND parametro = 'ETAPA';"`
vRUTA=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND PARAMETRO = 'vRUTA';"`
vFTP_RUTA_OUT=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND PARAMETRO = 'vFTP_RUTA_OUT';"`
vFTP_USER_OUT=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND PARAMETRO = 'vFTP_USER_OUT';"`
vFTP_PUERTO_OUT=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND PARAMETRO = 'vFTP_PUERTO_OUT';"`
vFTP_HOSTNAME_OUT=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND PARAMETRO = 'vFTP_HOSTNAME_OUT';"`
vFTP_PASS_OUT=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND PARAMETRO = 'vFTP_PASS_OUT';"`
vFTP_NOM_ARCHIVO=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND PARAMETRO = 'vFTP_NOM_ARCHIVO';"`
vRUTA_IN=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND PARAMETRO = 'vRUTA_IN';"`
vARCH_XTRCTR=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND PARAMETRO = 'vARCH_XTRCTR';"`
vTMain=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND PARAMETRO = 'vTMain';"`
vTAjust=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND PARAMETRO = 'vTAjust';"`
vNOM_PROCESO=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND PARAMETRO = 'vNOM_PROCESO';"`
vSFTP_PUERTO_OUT=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND PARAMETRO = 'vSFTP_PUERTO_OUT';"`
vSFTP_USER_OUT=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND PARAMETRO = 'vSFTP_USER_OUT';"`
vSFTP_HOSTNAME_OUT=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND PARAMETRO = 'vSFTP_HOSTNAME_OUT';"`
vSFTP_PASS_OUT=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND PARAMETRO = 'vSFTP_PASS_OUT';"`
vSFTP_RUTA_OUT=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND PARAMETRO = 'vSFTP_RUTA_OUT';"`
vNOM_ARCHIVO_OUT=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND PARAMETRO = 'vNOM_ARCHIVO_OUT';"`

vMASTER=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND parametro = 'vMASTER';"`
vDRIVER_MEMORY=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND parametro = 'vDRIVER_MEMORY';"`
vEXECUTOR_MEMORY=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND parametro = 'vEXECUTOR_MEMORY';"`
vNUM_EXECUTORS=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND parametro = 'vNUM_EXECUTORS';"`
vNUM_EXECUTORS_CORES=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND parametro = 'vNUM_EXECUTORS_CORES';"`
###########################################################################################################################################################
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: Parametros del SPARK GENERICO" 
###########################################################################################################################################################
vRUTA_SPARK=`mysql -N  <<<"select valor from params where ENTIDAD = 'SPARK_GENERICO' AND parametro = 'VAL_RUTA_SPARK';"`
VAL_KINIT=`mysql -N  <<<"select valor from params where ENTIDAD = 'SPARK_GENERICO' AND parametro = 'VAL_KINIT';"`
$VAL_KINIT

#DIA: Obtiene la fecha del sistema
DIA=`date '+%Y%m%d'` 
#HORA: Obtiene hora del sistema
HORA=`date '+%H%M%S'` 
vLOG=$vRUTA/Log/$ENTIDAD"_"$DIA"_"$HORA.log

echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: Iniciando registro en el log.." 2>&1 &>> $vLOG
###########################################################################################################################################################
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: Parametros definidos por consola o ControlM" 2>&1 &>> $vLOG
###########################################################################################################################################################
vFECHA_EJE=$1
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: FECHAEJE => " $vFECHA_EJE 2>&1 &>> $vLOG
###########################################################################################################################################################
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: Validacion de parametros iniciales, nulos y existencia de Rutas " 2>&1 &>> $vLOG
###########################################################################################################################################################

if  [ -z "$vRUTA" ] || 
	[ -z "$vFTP_RUTA_OUT" ] || 
	[ -z "$vFTP_USER_OUT" ] ||  
	[ -z "$vFTP_PUERTO_OUT" ] || 
	[ -z "$vFTP_HOSTNAME_OUT" ] || 
	[ -z "$vFTP_PASS_OUT" ] || 
	[ -z "$vFTP_NOM_ARCHIVO" ] || 
	[ -z "$vRUTA_IN" ] || 
	[ -z "$vFECHA_EJE" ] || 
	[ -z "$vRUTA_SPARK" ] || 
	[ -z "$vARCH_XTRCTR" ] || 
	[ -z "$vTMain" ] || 
	[ -z "$vTAjust" ] || 
	[ -z "$vNOM_PROCESO" ] || 
	[ -z "$vMASTER" ] || 
	[ -z "$vDRIVER_MEMORY" ] || 
	[ -z "$vEXECUTOR_MEMORY" ] || 
	[ -z "$vNUM_EXECUTORS" ] || 
	[ -z "$vNUM_EXECUTORS_CORES" ] ; then
	echo " ERROR - uno de los parametros esta vacio o nulo" 2>&1 &>> $vLOG
	exit 1
fi

# Creacion de funcion para la exportacion del archivo a ruta FTP
function exportar()
{
    ftp -n -v  << EOF
    open ${vFTP_HOSTNAME_OUT} ${vFTP_PUERTO_OUT}
    passive
    quote USER ${vFTP_USER_OUT}
    quote PASS ${vFTP_PASS_OUT}
    ascii
    put $vARCH_XTRCTR ${vFTP_RUTA_OUT}/${vFTP_NOM_ARCHIVO_FORMATO}
EOF
}

function exportar_sftp()
{
    /usr/bin/expect << EOF
		set timeout -1
		spawn sftp ${vSFTP_USER_OUT}@${vSFTP_HOSTNAME_OUT} ${vSFTP_PUERTO_OUT}
		expect "password:"
		send "${vSFTP_PASS_OUT}\n"
		expect "sftp>"
		send "cd ${vREMOTEDIR_OUT}\n"
		expect "sftp>"
		send "put $vARCH_XTRCTR\n"
		expect "sftp>"
		send "exit \n"
		interact
EOF
}
# send "put ${vARCH_XTRCTR} $(basename ${vFTP_NOM_ARCHIVO_FORMATO})\n"
# send "put $vARCH_XTRCTR\n"
# SE PROCEDE A OBTENER EL ANIO Y MES DE LA FECHA DE EJECUCION, PARA GENERAR EL NOMBRE DE ARCHIVO CON FORMATO:
# Extractor_Movimientos_MMaaaa

eval year=`echo $vFECHA_EJE | cut -c1-4`
eval month=`echo $vFECHA_EJE | cut -c5-6`
fechaExt=$month$year
vFPartExt=$year$month
extensionArchivo='.txt'
vFTP_NOM_ARCHIVO_FORMATO=$vFTP_NOM_ARCHIVO$fechaExt$extensionArchivo

vREMOTEDIR_1=`echo $vSFTP_RUTA_OUT|sed "s/\~}</ /g"`
vREMOTEDIR_2=`echo $vREMOTEDIR_1|sed "s/\*/ /g"`
vREMOTEDIR_OUT=${vREMOTEDIR_2}

###########################################################################################################################################################
if [ "$ETAPA" = "1" ]; then
###########################################################################################################################################################
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: ETAPA 1: Extraer datos desde hive " 2>&1 &>> $vLOG
###########################################################################################################################################################
$vRUTA_SPARK \
--jars /opt/cloudera/parcels/CDH/jars/hive-warehouse-connector-assembly-*.jar \
--conf spark.sql.extensions=com.hortonworks.spark.sql.rule.Extensions \
--conf spark.security.credentials.hiveserver2.enabled=false \
--conf spark.sql.hive.hwc.execution.mode=spark \
--conf spark.datasource.hive.warehouse.read.via.llap=false \
--conf spark.datasource.hive.warehouse.load.staging.dir=/tmp \
--conf spark.datasource.hive.warehouse.read.jdbc.mode=cluster \
--conf spark.datasource.hive.warehouse.user.name="rgenerator" \
--conf spark.port.maxRetries=100 \
--py-files /opt/cloudera/parcels/CDH/lib/hive_warehouse_connector/pyspark_hwc-1.0.0.7.1.7.1000-141.zip \
--conf spark.sql.hive.hiveserver2.jdbc.url="jdbc:hive2://quisrvbigdata1.otecel.com.ec:2181,quisrvbigdata2.otecel.com.ec:2181,quisrvbigdata10.otecel.com.ec:2181,quisrvbigdata11.otecel.com.ec:2181/default;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" \
--name $ENTIDAD \
--master $vMASTER \
--driver-memory $vDRIVER_MEMORY \
--executor-memory $vEXECUTOR_MEMORY \
--num-executors $vNUM_EXECUTORS \
--executor-cores $vNUM_EXECUTORS_CORES \
$vRUTA/Python/otc_t_ext_movimientos.py \
--vTMain $vTMain \
--vTAjust $vTAjust \
--vPart $vFECHA_EJE \
--vFechExt $vFPartExt \
--vRutaArchXtrct $vARCH_XTRCTR 2>&1 &>> $vLOG

echo "= Validacion de proceso SPARK ===="`date '+%Y%m%d%H%M%S'` 2>&1 &>> $vLOG
error_spark=`egrep 'An error occurred|Caused by:|ERROR: Creando df de query|NO EXISTE TABLA|cannot resolve|Non-ASCII character|UnicodeEncodeError:|can not accept object|pyspark.sql.utils.ParseException|AnalysisException:|NameError:|IndentationError:|Permission denied:|ValueError:|ERROR:|error:|unrecognized arguments:|No such file or directory|Failed to connect|Could not open client' $vLOG | wc -l`
	if [ $error_spark -eq 0 ];then
		echo "==== OK - La ejecucion del archivo spark ext_movimientos.py es EXITOSO ===="`date '+%H%M%S'` 2>&1 &>> $vLOG
		echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: ETAPA 1 --> La carga de informacion fue extraida de manera EXITOSA" 2>&1 &>> $vLOG	
		ETAPA=2
		#SE REALIZA EL SETEO DE LA ETAPA EN LA TABLA params
		echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: Se procesa la ETAPA 1 con EXITO " 2>&1 &>> $vLOG
		`mysql -N  <<<"update params_des set valor='2' where ENTIDAD = '${ENTIDAD}' and parametro = 'ETAPA';"`
	else		
		echo "==== ERROR: - En la ejecucion del archivo spark ext_movimientos.py ====" 2>&1 &>> $vLOG
		exit 1
	fi
fi
#############################################################################################
#ETAPA 2: CREA FUNCION PARA LA EXPORTACION DEL ARCHIVO A RUTA FTP Y REALIZA LA TRANSFERENCIA
#############################################################################################

if [ "$ETAPA" = "2" ]; then
###########################################################################################################################################################
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: ETAPA 2: Exportacion del archivo a ruta FTP " 2>&1 &>> $vLOG
###########################################################################################################################################################

#REALIZA LA TRANSFERENCIA DEL ARCHIVO CSV A RUTA FTP
echo  "==== Inicia exportacion del archivo CSV del servidor: $vFTP_HOSTNAME_OUT a ruta: $vFTP_RUTA_OUT  ====" 2>&1 &>> $vLOG
#exportar $vFTP_NOM_ARCHIVO_FORMATO 2>&1 &>> $vLOG
exportar_sftp $vFTP_NOM_ARCHIVO_FORMATO 2>&1 &>> $vLOG

#VALIDA EJECUCION DE LA TRANSFERENCIA DEL ARCHIVO CSV A RUTA FTP
echo "==== Valida transferencia del archivo CSV al servidor FTP ===="`date '+%Y%m%d%H%M%S'` 2>&1 &>> $vLOG
vERROR_FTP=`egrep 'Connection timed out|Not connected|syntax is incorrect|cannot find|There is not enough space|cannot find the file specified|Permission denied|No such file or directory|cannot access' $vLOG | wc -l`
	if [ $vERROR_FTP -eq 0 ]; then
		echo "==== OK - La transferencia EXITOSA del archivo CSV al servidor FTP ===="`date '+%Y%m%d%H%M%S'` 2>&1 &>> $vLOG
		#SE REALIZA EL SETEO DE LA ETAPA EN LA TABLA params
		echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: Se procesa la ETAPA 2 con EXITO " 2>&1 &>> $vLOG
		`mysql -N  <<<"update params_des set valor='1' where ENTIDAD = '${ENTIDAD}' and parametro = 'ETAPA';"`
	else
		echo "==== ERROR - En la transferencia del archivo CSV al servidor FTP ===="`date '+%Y%m%d%H%M%S'` 2>&1 &>> $vLOG
		exit 1
	fi
fi
exit 1
# sh -x /home/nae108834/Movimientos/Bin/OTC_T_EXT_MOVIMIENTOS.sh 20221031
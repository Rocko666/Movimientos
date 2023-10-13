set -e
#!/bin/bash
###########################################################################
#   Script de carga de archivo BASE_CARGA_DESCUENTO_NO_PYMES.xlsx
#  desde FTP para subirlo a como tabla a HIVE
#--------------------------------------------------------------------------
#--------------------------------------------------------------------------
# AUTOR: Cristian Ortiz y Rodrigo Sandoval
# MODIFICADO : 24/07/2023
############################################################################

#PARAMETROS ESTATICOS
ENTIDAD=D_OTC_T_DESC_NO_PYMES1

###########################################################################################################################################################
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: Parametros definidos en la tabla params"
###########################################################################################################################################################
VAL_RUTA=$1
VAL_ESQUEMA_HIVE=$2
VAL_TABLA_HIVE=$3
VAL_SFTP_RUTA_IN=$4
VAL_NOM_FILE_IN=$5
vMASTER=$6
vDRIVER_MEMORY=$7
vEXECUTOR_MEMORY=$8
vNUM_EXECUTORS=$9
vNUM_EXECUTORS_CORES=${10}
VAL_QUEUE=${11}

###########################################################################################################################################################
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: Parametros SFTP genericos"
###########################################################################################################################################################
SFTP_GENERICO_SH=`mysql -N  <<<"select valor from params where ENTIDAD = 'SFTP_GENERICO' AND parametro = 'SFTP_GENERICO_SH';"`
VAL_SFTP_PORT_IN=`mysql -N  <<<"select valor from params where ENTIDAD = 'SFTP_GENERICO' AND parametro = 'VAL_SFTP_PORT';"`
VAL_SFTP_HOST_IN=`mysql -N  <<<"select valor from params where ENTIDAD = 'SFTP_GENERICO' AND parametro = 'VAL_SFTP_HOST';"`
VAL_SFTP_USER_IN=`mysql -N  <<<"select valor from params where ENTIDAD = 'SFTP_GENERICO' AND parametro = 'VAL_SFTP_USER_ADMVENTAS';"`
VAL_SFTP_PASS_IN=`mysql -N  <<<"select valor from params where ENTIDAD = 'SFTP_GENERICO' AND parametro = 'VAL_SFTP_PASS_ADMVENTAS';"`

#PARAMETROS CALCULADOS Y AUTOGENERADOS
VAL_DIA=`date '+%Y%m%d'`
VAL_HORA=`date '+%H%M%S'`
VAL_LOG=$VAL_RUTA/Log/$ENTIDAD"_"$VAL_DIA"_"$VAL_HORA.log
VAL_RUTA_ARCHIVO=$VAL_RUTA/inputs
VAL_TRREMOTEDIR=`echo $VAL_FTP_RUTA|sed "s/\~}</ /g"`
VAL_REMOTEDIRFINAL=${VAL_TRREMOTEDIR}

###########################################################################################################################################################
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: Parametros del SPARK GENERICO"
###########################################################################################################################################################
vRUTA_SPARK=`mysql -N  <<<"select valor from params where ENTIDAD = 'SPARK_GENERICO' AND parametro = 'VAL_RUTA_SPARK';"`
VAL_KINIT=`mysql -N  <<<"select valor from params where ENTIDAD = 'SPARK_GENERICO' AND parametro = 'VAL_KINIT';"`
$VAL_KINIT

#INICIO DEL PROCESO
echo "==== Inicia ejecucion BASE_CARGA_DESCUENTO_NO_PYMES.xlsx===="`date '+%Y%m%d%H%M%S'` 2>&1 &>> $VAL_LOG
#PASO 1: REALIZA LA TRANSFERENCIA DE LOS ARCHIVOS CSV DESDE EL SERVIDOR FTP A RUTA LOCAL EN BIGDATA

echo "==== Realiza la transferencia de BASE_CARGA_DESCUENTO_NO_PYMES.xlsx desde el servidor FTP a BigData ===="`date '+%Y%m%d%H%M%S'` 2>&1 &>> $VAL_LOG
echo "Servidor: $VAL_SFTP_HOST_IN" 2>&1 &>> $VAL_LOG
echo "Puerto: $VAL_SFTP_PORT_IN" 2>&1 &>> $VAL_LOG
echo "Ruta: $VAL_SFTP_RUTA_IN" 2>&1 &>> $VAL_LOG

#PARAMETROS PARA IMPORTACION
VAL_MODO=1
VAL_BANDERA_FTP=0
VAL_LOCAL_RUTA_IN=$VAL_RUTA_ARCHIVO

if  [ -z "$ENTIDAD" ] ||
	[ -z "$VAL_RUTA" ] ||
	[ -z "$VAL_ESQUEMA_HIVE" ] ||
	[ -z "$VAL_TABLA_HIVE" ] ||
	[ -z "$vMASTER" ] ||
	[ -z "$vDRIVER_MEMORY" ] ||
	[ -z "$vEXECUTOR_MEMORY" ] ||
	[ -z "$vNUM_EXECUTORS" ] ||
	[ -z "$vNUM_EXECUTORS_CORES" ] ||
	[ -z "$SFTP_GENERICO_SH" ] ||
	[ -z "$VAL_MODO" ] ||
	[ -z "$VAL_BANDERA_FTP" ] ||
	[ -z "$VAL_SFTP_USER_IN" ] ||
	[ -z "$VAL_SFTP_PASS_IN" ] ||
	[ -z "$VAL_SFTP_HOST_IN" ] ||
	[ -z "$VAL_SFTP_PORT_IN" ] ||
	[ -z "$VAL_SFTP_RUTA_IN" ] ||
	[ -z "$VAL_NOM_FILE_IN" ] ||
	[ -z "$VAL_LOCAL_RUTA_IN" ] ||
	[ -z "$VAL_LOG" ]; then
	echo " ERROR - uno de los parametros esta vacio o nulo"
	exit 1
fi

sh -x $SFTP_GENERICO_SH $VAL_MODO $VAL_BANDERA_FTP $VAL_SFTP_USER_IN $VAL_SFTP_PASS_IN $VAL_SFTP_HOST_IN $VAL_SFTP_PORT_IN $VAL_SFTP_RUTA_IN $VAL_NOM_FILE_IN $VAL_LOCAL_RUTA_IN $VAL_LOG

#VALIDA LA TRANSFERENCIA DE LOS ARCHIVOS DESDE EL SERVIDOR FTP A BIGDATA
VAL_ERRORES=`egrep 'ERROR - En la transferencia del archivo' $VAL_LOG | wc -l`
if [ $VAL_ERRORES -ne 0 ];then
	echo "=== ERROR en la transferencia SFTP $VAL_NOM_FILE_IN" 2>&1 &>>$VAL_LOG
	exit 1
else
	echo "==== Transferencia SFTP $VAL_NOM_FILE_IN realizada con EXITO ====" 2>&1 &>>$VAL_LOG
fi

echo "==== Finaliza la transferencia de DESCUENTOS_NO_PYMES.xlsx===="`date '+%Y%m%d%H%M%S'` 2>&1 &>> $VAL_LOG

#HACE EL LLAMADO AL PYTHON QUE REALIZA LA CONVERSION DEL ARCHIVO  XLSX a tabla en Hive

echo "==== Hace el llamado al python que realiza la conversion del archivo xlsx a tabla en Hive ====" 2>&1 &>> $VAL_LOG
$vRUTA_SPARK \
--conf spark.port.maxRetries=100 \
--name $ENTIDAD \
--queue $VAL_QUEUE \
--master $vMASTER \
--driver-memory $vDRIVER_MEMORY \
--executor-memory $vEXECUTOR_MEMORY \
--num-executors $vNUM_EXECUTORS \
--executor-cores $vNUM_EXECUTORS_CORES \
$VAL_RUTA/Python/otc_t_desc_no_pymes.py \
--rutain=${VAL_RUTA}/inputs/$VAL_NOM_FILE_IN \
--tablaout=$VAL_ESQUEMA_HIVE.$VAL_TABLA_HIVE \
--tipo=overwrite 2>&1 &>> $VAL_LOG

#VALIDA EJECUCION DEL PYTHON
echo "==== Valida ejecucion del python que hace la conversion de excel a Hive ====" 2>&1 &>> $VAL_LOG
error_py=`egrep 'AnalysisException|TypeError:|FAILED:|Error|Table not found|Table already exists|Vertex|No such file or directory' $LOGS/$EJECUCION.err | wc -l`
if [ $error_py -eq 0 ];then
	echo "==== OK - La ejecucion del python  es EXITOSO ====" 2>&1 &>> $VAL_LOG
else
	echo "==== ERROR - En la ejecucion del python  ====" 2>&1 &>> $VAL_LOG
	exit 1
fi

echo "==== Proceso finaliza con EXITO ====" 2>&1 &>> $VAL_LOG
set -e
#!/bin/bash
############################################################################
#   Script de carga de archivo .xlsx catalogo de usuarios prepago		   #
#  desde FTP para subirlo a como tabla a HIVE							   #
#--------------------------------------------------------------------------#
#--------------------------------------------------------------------------#
# REALIZADO: CRISTIAN ORTIZ												   #
# MODIFICADO : 23/NOV/2022									    		   #
# COMENTARIO:                                                              #
# "							" 											   #
############################################################################

ENTIDAD=OTC_T_CTL_PRE_USR_NC

#PARAMETROS GENERICOS DEFINIDOS EN LA TABLA params_des

VAL_RUTA=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND PARAMETRO = 'VAL_RUTA';"` 
ETAPA=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND PARAMETRO = 'ETAPA';"` 

VAL_FTP_HOSTNAME=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND PARAMETRO = 'VAL_FTP_HOSTNAME';"` 
VAL_FTP_PUERTO=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND PARAMETRO = 'VAL_FTP_PUERTO';"` 
VAL_FTP_USER=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND PARAMETRO = 'VAL_FTP_USER';"` 
VAL_FTP_PASS=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND PARAMETRO = 'VAL_FTP_PASS';"` 
VAL_FTP_RUTA=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND PARAMETRO = 'VAL_FTP_RUTA';"` 
VAL_FTP_NOM_ARCHIVO=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND PARAMETRO = 'VAL_FTP_NOM_ARCHIVO';"` 
VAL_DIR_HDFS_CAT=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND PARAMETRO = 'VAL_DIR_HDFS_CAT';"` 

###########################################################################################################################################################
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: Parametros del SPARK GENERICO" 
###########################################################################################################################################################
vRUTA_SPARK=`mysql -N  <<<"select valor from params where ENTIDAD = 'SPARK_GENERICO' AND parametro = 'VAL_RUTA_SPARK';"`
VAL_KINIT=`mysql -N  <<<"select valor from params where ENTIDAD = 'SPARK_GENERICO' AND parametro = 'VAL_KINIT';"`
$VAL_KINIT

#PARAMETROS CALCULADOS Y AUTOGENERADOS
VAL_DIA=`date '+%Y%m%d'` 
VAL_HORA=`date '+%H%M%S'` 
VAL_LOG=$VAL_RUTA/Log/$ENTIDAD"_"$VAL_DIA"_"$VAL_HORA.log
VAL_RUTA_ARCHIVO=$VAL_RUTA/Input
VAL_TRREMOTEDIR=`echo $VAL_FTP_RUTA|sed "s/\~}</ /g"`
VAL_REMOTEDIRFINAL=${VAL_TRREMOTEDIR}

#VALIDACION DE PARAMETROS INICIALES
if  [ -z "$ENTIDAD" ] || 
	[ -z "$VAL_RUTA" ]  || 
	[ -z "$ETAPA" ] || 
	[ -z "$VAL_FTP_HOSTNAME" ] || 
	[ -z "$VAL_FTP_PUERTO" ] || 
	[ -z "$VAL_FTP_USER" ] || 
	[ -z "$VAL_FTP_PASS" ] || 
	[ -z "$VAL_FTP_RUTA" ] || 
	[ -z "$VAL_DIR_HDFS_CAT" ]; then
	echo " ERROR - uno de los parametros esta vacio o nulo"
	exit 1
fi

#INICIO DEL PROCESO
echo "==== Inicia ejecucion catalogo de usuarios prepago===="`date '+%Y%m%d%H%M%S'` 2>&1 &>> $VAL_LOG
#PASO 1: REALIZA LA TRANSFERENCIA DE LOS ARCHIVOS CSV DESDE EL SERVIDOR FTP A RUTA LOCAL EN BIGDATA
if [ "$ETAPA" = "1" ]; then
echo "==== Realiza la transferencia del catalogo de usuarios prepago desde el servidor FTP a BigData ===="`date '+%Y%m%d%H%M%S'` 2>&1 &>> $VAL_LOG
echo "Servidor: $VAL_FTP_HOSTNAME" 2>&1 &>> $VAL_LOG
echo "Puerto: $VAL_FTP_PUERTO" 2>&1 &>> $VAL_LOG
echo "Ruta: $VAL_FTP_RUTA" 2>&1 &>> $VAL_LOG

ftp -inv $VAL_FTP_HOSTNAME $VAL_FTP_PUERTO <<EOF 2>&1 &>> $VAL_LOG
user $VAL_FTP_USER $VAL_FTP_PASS
bin
pwd
cd ${VAL_REMOTEDIRFINAL}
lcd ${VAL_RUTA_ARCHIVO}
mget ${VAL_FTP_NOM_ARCHIVO}
bye
EOF

#VALIDA LA TRANSFERENCIA DE LOS ARCHIVOS DESDE EL SERVIDOR FTP A BIGDATA
echo "==== Valida la transferencia de los archivos desde el servidor FTP a BigData ===="`date '+%Y%m%d%H%M%S'` 2>&1 &>> $VAL_LOG
error_trnsf=`egrep 'Connection timed out|Not connected|syntax is incorrect|cannot find|There is not enough space|cannot find the file specified|Permission denied|No such file or directory|cannot access' $VAL_LOG | wc -l`
if [ $error_trnsf -eq 0 ];then
	echo "==== OK - La transferencia de los archivos desde el servidor FTP se realiza con EXITO ===="`date '+%Y%m%d%H%M%S'` 2>&1 &>> $VAL_LOG
	else
	echo "==== ERROR - En la transferencia de los archivos desde el servidor FTP ===="`date '+%Y%m%d%H%M%S'` 2>&1 &>> $VAL_LOG
    exit 1
fi
fi

echo "==== Finaliza la transferencia del catalogo de usuarios prepago===="`date '+%Y%m%d%H%M%S'` 2>&1 &>> $VAL_LOG

ETAPA=2
#HACE EL LLAMADO AL PYTHON QUE REALIZA LA CONVERSION DEL ARCHIVO  XLS a tabla en Hive
if [ "$ETAPA" = "2" ]; then
echo "==== Hace el llamado al python que realiza la conversion del archivo xls a tabla en Hive ====" 2>&1 &>> $VAL_LOG
$vRUTA_SPARK \
 
--conf spark.shuffle.service.enabled=true \

--conf spark.port.maxRetries=100 \
--name $ENTIDAD \
--master yarn \
--executor-memory 5G \
--num-executors 4 \
--executor-cores 6 \
--driver-memory 4G \
${VAL_RUTA}/Python/otc_t_ctl_pre_usr_nc.py \
--rutain=${VAL_RUTA}/Input/$VAL_FTP_NOM_ARCHIVO \
--tablaout=$VAL_DIR_HDFS_CAT \
--tipo=overwrite 2>&1 &>> $VAL_LOG

#VALIDA EJECUCION DEL PYTHON
echo "==== Valida ejecucion del python que hace la conversion de excel a Hive ====" 2>&1 &>> $VAL_LOG
error_py=`egrep 'AnalysisException|TypeError:|FAILED:|Error|Table not found|Table already exists|Vertex|No such file or directory' $VAL_LOG | wc -l`
if [ $error_py -eq 0 ];then
			echo "==== OK - La ejecucion del python  es EXITOSO ====" 2>&1 &>> $VAL_LOG
		else
			echo "==== ERROR - En la ejecucion del python  ====" 2>&1 &>> $VAL_LOG
			exit 1
fi		
fi

### sh -x /home/nae108834/Catalogo_usuarios_prepago/Bin/OTC_T_CTL_PRE_USR_NC.sh
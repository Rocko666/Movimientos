set -e
#########################################################################################################
# NOMBRE: OTC_T_SOLICIT_PORT_IN.sh  		      												                        
# DESCRIPCION:																							                                            
# Shell que carga los datos desde Oracle a Hive		                                                 											             
# AUTOR: Cristian Ortiz - Softconsulting             														                          
# FECHA CREACION: 2022-08-26																			                                      
# PARAMETROS DEL SHELL                            													                            
# $1: Parametro de Fecha Inicial del proceso a ejecutar  								        		                    						                	
#########################################################################################################
# MODIFICACIONES																						                                            
# FECHA  		AUTOR     		DESCRIPCION MOTIVO		
# 2022-11-17	Cristian Ortiz (Softconsulting) BIGD-60 - Cambios en la shell parametrizando                                                                                                    
#########################################################################################################

##############
# VARIABLES #
##############

ENTIDAD=OTC_T_SOLICIT_PORT_IN
SPARK_GENERICO=SPARK_GENERICO

SHELL=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'SHELL';"`

VAL_HORA=`date '+%Y%m%d%H%M%S'`
VAL_RUTA_LOG=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'RUTA_LOG';"`
VAL_LOG=$VAL_RUTA_LOG/$ENTIDAD"_"$VAL_HORA.log
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: Iniciando registro en el log.." 2>&1 &>> $VAL_LOG
###########################################################################################################################################################
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: Parametros definidos en la tabla params" 2>&1 &>> $VAL_LOG
###########################################################################################################################################################
VAL_RUTA=`mysql -N <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'RUTA';"` 
HIVEDB=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'HIVEDB';"`         
HIVETABLE=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'HIVETABLE';"`        
RUTA_PYTHON=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'RUTA_PYTHON';"` 
VAL_MASTER=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_MASTER';"`
VAL_DRIVER_MEMORY=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_DRIVER_MEMORY';"`
VAL_EXECUTOR_MEMORY=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_EXECUTOR_MEMORY';"`
VAL_NUM_EXECUTORS=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_NUM_EXECUTORS';"`
VAL_NUM_EXECUTORS_CORES=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_NUM_EXECUTORS_CORES';"`
VAL_COLA_EJECUCION=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'QUEUE';"`
ETAPA=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'ETAPA';"`

###################################################################################################################
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: Obtener y validar parametros genericos SPARK..." 2>&1 &>> $VAL_LOG
###################################################################################################################
TDCLASS=`mysql -N  <<<"select valor from params where entidad = 'SPARK_GENERICO'  AND parametro = 'TDCLASS_ORC';"`
VAL_RUTA_SPARK=`mysql -N  <<<"select valor from params where entidad = 'SPARK_GENERICO'  AND parametro = 'VAL_RUTA_SPARK';"`
VAL_RUTA_LIB=`mysql -N  <<<"select valor from params where entidad = 'SPARK_GENERICO'  AND parametro = 'VAL_RUTA_LIB';"`
VAL_LIB=`mysql -N  <<<"select valor from params where entidad = 'SPARK_GENERICO'  AND parametro = 'VAL_NOM_JAR_ORC_11';"`

###################################################################################################################
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: Obtener y validar parametros de oracle definidos en la tabla params..." 2>&1 &>> $VAL_LOG
###################################################################################################################
TDDB=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'TDDB';"`
TDUSER=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'TDUSER';"`
TDPASS=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'TDPASS';"`
TDHOST=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'TDHOST';"`
TDPORT=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'TDPORT';"`

###########################################################################################################################################################
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: Definir parametros por consola o ControlM" 2>&1 &>> $VAL_LOG
###########################################################################################################################################################
VAL_FECHA_PROCESO=$1
###########################################################################################################################################################
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: Validacion de parametros iniciales, nulos y existencia de Rutas " 2>&1 &>> $VAL_LOG
###########################################################################################################################################################
if  [ -z "$ENTIDAD" ] || 
	[ -z "$SPARK_GENERICO" ] || 
	[ -z "$SHELL" ] || 
	[ -z "$VAL_HORA" ] || 
	[ -z "$VAL_RUTA_LOG" ] || 
	[ -z "$VAL_LOG" ] || 
	[ -z "$VAL_RUTA" ] || 
	[ -z "$HIVEDB" ] || 
	[ -z "$HIVETABLE" ] || 
	[ -z "$RUTA_PYTHON" ] || 
	[ -z "$VAL_MASTER" ] || 
	[ -z "$VAL_DRIVER_MEMORY" ] || 
	[ -z "$VAL_EXECUTOR_MEMORY" ] || 
	[ -z "$VAL_NUM_EXECUTORS" ] || 
	[ -z "$VAL_NUM_EXECUTORS_CORES" ] || 
	[ -z "$VAL_COLA_EJECUCION" ] || 
	[ -z "$ETAPA" ] ||
    [ -z "$TDCLASS" ] ||
	[ -z "$VAL_RUTA_SPARK" ] ||
	[ -z "$VAL_RUTA_LIB" ] ||
	[ -z "$VAL_LIB" ] ||
	[ -z "$TDDB" ] ||
	[ -z "$TDUSER" ] ||
	[ -z "$TDPASS" ] ||
	[ -z "$TDHOST" ] ||
	[ -z "$TDPORT" ]; then
echo `date '+%Y-%m-%d %H:%M:%S'`" ERROR: $TIME [ERROR] $rc unos de los parametros esta vacio o es nulo" 2>&1 &>> $VAL_LOG
error=1
exit $error
fi
###########################################################################################################################################################
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: Parametros calculados de fechas  " 2>&1 &>> $VAL_LOG
###########################################################################################################################################################
FECHA_EJECUCION=`date '+%Y%m01' -d "$VAL_FECHA_PROCESO"`
VAL_F_INICIAL=$(date -d "$FECHA_EJECUCION -6 month" +%Y%m%d)
JDBCURL1=jdbc:oracle:thin:@$TDHOST:$TDPORT/$TDDB
#$TDDB

if  [ -z "$ETAPA" ] || 
	[ -z "$VAL_F_INICIAL" ] || 
	[ -z "$FECHA_EJECUCION" ] || 
	[ -z "$JDBCURL1" ]; then
  echo `date '+%Y-%m-%d %H:%M:%S'`" ERROR: $TIME [ERROR] $rc unos de los parametros calculados esta vacio o es nulo" 2>&1 &>> $VAL_LOG
  error=1
  exit $error
fi


MES_F=$(echo "$FECHA_EJECUCION" | cut -c 5-6)
ANIO_F=$(echo "$FECHA_EJECUCION" | cut -c 3-4)
FECHA_FIN="01${MES_F}${ANIO_F}"

MES_I=$(echo "$VAL_F_INICIAL" | cut -c 5-6)
ANIO_I=$(echo "$VAL_F_INICIAL" | cut -c 3-4)
FECHA_INI="01${MES_I}${ANIO_I}"


echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: FECHA_INI => " $FECHA_INI 2>&1 &>> $VAL_LOG
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: FECHA_FIN => " $FECHA_FIN 2>&1 &>> $VAL_LOG
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: JDBCURL1 => " $JDBCURL1 2>&1 &>> $VAL_LOG

if [ "$ETAPA" = "1" ]; then
###########################################################################################################################################################
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: ETAPA 1: Oracle Import " 2>&1 &>> $VAL_LOG
###########################################################################################################################################################
$VAL_RUTA_SPARK \
 
--conf spark.shuffle.service.enabled=true \

--conf spark.port.maxRetries=100 \
--name $ENTIDAD \
--master $VAL_MASTER \
--driver-memory $VAL_DRIVER_MEMORY \
--executor-memory $VAL_EXECUTOR_MEMORY \
--num-executors $VAL_NUM_EXECUTORS \
--executor-cores $VAL_NUM_EXECUTORS_CORES \
--jars $VAL_RUTA_LIB/$VAL_LIB \
$RUTA_PYTHON/otc_t_solict_port_in.py \
--vSEntidad=$ENTIDAD \
--vTable=$HIVEDB.$HIVETABLE \
--vJdbcUrl=$JDBCURL1 \
--vTDPass=$TDPASS \
--vTDUser=$TDUSER \
--vFIni=$FECHA_INI \
--vFFin=$FECHA_FIN \
--vTDClass=$TDCLASS 2>&1 &>> $VAL_LOG

	# Validamos el LOG de la ejecucion, si encontramos errores finalizamos con error >0
	VAL_ERRORES=`egrep 'NODATA:|ERROR:|FAILED:|Error|Table not found|Table already exists|Vertex|Permission denied|cannot resolve' $VAL_LOG | wc -l`
	if [ $VAL_ERRORES -ne 0 ];then
		echo `date '+%Y-%m-%d %H:%M:%S'`" ERROR: ETAPA 1 --> Problemas en la carga de informacion a ORACLE " 2>&1 &>> $VAL_LOG
		exit 1																																 
	else
		echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: ETAPA 1 --> La carga de informacion a ORACLE fue ejecutada de manera EXITOSA" 2>&1 &>> $VAL_LOG	
		ETAPA=2
		#SE REALIZA EL SETEO DE LA ETAPA EN LA TABLA params
		echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: $SHELL --> Se procesa la ETAPA 2 con EXITO " 2>&1 &>> $VAL_LOG
		`mysql -N  <<<"update params set valor='2' where ENTIDAD = '${ENTIDAD}' and parametro = 'ETAPA';"`
	fi
fi

if [ "$ETAPA" = "2" ]; then
###########################################################################################################################################################
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: ETAPA 2: Finalizar el proceso " 2>&1 &>> $VAL_LOG
###########################################################################################################################################################

	#SE REALIZA EL SETEO DE LA ETAPA EN LA TABLA params
	`mysql -N  <<<"update params set valor='1' where ENTIDAD = '${ENTIDAD}' and parametro = 'ETAPA';"`

	echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: El proceso OTC_T_SOLICIT_PORT_IN finaliza correctamente " 2>&1 &>> $VAL_LOG
fi

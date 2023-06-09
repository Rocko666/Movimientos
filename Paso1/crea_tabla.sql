-- DESARROLLO
DROP TABLE IF EXISTS db_desarrollo2021.otc_t_rtd_oferta_sugerida;
CREATE TABLE `db_desarrollo2021.otc_t_rtd_oferta_sugerida`(
  `abonado_cd` string, 
  `numero_telefono` string, 
  `linea_negocio` string, 
  `app` decimal(1,0), 
  `flag_lista_blanca` decimal(1,0), 
  `grupo_prepago` string, 
  `scoring` decimal(1,0), 
  `bancarizado` decimal(1,0), 
  `datos` decimal(1,0), 
  `combero` decimal(1,0), 
  `ultimo_combo` string, 
  `ticket_promedio` decimal(5,2), 
  `limite_credito` decimal(5,2), 
  `es_smartphone` string, 
  `ultimo_combo_ub` string, 
  `tipo_consumidor` string,
  `movimiento` string,
  `oferta_sugerida` string, 
  `beneficio` string, 
  `fecha_inicio_benef` date, 
  `fecha_fin_benef` date,
  `duracion_oferta` int,
  `script_oferta` string,
  `cod_activacion_beneficio` string,
  `gatillador_beneficio_os` string,
  `saldo` decimal(5,2),
  `valor_oferta` int,
  `duracion_dias` string,
  `duracion_beneficio` string,
  `combo_complemento` string
  )COMMENT 'Tabla particionada con informacion diaria de oferta sugerida'
PARTITIONED BY ( 
fecha_proceso bigint COMMENT 'Fecha de proceso que corresponde a la particion') 
CLUSTERED BY ( 
  numero_telefono) 
INTO 1 BUCKETS
STORED as ORC tblproperties ('orc.compress' = 'SNAPPY');

-- PRODUCCION
DROP TABLE IF EXISTS db_reportes.otc_t_rtd_oferta_sugerida;
CREATE TABLE db_reportes.otc_t_rtd_oferta_sugerida(
  abonado_cd string, 
  numero_telefono string, 
  linea_negocio string, 
  app decimal(1,0), 
  flag_lista_blanca decimal(1,0), 
  grupo_prepago string, 
  scoring decimal(1,0), 
  bancarizado decimal(1,0), 
  datos decimal(1,0), 
  combero decimal(1,0), 
  ultimo_combo string, 
  ticket_promedio decimal(5,2), 
  limite_credito decimal(5,2), 
  es_smartphone string, 
  ultimo_combo_ub string, 
  tipo_consumidor string,
  movimiento string,
  oferta_sugerida string, 
  beneficio string, 
  fecha_inicio_benef date, 
  fecha_fin_benef date,
  duracion_oferta int,
  script_oferta string,
  cod_activacion_beneficio string,
  gatillador_beneficio_os string,
  saldo decimal(5,2),
  valor_oferta int,
  duracion_dias string,
  duracion_beneficio string,
  combo_complemento string
  )COMMENT 'Tabla particionada con informacion diaria de oferta sugerida'
PARTITIONED BY ( 
fecha_proceso bigint COMMENT 'Fecha de proceso que corresponde a la particion') 
CLUSTERED BY ( 
  numero_telefono) 
INTO 1 BUCKETS
STORED as ORC tblproperties ('transactional'='false','orc.compress' = 'SNAPPY');



---MOVIMIENTOS PARQUE

----db_reportes.otc_t_alta_baja_hist
drop table if exists db_desarrollo2021.otc_t_alta_baja_hist_cloudera;
CREATE TABLE db_desarrollo2021.otc_t_alta_baja_hist_cloudera(
--drop table if exist db_reportes.otc_t_alta_baja_hist;
--CREATE TABLE db_reportes.otc_t_alta_baja_hist(
  `tipo` varchar(20), 
  `telefono` varchar(9), 
  `fecha` date, 
  `canal` varchar(50), 
  `sub_canal` varchar(50), 
  `nuevo_sub_canal` varchar(50), 
  `portabilidad` varchar(10), 
  `operadora_origen` varchar(20), 
  `operadora_destino` varchar(20), 
  `motivo` varchar(50), 
  `distribuidor` varchar(50), 
  `oficina` varchar(50))
--STORED AS PARQUET TBLPROPERTIES ("transactional"="false",'parquet.compression' = 'SNAPPY');
STORED AS ORC TBLPROPERTIES ("transactional"="true");

----db_reportes.otc_t_transfer_hist
CREATE TABLE db_desarrollo2021.otc_t_transfer_hist_cloudera(
--CREATE TABLE db_reportes.otc_t_transfer_hist(
  `tipo` varchar(20), 
  `telefono` varchar(9), 
  `fecha` date, 
  `canal` varchar(50), 
  `sub_canal` varchar(50), 
  `nuevo_sub_canal` varchar(50), 
  `distribuidor` varchar(50), 
  `oficina` varchar(50))
CLUSTERED BY (telefono) INTO 4 BUCKETS
STORED AS ORC TBLPROPERTIES ("transactional"="true");

----db_reportes.otc_t_cambio_plan_hist
CREATE TABLE db_desarrollo2021.otc_t_cambio_plan_hist_cloudera(
--CREATE TABLE db_reportes.otc_t_cambio_plan_hist(
  `tipo` varchar(20), 
  `telefono` varchar(9), 
  `fecha` date, 
  `canal` varchar(50), 
  `sub_canal` varchar(50), 
  `nuevo_sub_canal` varchar(50), 
  `distribuidor` varchar(50), 
  `oficina` varchar(50), 
  `cod_plan_anterior` varchar(10), 
  `des_plan_anterior` varchar(50), 
  `tb_descuento` double, 
  `tb_override` double, 
  `delta` double)
CLUSTERED BY (telefono) INTO 4 BUCKETS
STORED AS ORC TBLPROPERTIES ("transactional"="true");

----db_reportes.otc_t_cambio_plan_hist





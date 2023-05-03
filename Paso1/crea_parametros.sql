
---PARAMETROS PARA LA ENTIDAD OTC_T_360_PIVOT_PARQUE
DELETE FROM params_des WHERE entidad='OTC_T_360_PIVOT_PARQUE';
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_PIVOT_PARQUE','HIVEDB','db_desarrollo2021','0','1');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_PIVOT_PARQUE','VAL_ESQUEMA_TMP','db_desarrollo2021','0','1');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_PIVOT_PARQUE','HIVETABLE','otc_t_360_parque_1_tmp','0','1');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_PIVOT_PARQUE','RUTA','/home/nae108834/Movimientos','0','1');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_PIVOT_PARQUE','SHELL','/home/nae108834/Movimientos/Bin/OTC_T_360_PIVOT_PARQUE.sh','0','1');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_PIVOT_PARQUE','RUTA_LOG','/home/nae108834/Movimientos/Log','0','1');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_PIVOT_PARQUE','ETAPA','1','0','1');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_PIVOT_PARQUE','RUTA_PYTHON','/home/nae108834/Movimientos/Python','0','1');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_PIVOT_PARQUE','vTAltasBi','db_cs_altas.otc_t_altas_bi','0','1');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_PIVOT_PARQUE','vTTransferOutBi','db_cs_altas.otc_t_transfer_out_bi','0','1');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_PIVOT_PARQUE','vTTransferInBi','db_cs_altas.otc_t_transfer_in_bi','0','1');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_PIVOT_PARQUE','vTCPBi','db_cs_altas.otc_t_cambio_plan_bi','0','1');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_PIVOT_PARQUE','vTBajasInv','db_cs_altas.otc_t_bajas_involuntarias','0','1');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_PIVOT_PARQUE','vTChurnSP2','db_cs_altas.otc_t_churn_sp2','0','1');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_PIVOT_PARQUE','vTCFact','db_rbm.otc_t_vw_cta_facturacion','0','1');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_PIVOT_PARQUE','vTPRMANDATE','db_rbm.otc_t_prmandate','0','1');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_PIVOT_PARQUE','vTBajasBi','db_cs_altas.otc_t_bajas_bi','0','1');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_PIVOT_PARQUE','vTRiMobPN','db_rdb.otc_t_r_ri_mobile_phone_number','0','1');

INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_PIVOT_PARQUE','VAL_ETP01_MASTER','yarn','0','1');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_PIVOT_PARQUE','VAL_ETP01_DRIVER_MEMORY','16G','0','1');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_PIVOT_PARQUE','VAL_ETP01_EXECUTOR_MEMORY','16G','0','1');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_PIVOT_PARQUE','VAL_ETP01_NUM_EXECUTORS','8','0','1');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_PIVOT_PARQUE','VAL_ETP01_NUM_EXECUTORS_CORES','8','0','1');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_PIVOT_PARQUE','VAL_ETP02_MASTER','yarn','0','1');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_PIVOT_PARQUE','VAL_ETP02_DRIVER_MEMORY','16G','0','1');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_PIVOT_PARQUE','VAL_ETP02_EXECUTOR_MEMORY','16G','0','1');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_PIVOT_PARQUE','VAL_ETP02_NUM_EXECUTORS','8','0','1');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_PIVOT_PARQUE','VAL_ETP02_NUM_EXECUTORS_CORES','8','0','1');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_PIVOT_PARQUE','VAL_ETP03_MASTER','yarn','0','1');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_PIVOT_PARQUE','VAL_ETP03_DRIVER_MEMORY','16G','0','1');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_PIVOT_PARQUE','VAL_ETP03_EXECUTOR_MEMORY','16G','0','1');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_PIVOT_PARQUE','VAL_ETP03_NUM_EXECUTORS','8','0','1');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_PIVOT_PARQUE','VAL_ETP03_NUM_EXECUTORS_CORES','8','0','1');
SELECT * FROM params_des WHERE ENTIDAD='OTC_T_360_PIVOT_PARQUE';

---PARAMETROS PARA LA ENTIDAD OTC_T_360_MOVIMIENTOS_PARQUE
DELETE FROM params_des WHERE entidad='OTC_T_360_MOVIMIENTOS_PARQUE';
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_MOVIMIENTOS_PARQUE','RUTA','/home/nae108834/Movimientos','0','1');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_MOVIMIENTOS_PARQUE','SHELL','/home/nae108834/Movimientos/Bin/OTC_T_360_MOVIMIENTOS_PARQUE.sh','0','1');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_MOVIMIENTOS_PARQUE','RUTA_LOG','/home/nae108834/Movimientos/Log','0','1');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_MOVIMIENTOS_PARQUE','RUTA_PYTHON','/home/nae108834/Movimientos/Python','0','1');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_MOVIMIENTOS_PARQUE','VAL_ESQUEMA_TMP','db_desarrollo2021','0','1');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_MOVIMIENTOS_PARQUE','VAL_ESQUEMA_REP','db_desarrollo2021','0','1');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_MOVIMIENTOS_PARQUE','vTAltBajHist','db_desarrollo2021.otc_t_alta_baja_hist','0','1');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_MOVIMIENTOS_PARQUE','vTAltBI','db_cs_altas.otc_t_altas_bi','0','1');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_MOVIMIENTOS_PARQUE','vTBajBI','db_cs_altas.otc_t_bajas_bi','0','1');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_MOVIMIENTOS_PARQUE','vTTransfHist','db_desarrollo2021.otc_t_transfer_hist_2','0','1');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_MOVIMIENTOS_PARQUE','vTTrInBI','db_cs_altas.otc_t_transfer_in_bi','0','1');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_MOVIMIENTOS_PARQUE','vTTrOutBI','db_cs_altas.otc_t_transfer_out_bi','0','1');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_MOVIMIENTOS_PARQUE','vTCPHist','db_desarrollo2021.otc_t_cambio_plan_hist_2','0','1');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_MOVIMIENTOS_PARQUE','vTCPBI','db_cs_altas.otc_t_cambio_plan_bi','0','1');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_MOVIMIENTOS_PARQUE','vTNRCSA','db_cs_altas.no_reciclable','0','1');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_MOVIMIENTOS_PARQUE','vTCatPosUsr','db_cs_altas.otc_t_ctl_pos_usr_nc','0','1');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_MOVIMIENTOS_PARQUE','vTCatPreUsr','db_desarrollo2021.otc_t_ctl_pre_usr_nc','0','1');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_MOVIMIENTOS_PARQUE','vTPivotParq','db_desarrollo2021.otc_t_360_parque_1_tmp','0','1');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_MOVIMIENTOS_PARQUE','VAL_ETP01_MASTER','yarn','0','1');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_MOVIMIENTOS_PARQUE','VAL_ETP01_DRIVER_MEMORY','8G','0','1');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_MOVIMIENTOS_PARQUE','VAL_ETP01_EXECUTOR_MEMORY','16G','0','1');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_MOVIMIENTOS_PARQUE','VAL_ETP01_NUM_EXECUTORS','5','0','1');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_MOVIMIENTOS_PARQUE','VAL_ETP01_NUM_EXECUTORS_CORES','3','0','1');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_MOVIMIENTOS_PARQUE','ETAPA','1','0','1');

--PARAMETROS PARA LA ENTIDAD OTC_T_360_GENERAL
DELETE FROM params_des WHERE ENTIDAD='OTC_T_360_GENERAL';
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_GENERAL','RUTA','/home/nae108834/Movimientos','0','1');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_GENERAL','SHELL','/home/nae108834/Movimientos/Bin/OTC_T_360_GENERAL.sh','0','1');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_GENERAL','RUTA_LOG','/home/nae108834/Movimientos/Log','0','1');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_GENERAL','RUTA_PYTHON','/home/nae108834/Movimientos/Python','0','1');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_GENERAL','FILE_PYTHON','otc_t_360_general.py','0','1');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_GENERAL','PASO','1','0','1');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_GENERAL','HIVEDB','db_desarrollo2021','0','1');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_GENERAL','HIVETABLE','otc_t_360_general','0','1');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_GENERAL','ESQUEMA_TEMP','db_desarrollo2021','0','1');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_GENERAL','ESQUEMA_TEMP_READ','db_temporales','0','1');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_GENERAL','PATH_QUERY','/home/nae108834/Movimientos/Python/Querys','0','1');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_GENERAL','PATH_CONF','/home/nae108834/Movimientos/Python/Configuraciones','0','1');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_GENERAL','QUEUE','capa_semantica','0','1');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_GENERAL','PESOS_PARAMETROS',' 0.45;0.05;0.40;0.04;0.02;0.02;0.02','0','1');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_GENERAL','PESOS_NSE','0.80;0.60;0.40;0.20;0.00','0','1');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_GENERAL','TOPE_RECARGAS','45','0','1');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_GENERAL','TOPE_TARIFA_BASICA','45','0','1');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_GENERAL','ESQUEMA_TABLA','db_thebox','0','1');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_GENERAL','ESQUEMA_TABLA_1','db_files_novum','0','1');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_GENERAL','ESQUEMA_TABLA_2','db_thebox','0','1');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_GENERAL','ESQUEMA_TABLA_3','db_payment_manager','0','1');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_GENERAL','TIPO_CARGA','append','0','1');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_GENERAL','VAL_MASTER','yarn','0','1');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_GENERAL','VAL_DRIVER_MEMORY','16G','0','1');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_GENERAL','VAL_EXECUTOR_MEMORY','16G','0','1');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_GENERAL','VAL_NUM_EXECUTORS','7','0','1');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_GENERAL','VAL_NUM_EXECUTORS_CORES','4','0','1');



--PARAMETROS PARA LA ENTIDAD OTC_T_AJUSTES_MOVIMIENTOS
DELETE FROM params_des WHERE entidad='OTC_T_AJUSTES_MOVIMIENTOS';
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_AJUSTES_MOVIMIENTOS','ETAPA','1',0,1);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_AJUSTES_MOVIMIENTOS','VAL_RUTA','/home/nae108834/Movimientos',0,1);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_AJUSTES_MOVIMIENTOS','VAL_FTP_RUTA','"/MOVIMIENTOS"',0,1);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_AJUSTES_MOVIMIENTOS','VAL_FTP_PUERTO','9694',0,1);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_AJUSTES_MOVIMIENTOS','VAL_FTP_HOSTNAME','fileuio03',0,1);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_AJUSTES_MOVIMIENTOS','VAL_FTP_PASS','bii2022-NA10',0,1);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_AJUSTES_MOVIMIENTOS','VAL_FTP_NOM_ARCHIVO','AJUSTES_MOVIMIENTOS.xlsx',0,1);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_AJUSTES_MOVIMIENTOS','VAL_DIR_HDFS_CAT','db_desarrollo2021.otc_t_ajustes_movimientos',1,1);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_AJUSTES_MOVIMIENTOS','VAL_FTP_USER','NABIFI01',0,1);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_AJUSTES_MOVIMIENTOS','vMASTER','yarn','0','1');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_AJUSTES_MOVIMIENTOS','vDRIVER_MEMORY','2G','0','1');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_AJUSTES_MOVIMIENTOS','vEXECUTOR_MEMORY','2G','0','1');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_AJUSTES_MOVIMIENTOS','vNUM_EXECUTORS','2','0','1');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_AJUSTES_MOVIMIENTOS','vNUM_EXECUTORS_CORES','2','0','1');
SELECT * FROM params_des WHERE ENTIDAD='OTC_T_AJUSTES_MOVIMIENTOS';


--PARAMETROS PARA LA ENTIDAD OTC_T_COMBO_INICIAL
DELETE FROM params_des WHERE entidad='OTC_T_COMBO_INICIAL';
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_COMBO_INICIAL','ETAPA','1',0,1);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_COMBO_INICIAL','VAL_RUTA','/home/nae108834/Movimientos',0,1);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_COMBO_INICIAL','VAL_FTP_RUTA','"/MOVIMIENTOS"',0,1);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_COMBO_INICIAL','VAL_FTP_PUERTO','9694',0,1);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_COMBO_INICIAL','VAL_FTP_HOSTNAME','fileuio03',0,1);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_COMBO_INICIAL','VAL_FTP_PASS','bii2022-NA10',0,1);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_COMBO_INICIAL','VAL_FTP_NOM_ARCHIVO','COMBO_INICIAL.xlsx',0,1);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_COMBO_INICIAL','VAL_DIR_HDFS_CAT','db_desarrollo2021.otc_t_combo_inicial',1,1);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_COMBO_INICIAL','VAL_FTP_USER','NABIFI01',0,1);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_COMBO_INICIAL','vMASTER','yarn','0','1');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_COMBO_INICIAL','vDRIVER_MEMORY','2G','0','1');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_COMBO_INICIAL','vEXECUTOR_MEMORY','2G','0','1');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_COMBO_INICIAL','vNUM_EXECUTORS','2','0','1');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_COMBO_INICIAL','vNUM_EXECUTORS_CORES','2','0','1');
SELECT * FROM params_des WHERE ENTIDAD='OTC_T_COMBO_INICIAL';


-- PARAMS PARA CATALOGO_CONSOLIDADO
--CAMBIAR LA ENTIDAD
DELETE FROM params_des WHERE entidad='OTC_T_CTL_CONSOLIDADO_ID';
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_CTL_CONSOLIDADO_ID','ETAPA','1',0,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_CTL_CONSOLIDADO_ID','VAL_RUTA','/home/nae108834/Movimientos',0,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_CTL_CONSOLIDADO_ID','VAL_FTP_RUTA','/MOVIMIENTOS',0,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_CTL_CONSOLIDADO_ID','VAL_FTP_PUERTO','9694',0,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_CTL_CONSOLIDADO_ID','VAL_FTP_HOSTNAME','fileuio03',0,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_CTL_CONSOLIDADO_ID','VAL_FTP_PASS','bii2022-NA10',0,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_CTL_CONSOLIDADO_ID','VAL_FTP_NOM_ARCHIVO','ConsolidadoID.xlsx',0,0);
insert into params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_CTL_CONSOLIDADO_ID','VAL_DIR_HDFS_CAT','db_desarrollo2021.otc_t_ctl_consolidado_id',1,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_CTL_CONSOLIDADO_ID','VAL_FTP_USER','NABIFI01',0,0);
SELECT * FROM params_des WHERE ENTIDAD='OTC_T_CTL_CONSOLIDADO_ID';

--                                          PARAMS PARA CATALOGO_CONSOLIDADO
--CAMBIAR LA ENTIDAD
DELETE FROM params_des WHERE entidad='OTC_T_CTL_PRE_USR_NC';
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_CTL_PRE_USR_NC','ETAPA','1',0,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_CTL_PRE_USR_NC','VAL_RUTA','/home/nae108834/Movimientos',0,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_CTL_PRE_USR_NC','VAL_FTP_RUTA','"/Business Intelligence/3_Operaciones/Procesos/EMERGENTE/NC/PARQUE/CATALOGO"',0,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_CTL_PRE_USR_NC','VAL_FTP_PUERTO','9664',0,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_CTL_PRE_USR_NC','VAL_FTP_HOSTNAME','fileuio03',0,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_CTL_PRE_USR_NC','VAL_FTP_PASS','bii2022-NA10',0,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_CTL_PRE_USR_NC','VAL_FTP_NOM_ARCHIVO','CTL_PRE_USR_NC.xls',0,0);
insert into params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_CTL_PRE_USR_NC','VAL_DIR_HDFS_CAT','db_desarrollo2021.OTC_T_CTL_PRE_USR_NC',1,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_CTL_PRE_USR_NC','VAL_FTP_USER','NABIFI01',0,0);
SELECT * FROM params_des WHERE ENTIDAD='OTC_T_CTL_PRE_USR_NC';


--  PARAMS PARA perimetros transfer in y altas
DELETE FROM params_des WHERE entidad='OTC_T_PRMTR_ALTAS_TI';
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_PRMTR_ALTAS_TI','ETAPA','1',0,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_PRMTR_ALTAS_TI','VAL_RUTA','/home/nae108834/Movimientos',0,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_PRMTR_ALTAS_TI','VAL_FTP_RUTA','"/MOVIMIENTOS"',0,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_PRMTR_ALTAS_TI','VAL_FTP_PUERTO','9694',0,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_PRMTR_ALTAS_TI','VAL_FTP_HOSTNAME','fileuio03',0,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_PRMTR_ALTAS_TI','VAL_FTP_PASS','bii2022-NA10',0,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_PRMTR_ALTAS_TI','VAL_FTP_NOM_ARCHIVO','Perimetro Septiembre22_EN Altas.xlsx',0,0);
--INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_PRMTR_ALTAS_TI','VAL_FTP_NOM_ARCHIVO','Perimetro Octubre22_EN Altas.xlsx',0,0);
--INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_PRMTR_ALTAS_TI','VAL_FTP_NOM_ARCHIVO','Perimetro Noviembre22_EN Altas.xlsx',0,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_PRMTR_ALTAS_TI','VAL_DIR_HDFS_CAT','db_desarrollo2021.OTC_T_PRMTR_ALTAS_TI',1,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_PRMTR_ALTAS_TI','VAL_FTP_USER','NABIFI01',0,0);
SELECT * FROM params_des WHERE ENTIDAD='OTC_T_PRMTR_ALTAS_TI';

--  PARAMS PARA perimetros transfer out y bajas
DELETE FROM params_des WHERE entidad='OTC_T_PRMTR_BAJAS_TO';
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_PRMTR_BAJAS_TO','ETAPA','1',0,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_PRMTR_BAJAS_TO','VAL_RUTA','/home/nae108834/Movimientos',0,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_PRMTR_BAJAS_TO','VAL_FTP_RUTA','"/MOVIMIENTOS"',0,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_PRMTR_BAJAS_TO','VAL_FTP_PUERTO','9694',0,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_PRMTR_BAJAS_TO','VAL_FTP_HOSTNAME','fileuio03',0,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_PRMTR_BAJAS_TO','VAL_FTP_PASS','bii2022-NA10',0,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_PRMTR_BAJAS_TO','VAL_FTP_NOM_ARCHIVO','Perimetro Septiembre22_EN Bajas.xlsx',0,0);
--INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_PRMTR_BAJAS_TO','VAL_FTP_NOM_ARCHIVO','Perimetro Octubre22_EN Bajas.xlsx',0,0);
--INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_PRMTR_BAJAS_TO','VAL_FTP_NOM_ARCHIVO','Perimetro Noviembre22_EN Bajas.xlsx',0,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_PRMTR_BAJAS_TO','VAL_DIR_HDFS_CAT','db_desarrollo2021.OTC_T_PRMTR_BAJAS_TO',1,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_PRMTR_BAJAS_TO','VAL_FTP_USER','NABIFI01',0,0);
SELECT * FROM params_des WHERE ENTIDAD='OTC_T_PRMTR_BAJAS_TO';


-------PARAMS PARA EXTRACTOR DE MOVIMIENTOS
DELETE FROM params_des WHERE entidad='EXT_MVMNTS0010';
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('EXT_MVMNTS0010','ETAPA','1',0,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('EXT_MVMNTS0010','vRUTA','/home/nae108834/Movimientos',0,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('EXT_MVMNTS0010','vFTP_RUTA_OUT','/PROYECTO_COMISIONES_PRODUCCION/EXT_MOVIMIENTOS',0,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('EXT_MVMNTS0010','vFTP_USER_OUT','NABIFI01',0,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('EXT_MVMNTS0010','vFTP_PUERTO_OUT','9694',0,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('EXT_MVMNTS0010','vFTP_HOSTNAME_OUT','fileuio03',0,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('EXT_MVMNTS0010','vFTP_PASS_OUT','bii2022-NA10',0,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('EXT_MVMNTS0010','vFTP_NOM_ARCHIVO','Extractor_Movimientos_',0,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('EXT_MVMNTS0010','vRUTA_IN','/home/nae108834/Movimientos/Input',0,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('EXT_MVMNTS0010','vARCH_XTRCTR','/home/nae108834/Movimientos/Input/Extractor_Movimientos.txt',0,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('EXT_MVMNTS0010','vTMain','db_desarrollo2021.otc_t_ext_movimientos',0,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('EXT_MVMNTS0010','vTAjust','db_desarrollo2021.otc_t_ajustes_movimientos',0,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('EXT_MVMNTS0010','vNOM_PROCESO','EXTRACTOR_DE_MOVIMIENTOS',0,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('EXT_MVMNTS0010','vSFTP_PUERTO_OUT','22','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('EXT_MVMNTS0010','vSFTP_USER_OUT','telefonicaecuadorprod','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('EXT_MVMNTS0010','vSFTP_HOSTNAME_OUT','ftp.cloud.varicent.com','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('EXT_MVMNTS0010','vSFTP_PASS_OUT','RqiZ2lkJmeiQTi2hvRwd','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('EXT_MVMNTS0010','vSFTP_RUTA_OUT','/Data','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('EXT_MVMNTS0010','vNOM_ARCHIVO_OUT','Extractor_Movimientos','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('EXT_MVMNTS0010','vMASTER','yarn','0','1');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('EXT_MVMNTS0010','vDRIVER_MEMORY','16G','0','1');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('EXT_MVMNTS0010','vEXECUTOR_MEMORY','16G','0','1');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('EXT_MVMNTS0010','vNUM_EXECUTORS','4','0','1');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('EXT_MVMNTS0010','vNUM_EXECUTORS_CORES','4','0','1');
SELECT * FROM params_des WHERE ENTIDAD='EXT_MVMNTS0010';


-- PARAMS PARA BASE_CARGA_DESCUENTO_NO_PYMES
DELETE FROM params_des WHERE entidad='OTC_T_DESC_NO_PYMES';
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_DESC_NO_PYMES','ETAPA','1',0,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_DESC_NO_PYMES','VAL_RUTA','/home/nae108834/Movimientos',0,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_DESC_NO_PYMES','VAL_FTP_RUTA','"/MOVIMIENTOS"',0,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_DESC_NO_PYMES','VAL_FTP_PUERTO','9694',0,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_DESC_NO_PYMES','VAL_FTP_HOSTNAME','fileuio03',0,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_DESC_NO_PYMES','VAL_FTP_PASS','bii2022-NA10',0,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_DESC_NO_PYMES','VAL_FTP_NOM_ARCHIVO','BASE_CARGA_DESCUENTO_NO_PYMES.xlsx',0,0);
insert into params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_DESC_NO_PYMES','VAL_DIR_HDFS_CAT','db_desarrollo2021.otc_t_desc_no_pymes',1,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_DESC_NO_PYMES','VAL_FTP_USER','NABIFI01',0,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_DESC_NO_PYMES','vMASTER','yarn','0','1');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_DESC_NO_PYMES','vDRIVER_MEMORY','16G','0','1');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_DESC_NO_PYMES','vEXECUTOR_MEMORY','16G','0','1');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_DESC_NO_PYMES','vNUM_EXECUTORS','4','0','1');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_DESC_NO_PYMES','vNUM_EXECUTORS_CORES','4','0','1');
SELECT * FROM params_des WHERE ENTIDAD='OTC_T_DESC_NO_PYMES';






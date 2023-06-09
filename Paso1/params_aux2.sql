
---PARAMETROS PARA LA ENTIDAD OTC_T_360_CIERRE
DELETE FROM params WHERE entidad='OTC_T_360_CIERRE';
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_CIERRE','RUTA','/RGenerator/reportes/Cliente360','0','1');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_CIERRE','SHELL','/RGenerator/reportes/Cliente360/Bin/OTC_T_360_CIERRE.sh','0','1');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_CIERRE','RUTA_LOG','/RGenerator/reportes/Cliente360/Log','0','1');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_CIERRE','RUTA_PYTHON','/RGenerator/reportes/Cliente360/Python','0','1');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_CIERRE','VAL_ESQUEMA_TMP','db_temporales','0','1');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_CIERRE','VAL_ESQUEMA_REP','db_reportes','0','1');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_CIERRE','vTDetRec','db_cs_recargas.otc_t_cs_detalle_recargas','0','1');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_CIERRE','vTParOriRec','db_altamira.par_origen_recarga','0','1');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_CIERRE','vTCBPDV','db_reportes.cat_bonos_pdv','0','1');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_CIERRE','vTABI','db_cs_altas.otc_t_altas_bi','0','1');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_CIERRE','vTTOBI','db_cs_altas.otc_t_transfer_out_bi','0','1');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_CIERRE','vTTIBI','db_cs_altas.otc_t_transfer_in_bi','0','1');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_CIERRE','vTCPBI','db_cs_altas.otc_t_cambio_plan_bi','0','1');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_CIERRE','vTBInv','db_cs_altas.otc_t_bajas_involuntarias','0','1');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_CIERRE','vTChurnSP2','db_cs_altas.otc_t_churn_sp2','0','1');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_CIERRE','vTVWCFac','db_rbm.otc_t_vw_cta_facturacion','0','1');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_CIERRE','vTPrmDate','db_rbm.otc_t_prmandate','0','1');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_CIERRE','vTNCMovParV1','db_cs_altas.otc_t_nc_movi_parque_v1','0','1');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_CIERRE','vTPlCatT','db_cs_altas.otc_t_ctl_planes_categoria_tarifa','0','1');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_CIERRE','vTBBI','db_cs_altas.otc_t_bajas_bi','0','1');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_CIERRE','vTRIMobPN','db_rdb.otc_t_r_ri_mobile_phone_number','0','1');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_CIERRE','vTTmp360UR','db_temporales.tmp_360_ultima_renovacion_end','0','1');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_CIERRE','vTTmp360AA','db_temporales.tmp_360_account_address','0','1');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_CIERRE','vTTmp360VA','db_temporales.tmp_360_vigencia_abonado_plan_def','0','1');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_CIERRE','vTRABH','db_reportes.otc_t_alta_baja_hist','0','1');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_CIERRE','vTTrH','db_reportes.otc_t_transfer_hist','0','1');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_CIERRE','vTCPH','db_reportes.otc_t_cambio_plan_hist','0','1');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_CIERRE','vTPRQGLBBI','db_cs_altas.otc_t_prq_glb_bi','0','1');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_CIERRE','vTBOEBSNS','db_rdb.otc_t_r_boe_bsns_prod_inst','0','1');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_CIERRE','vTBOESUSPRSN','db_rdb.otc_t_r_boe_bsns_prod_inst_susp_rsn','0','1');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_CIERRE','vTPIMStCh','db_rdb.otc_t_r_pim_status_change','0','1');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_CIERRE','vTTerSC','db_cs_terminales.otc_t_terminales_simcards','0','1');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_CIERRE','vTFacTeSCL','db_cs_terminales.otc_t_facturacion_terminales_scl','0','1');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_CIERRE','vTAddress','db_rbm.otc_t_address','0','1');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_CIERRE','vTAccount','db_rbm.otc_t_account','0','1');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_CIERRE','vTCNTMConIt','db_rdb.otc_t_r_cntm_contract_item','0','1');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_CIERRE','vTCNTMCA','db_rdb.otc_t_r_cntm_com_agrm','0','1');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_CIERRE','vTAmCPE','db_rdb.otc_t_r_am_cpe','0','1');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_CIERRE','vTPimPRDOff','db_rdb.otc_t_r_pim_prd_off','0','1');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_CIERRE','vTRepCart','db_rbm.reporte_cartera','0','1');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_CIERRE','vTAltPPCSLl','db_altamira.otc_t_ppcs_llamadas','0','1');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_CIERRE','vTDevCatP','db_reportes.otc_t_dev_cat_plan','0','1');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_CIERRE','vTPPCSDi','db_altamira.otc_t_ppcs_diameter','0','1');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_CIERRE','vTPPCSMe','db_altamira.otc_t_ppcs_mecoorig','0','1');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_CIERRE','vTPPCSCon','db_altamira.otc_t_ppcs_content','0','1');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_CIERRE','vTAccDet','db_rbm.otc_t_accountdetails','0','1');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_CIERRE','vTPaymMeth','db_rbm.otc_t_paymentmethod','0','1');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_CIERRE','vTHomSeg','db_cs_altas.otc_t_homologacion_segmentos','0','1');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_CIERRE','vTBoxPE20','db_thebox.otc_t_parque_edad20','0','1');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_CIERRE','vT360Mod','db_reportes.otc_t_360_modelo','0','1');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_CIERRE','vTUsuAct','db_files_novum.otc_t_usuariosactivos','0','1');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_CIERRE','vTUsuReg','db_files_novum.otc_t_rep_usuarios_registrados','0','1');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_CIERRE','vTUseSem','db_mplay.otc_t_users_semanal','0','1');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_CIERRE','vTMPUsers','db_mplay.otc_t_users','0','1');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_CIERRE','vTPPGAAd','db_rdb.otc_t_ppga_adquisiciones','0','1');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_CIERRE','vTABoPre','db_rdb.otc_t_ppga_actabopre','0','1');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_CIERRE','vTOfComComb','db_rdb.otc_t_oferta_comercial_comberos','0','1');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_CIERRE','vTBonCom','db_payment_manager.otc_t_pmg_bonos_combos','0','1');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_CIERRE','vTCTLBon','db_dwec.otc_t_ctl_bonos','0','1');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_CIERRE','vTChuPre','db_rdb.otc_t_churn_prepago','0','1');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_CIERRE','vTTmp360Parq1','db_temporales.otc_t_360_parque_1_tmp','0','1');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_CIERRE','vTPredPort22','db_thebox.pred_portabilidad2022','0','1');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_CIERRE','vTCatCelDPA','db_ipaccess.catalogo_celdas_dpa','0','1');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_CIERRE','vT360Ing','db_reportes.otc_t_360_ingresos','0','1');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_CIERRE','vTScTX','db_reportes.otc_t_scoring_tiaxa','0','1');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_CIERRE','vTXDRSMS','default.otc_t_xdrcursado_sms','0','1');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_CIERRE','vTNumBSMS','db_rdb.otc_t_numeros_bancos_sms','0','1');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_CIERRE','vTBonFid','db_rdb.otc_t_bonos_fidelizacion','0','1');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_CIERRE','vT360Gen','db_reportes.otc_t_360_general','0','1');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_CIERRE','vT360Traf','db_reportes.otc_t_360_trafico','0','1');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_CIERRE','vTCBMBiAc','db_rdb.otc_t_r_cbm_billing_acct','0','1');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_CIERRE','vTPortUs','db_lportal.otc_t_user','0','1');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_CIERRE','vTResCusAcc','db_rdb.otc_t_r_cim_res_cust_acct','0','1');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_CIERRE','vTRegUs','db_trxdb.otc_t_registro_usuario','0','1');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_CIERRE','vTMinWV','db_trxdb.otc_t_mines_wv','0','1');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_CIERRE','vTCimCont','db_rdb.otc_t_r_cim_cont','0','1');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_CIERRE','vTBCenso','db_thebox.base_censo','0','1');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_CIERRE','VAL_ETP01_MASTER','yarn','0','1');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_CIERRE','VAL_ETP01_DRIVER_MEMORY','16G','0','1');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_CIERRE','VAL_ETP01_EXECUTOR_MEMORY','16G','0','1');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_CIERRE','VAL_ETP01_NUM_EXECUTORS','8','0','1');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_CIERRE','VAL_ETP01_NUM_EXECUTORS_CORES','8','0','1');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_CIERRE','QUEUE','capa_semantica','0','1');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_CIERRE','ETAPA','1','0','1');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_CIERRE','TOPE_RECARGAS','45','0','1');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_CIERRE','TOPE_TARIFA_BASICA','45','0','1');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_CIERRE','PESOS_PARAMETROS','0.45;0.05;0.40;0.04;0.02;0.02;0.02','0','1');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_CIERRE','PESOS_NSE','0.80;0.60;0.40;0.20;0.00','0','1');

----------------------------------
select * from params where  ENTIDAD = 'OTC_T_360_CIERRE';
+------------------+--------------------+---------------------------------------------------------+-------+----------+
| ENTIDAD          | PARAMETRO          | VALOR                                                   | ORDEN | AMBIENTE |
+------------------+--------------------+---------------------------------------------------------+-------+----------+
| OTC_T_360_CIERRE | RUTA               | /RGenerator/reportes/Cliente360                         |     0 |        1 |
| OTC_T_360_CIERRE | RUTA_LOG           | /RGenerator/reportes/Cliente360                         |     0 |        1 |
| OTC_T_360_CIERRE | SHELL              | /RGenerator/reportes/Cliente360/Bin/OTC_T_360_CIERRE.sh |     0 |        1 |
| OTC_T_360_CIERRE | LIMPIAR            | 1                                                       |     0 |        1 |
| OTC_T_360_CIERRE | ESQUEMA_TABLA      | db_reportes                                             |     0 |        1 |
| OTC_T_360_CIERRE | TOPE_RECARGAS      | 45                                                      |     0 |        1 |
| OTC_T_360_CIERRE | TOPE_TARIFA_BASICA | 45                                                      |     0 |        1 |
| OTC_T_360_CIERRE | PESOS_PARAMETROS   | 0.45;0.05;0.40;0.04;0.02;0.02;0.02                      |     0 |        1 |
| OTC_T_360_CIERRE | PESOS_NSE          | 0.80;0.60;0.40;0.20;0.00                                |     0 |        1 |
+------------------+--------------------+---------------------------------------------------------+-------+----------+



print(lne_dvs())
vStp='[ETAPA {} / Paso 3.24]: tabla temporal para obtener el ultimo descuento por min '
try:
    ts_step = datetime.now()
    print(etq_info(vStp))
    print(lne_dvs())
    VSQL=qry_tmp_otc_t_desc_planes(fechamas1,fecha_inico_mes_1_1,fecha_eje1)
    print(etq_sql(VSQL))
    df0 = spark.sql(VSQL)
    ts_step_count = datetime.now()
    vTotDf=df0.count()
    te_step_count = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion('df0',vle_duracion(ts_step_count,te_step_count))))
    if df0.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata('df0')))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(vTblInt24)))
            df0.write.mode('overwrite').saveAsTable(vTblInt24)
            df0.printSchema()
            print(etq_info(msg_t_total_registros_hive(vTblInt24,str(vTotDf))))
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(vTblInt24,vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive(vTblInt24,str(e))))
    del df0
    print(etq_info("Eliminar dataframe [{}]".format('df0')))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vStp,str(e))))

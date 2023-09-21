# T: Tabla
# D: Date
# I: Integer
# S: String


# N08
def qry_xtrct_movs(vPart,vTAjust):
    qry='''
SELECT	DISTINCT
	nvl(ajst.id_producto,a.id_producto) as id_producto
	, nvl(ajst.linea_negocio,a.linea_negocio_homologado) as linea_negocio
	, nvl(ajst.telefono,a.num_telefonico) as telefono
	, numero_abonado
	, nvl(ajst.account_num,a.account_num) as account_num
	, date_format(nvl(ajst.fecha_movimiento,a.fecha_movimiento_mes_cmsns),'dd/MM/yyyy') AS fecha_movimiento
	, estado_abonado
	, nombre_cliente AS cliente
	, identificacion_cliente AS documento_cliente
	,(CASE
		WHEN upper(tipo_doc_cliente) LIKE '%C%DULA%' THEN 'CEDULA'
		WHEN tipo_doc_cliente IS NULL THEN 'N/A'
		ELSE upper(tipo_doc_cliente)
	END) AS tipo_doc_cliente
	, codigo_plan AS plan_codigo
	, nombre_plan
	, ciudad
	, provincia_activacion AS provincia
	, imei
	, equipo
	, icc
	, sub_segmento
	, segmento
	, segmento_parque AS segmento_fin
	, DATE_FORMAT(CAST( CONCAT(SUBSTR((CAST(a.fecha_proceso AS STRING)), 1, 4), '-'
	, SUBSTR((CAST(a.fecha_proceso AS STRING)), 5, 2), '-'
	, SUBSTR((CAST(a.fecha_proceso AS STRING)), 7, 2))AS date), 'dd/MM/yyyy') AS fecha_proceso
	, round(TARIFA, 2) AS tarifa_basica_actual
	, categoria_plan
	, cod_categoria
	, nvl(nvl(ajst.domain_login_ow,a.domain_login_ow),'N/A') AS domain_login_ow
	, nvl(ajst.nombre_usuario_ow,a.nombre_usuario_ow) as nombre_usuario_ow
	, domain_login_sub
	, nombre_usuario_sub
	, canal_transacc AS canal
	,(CASE
		WHEN distribuidor_crm LIKE '%COMERCIAL MA.VER%NICA%' THEN 'COMERCIAL MA.VERONICA'
		WHEN distribuidor_crm LIKE 'YOUPHONE C%A LTDA.' THEN 'YOUPHONE CIA LTDA.'
		ELSE distribuidor_crm
	END) AS distribuidor
	, nvl(a.oficina_movimiento_mes, a.oficina_movimiento_mes_cmsns) AS oficina
	, nvl(ajst.portabilidad_alta,a.portabilidad_cmsns) as portabilidad_alta
	,(CASE
		WHEN upper(forma_pago) LIKE 'D%BITO%DIRECTO%BANCO%' THEN 'DEBITO DIRECTO BANCO'
		WHEN upper(forma_pago) LIKE 'D%BITO%DIRECTO%TARJETA%' THEN 'DEBITO DIRECTO TARJETA'
		ELSE upper(forma_pago)
	END) AS forma_pago
	, cod_da
	, nom_usuario
	, nvl(nvl(ajst.id_canal,a.id_canal), -1) as id_canal
	, nvl(ajst.canal_comercial,a.canal_movimiento_mes_cmsns) as canal_comercial
	, nvl(ajst.campania,a.campania) as campania
	, nvl(ajst.codigo_distribuidor,a.codigo_distribuidor_movimiento_mes) as codigo_distribuidor
	, nvl(ajst.nom_distribuidor,a.distribuidor_movimiento_mes_cmsns) as nom_distribuidor
	, nvl(ajst.codigo_plaza,a.codigo_plaza) as codigo_plaza
	, nvl(ajst.nom_plaza,a.nom_plaza_movimiento_mes) as nom_plaza
	, nvl(ajst.region,a.region) as region
	, nvl(ajst.ruc_distribuidor,a.ruc_distribuidor) as ruc_distribuidor
	, provincia_ivr
	, nvl(ajst.operadora_origen,a.operadora_origen) as operadora_origen
	, ejecutivo_asignado_ptr
	, area_ptr
	, codigo_vendedor_da_ptr
	, jefatura_ptr
	, a.email AS nom_email
	, provincia_ms
	, nvl(nvl(ajst.id_sub_canal,a.id_subcanal), -1) as id_sub_canal
	, nvl(ajst.sub_canal,a.sub_canal_movimiento_mes_cmsns) as sub_canal
	, round(tarifa_plan_actual_ov, 2) AS overwrite
	, round(descuento_tarifa_plan_act, 2) AS descuento
	, codigo_usuario
	, marca
	, tecnologia AS tecno_dispositivo
	, descripcion_desp
	, calf_riesgo
	, cap_endeu
	, valor_cred
	, date_format(fecha_alta_pospago_historica
	, 'dd/MM/yyyy') AS fecha_alta_pospago_historica
	, motivo_cmsns AS movimiento_baja
	,(CASE
		WHEN dias_transcurridos_baja < 0 THEN ''
		ELSE dias_transcurridos_baja
	END) AS dias_transcurridos_baja
	, date_format(fecha_cambio_plan, 'dd/MM/yyyy') AS fecha_cambio
	, upper(linea_de_negocio_anterior) AS linea_de_negocio_anterior
	, dias_en_parque
	, nvl(nvl(ajst.id_tipo_movimiento,a.id_tipo_movimiento), -1) as id_tipo_movimiento
	,(CASE
		WHEN upper(nvl(ajst.tipo_movimiento,a.tipo_movimiento)) LIKE 'PRE_POS' THEN 'TRANSFER_IN'
		WHEN upper(nvl(ajst.tipo_movimiento,a.tipo_movimiento)) LIKE 'POS_PRE' THEN 'TRANSFER_OUT'
		WHEN upper(nvl(ajst.tipo_movimiento,a.tipo_movimiento)) LIKE 'UPSELL' THEN 'CAMBIO_PLAN'
		WHEN upper(nvl(ajst.tipo_movimiento,a.tipo_movimiento)) LIKE 'DOWNSELL' THEN 'CAMBIO_PLAN'
		WHEN upper(nvl(ajst.tipo_movimiento,a.tipo_movimiento)) LIKE 'MISMA_TARIFA' THEN 'CAMBIO_PLAN'
		ELSE upper(nvl(ajst.tipo_movimiento,a.tipo_movimiento))
	END) AS tipo_movimiento
	, cod_plan_anterior_cmsns AS codigo_plan_anterior
	, cliente_anterior
	, des_plan_anterior_cmsns AS nombre_plan_anterior
	, round(tarifa_basica_anterior, 2) AS tarifa_basica_anterior
	, date_format(fecha_inicio_plan_anterior, 'dd/MM/yyyy') AS fecha_inicio_plan_anterior
	, round(delta, 2) AS delta_tarifa
	, dias_reciclaje
	, tipo_descuento
	, tipo_descuento_conadis
	, dias_en_parque_prepago
	, vol_invol
	, round(tarifa_basica_baja, 2) AS tarifa_basica_baja
	, portabilidad_movimiento_mes_cmsns AS portabilidad_baja
	, operadora_destino_movimiento_mes AS operadora_destino
	, account_num_anterior
	, ciudad_usuario
	, provincia_usuario
	, mismo_cliente
	, date_format(fecha_alta_prepago, 'dd/MM/yyyy') AS fecha_alta_prepago
	, round(tarifa_plan_actual_ov, 2) AS tarifa_plan_actual_ov
	, date_format(fecha_baja_reciclada, 'dd/MM/yyyy') AS fecha_baja_reciclada
	, round(tarifa_final_plan_act, 2) AS tarifa_final_plan_act
	, round(tarifa_final_plan_ant, 2) AS tarifa_final_plan_ant
	,(tarifa_final_plan_act - tarifa_final_plan_ant) AS delta_tarifa_final
	, upper(tipo_de_cuenta_en_operador_donante) AS tipo_de_cuenta_en_operador_donante
	, CAST(NULL AS STRING) AS id_hash
	, ajst.observacion as observacion
	, ci.combo_inicial_sin_iva AS combo_inicial_sin_iva
	, ci.combo_inicial_con_iva AS combo_inicial_con_iva
	, (CASE
		WHEN a.tipo_movimiento = 'PRE_POS'
		AND linea_negocio_homologado = 'PREPAGO' THEN 'borrar'
		WHEN a.tipo_movimiento = 'POS_PRE'
		AND linea_negocio_homologado = 'POSPAGO' THEN 'borrar'
		ELSE ''
	END) AS t_filter
	, CAST(SUBSTRING(a.fecha_proceso,1,6) AS bigint) AS pt_fecha
	,(ROW_NUMBER() OVER 
	(PARTITION BY a.num_telefonico
	, a.account_num
	, nombre_cliente
	, fecha_movimiento_mes
ORDER BY
	fecha_movimiento_mes DESC)) AS orden
FROM
	db_desarrollo2021.otc_t_360_general a
LEFT JOIN {vTAjust} ajst
ON 
	(ajst.telefono=a.num_telefonico)
AND
	(ajst.account_num=a.account_num)
AND 
	(cast (ajst.fecha_proceso as bigint)=a.fecha_proceso)
LEFT JOIN db_desarrollo2021.otc_t_combo_inicial ci
ON 
	(ci.telefono=a.num_telefonico)
AND 
	(cast (ci.fecha_proceso as bigint)=a.fecha_proceso)
WHERE
	a.fecha_proceso = {vPart}
	AND a.tipo_movimiento IS NOT NULL
	AND num_telefonico IS NOT NULL
	AND UPPER(estado_abonado) <> 'PREACTIVO'
	AND a.account_num IS NOT NULL
	'''.format(vPart=vPart,vTAjust=vTAjust)
    return qry

# N02
def qry_xtrct_csv(vTMain,vFechExt):
    qry='''
SELECT
	id_producto
	, linea_negocio
	, telefono
	, numero_abonado
	, account_num
	, fecha_movimiento
	, estado_abonado
	, cliente
	, documento_cliente
	, tipo_doc_cliente
	, plan_codigo
	, nombre_plan
	, ciudad
	, provincia
	, imei
	, equipo
	, icc
	, sub_segmento
	, segmento
	, segmento_fin
	, fecha_proceso
	, tarifa_basica_actual
	, categoria_plan
	, cod_categoria
	, domain_login_ow
	, nombre_usuario_ow
	, domain_login_sub
	, nombre_usuario_sub
	, canal
	, distribuidor
	, oficina
	, portabilidad_alta
	, forma_pago
	, cod_da
	, nom_usuario
	, id_canal
	, canal_comercial
	, campania
	, codigo_distribuidor
	, nom_distribuidor
	, codigo_plaza
	, nom_plaza
	, region
	, ruc_distribuidor
	, provincia_ivr
	, operadora_origen
	, ejecutivo_asignado_ptr
	, area_ptr
	, codigo_vendedor_da_ptr
	, jefatura_ptr
	, nom_email
	, provincia_ms
	, id_sub_canal
	, sub_canal
	, overwrite
	, descuento
	, codigo_usuario
	, marca
	, tecno_dispositivo
	, descripcion_desp
	, calf_riesgo
	, cap_endeu
	, valor_cred
	, fecha_alta_pospago_historica
	, movimiento_baja
	, dias_transcurridos_baja
	, fecha_cambio
	, linea_de_negocio_anterior
	, dias_en_parque
	, id_tipo_movimiento
	, tipo_movimiento
	, codigo_plan_anterior
	, cliente_anterior
	, nombre_plan_anterior
	, tarifa_basica_anterior
	, fecha_inicio_plan_anterior
	, delta_tarifa
	, dias_reciclaje
	, tipo_descuento
	, tipo_descuento_conadis
	, dias_en_parque_prepago
	, vol_invol
	, tarifa_basica_baja
	, portabilidad_baja
	, operadora_destino
	, account_num_anterior
	, ciudad_usuario
	, provincia_usuario
	, mismo_cliente
	, fecha_alta_prepago
	, tarifa_plan_actual_ov
	, fecha_baja_reciclada
	, tarifa_final_plan_act
	, tarifa_final_plan_ant
	, delta_tarifa_final
	, tipo_de_cuenta_en_operador_donante
	, id_hash
	, observacion
	, combo_inicial_sin_iva
	, combo_inicial_con_iva
	, pt_fecha
FROM {vTMain}
WHERE pt_fecha={vFechExt}
	'''.format(vTMain=vTMain,vFechExt=vFechExt)
    return qry


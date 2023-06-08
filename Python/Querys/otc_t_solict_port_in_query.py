# T: Tabla
# D: Date
# I: Integer
# S: String


# N08
def qry_solicitudes_port_in(vFIni,vFFin):
    qry='''
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
    where a.created_when >= to_date('{vFIni}','ddmmyy') and  a.created_when < to_date('{vFFin}','ddmmyy') )
	'''.format(vFIni=vFIni,vFFin=vFFin)
    return qry





drop  TABLE db_desarrollo2021.otc_t_360_general_V_20230905;

CREATE TABLE db_desarrollo2021.otc_t_360_general_V_20230905 
STORED AS PARQUET
TBLPROPERTIES ('external.table.purge'='true')
AS
(
SELECT 
	CASE WHEN	p.num_telefonico	= d.num_telefonico	then 'IGUAL' else 'DIF' END AS 	num_telefonico
,	CASE WHEN	p.account_num	= d.account_num	then 'IGUAL' else 'DIF' END AS 	account_num
FROM db_reportes.otc_t_360_general AS p
LEFT JOIN db_desarrollo2021.otc_t_360_general as d 
on (p.num_telefonico=d.num_telefonico)
and (p.account_num=d.account_num)
where p.fecha_proceso=20230904
and d.fecha_proceso=20230904
);

select count(1) from db_desarrollo2021.otc_t_360_general_V_20230905;


show create table db_temporales.otc_t_scoring_tiaxa_tmp;

select score_1_tiaxa,score_2_tiaxa
--from db_reportes.otc_t_360_general
from db_desarrollo2021.otc_t_360_general
where fecha_proceso=20230904;













































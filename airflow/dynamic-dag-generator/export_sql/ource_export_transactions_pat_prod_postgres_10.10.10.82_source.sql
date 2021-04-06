-- sample export SQL
select
	tbl1.id,
	tbl1.transid,
	tbl1.databaseid,
	tbl1.cluster,
	tbl1.time_modified,
	case
		when tbl1.jcol::json ->>'renewal' = '' then 0
		else (tbl1.jcopl::json ->>'renewal')::int
	end renewal,
	tbl1.update_timestamp
from
	transactions.pat tbl1
inner join transactions.job tbl2 on
	tbl1.id = tbl2.params_id
where
	tbl1.update_timestamp >= '{}' and tbl1.update_timestamp <='{}';

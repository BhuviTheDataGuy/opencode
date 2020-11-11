"""
Author: Bhuvanesh
Version: 1.0
"""
from airflow import DAG
import json
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.hooks.bigquery import  BigQueryHook
from airflow.models import Variable


# Add args and Dag
default_args = {
	'owner': 'DBteam',
	'depends_on_past': False,
	'start_date': days_ago(1)
	}
dag = DAG(
	'greenplum_migration',
	default_args=default_args,
	description='create the table structure along with partition',
	schedule_interval=None,
	catchup=False,
	max_active_runs=1
)

# If you want to use variables then uncomment the below lines
# table_name = Variable.get("table_name")
# schema_name = Variable.get("schema_name")

table_name='partition_test6'
schema_name='public'

# Extract the parition info:
def func_partition_info(schema,table,**kwargs):
	postgres_hook = PostgresHook(postgres_conn_id='my_greenplum_airflow_connection_id')
	ispartition=postgres_hook.get_records(sql="""
		WITH cte AS (
	    SELECT
	        schemaname, tablename
	    FROM
	        pg_catalog.pg_partitions
	    WHERE
	        tablename = '{}'
	        AND schemaname = '{}'
	        AND partitiontype = 'range'
	)
	SELECT
	    CASE
	        WHEN count(*)<> 0 THEN 'partition'
	        ELSE 'nopartition'
	    END AS part
	FROM
	    cte
		""".format(table,schema))
	
	print(ispartition)
	
	if ispartition[0][0] == 'partition':
		
		partition_colname=postgres_hook.get_records(sql="""
			SELECT
			    columnname
			FROM
			    pg_catalog.pg_partition_columns
			WHERE
			    tablename = '{}'
			    AND schemaname = '{}'
			""".format(table,schema))
		print(partition_colname)

		bqpartition_type=postgres_hook.get_records(sql="""
			SELECT
			    CASE
			        WHEN (
			            data_type LIKE 'timestamp%'
			            OR data_type = 'date'
			            OR data_type LIKE 'time%'
			        ) THEN 'timeunit'
			        WHEN data_type = 'integer' THEN 'integer'
			        ELSE 'nosupport'
			    END AS bqtype
			FROM
			    information_schema.columns
			WHERE
			    column_name = '{}'
			    AND table_name = '{}'
			    AND table_schema = '{}'
			""".format(partition_colname[0][0],table,schema))
		print(bqpartition_type)

		if bqpartition_type[0][0] == 'timeunit':
			gppartitiontype = 'timePartitioning'
			bqpartition=postgres_hook.get_records(sql="""
			SELECT
			    CASE
			        WHEN partitioneveryclause LIKE '%day%' THEN 'DAY'
			        WHEN partitioneveryclause LIKE '%mon%' THEN 'MONTH'
			        WHEN partitioneveryclause LIKE '%year%' THEN 'YEAR'
			        ELSE 'HOUR'
			    END AS partitionby
			FROM
			    pg_catalog.pg_partitions
			WHERE
			    tablename = '{}'
			    AND schemaname = '{}'
			    AND partitioneveryclause != ''
			GROUP BY
			    partitionby	
			""".format(table,schema))
		elif bqpartition_type[0][0] == 'integer':
			gppartitiontype = 'rangePartitioning'
			bqpartition=postgres_hook.get_records(sql="""
			WITH cte AS (
			    SELECT
			        *
			    FROM
			        pg_catalog.pg_partitions
			    WHERE
			        partitionlevel = 0
			        AND tablename = '{}'
			        AND schemaname = '{}'
			)
			SELECT
			    'start=' || min(partitionrangestart)|| ', end=' || max(partitionrangeend)|| ', interval=' || partitioneveryclause AS partitionrange
			FROM
			    cte
			WHERE
			    partitionrangestart != ''
			    AND partitiontype = 'range'
			GROUP BY
			    tablename,
			    partitioneveryclause
			""".format(table,schema))
		else:
			print('Not table calculate the partition')
		return (gppartitiontype,partition_colname[0][0],bqpartition[0][0])
	else:
		print('This table is not partitioned')
		return (['nopartition'])

# Extract the table DDL
def func_get_pgschema(schema,table,**kwargs):
	sql="""
	WITH cte AS (
	    SELECT
	        '{{"mode": "' ||
	        CASE
	            WHEN is_nullable = 'YES' THEN 'NULLABLE'
	            ELSE 'REQUIRED'
	        END || '", "name": "' || column_name || '", "type": "' ||
	        CASE
	            WHEN data_type = 'bigint' THEN 'INT64'
	            WHEN data_type = 'bigserial' THEN 'INT64'
	            WHEN data_type = 'bit' THEN 'INT64'
	            WHEN data_type = 'bit varying' THEN 'INT64'
	            WHEN data_type = 'boolean' THEN 'BOOL'
	            WHEN data_type = 'bytea' THEN 'BYTEA'
	            WHEN data_type = 'character' THEN 'STRING'
	            WHEN data_type = 'character varying' THEN 'STRING'
	            WHEN data_type = 'date' THEN 'DATE'
	            WHEN data_type = 'decimal' THEN 'FLOAT64'
	            WHEN data_type = 'double precision' THEN 'FLOAT64'
	            WHEN data_type = 'integer' THEN 'INT64'
	            WHEN data_type = 'money' THEN 'FLOAT64'
	            WHEN data_type = 'real' THEN 'FLOAT64'
	            WHEN data_type = 'serial' THEN 'INT64'
	            WHEN data_type = 'smallint' THEN 'INT64'
	            WHEN data_type = 'text' THEN 'STRING'
	            WHEN data_type = 'time without time zone' THEN 'TIME'
	            WHEN data_type = 'time with time zone' THEN 'STRING'
	            WHEN data_type = 'timestamp without time zone' THEN 'DATETIME'
	            WHEN data_type = 'timestamp withtime zone' THEN 'TIMESTAMP'
	            WHEN data_type = 'uuid' THEN 'STRING'
	            WHEN data_type = 'numeric' THEN 'FLOAT64'
	            ELSE 'STRING'
	        END || '"}}' AS TYPE
	    FROM
	        information_schema.columns
	    WHERE
	        table_schema = '{}'
	        AND table_name = '{}'
	    ORDER BY
	        ordinal_position ASC
	)
	SELECT
	    '[' || string_agg(
	        TYPE, ', '
	    )|| ']' AS SCHEMA
	FROM
	    cte;	
	""".format(schema,table)
	postgres_hook = PostgresHook(postgres_conn_id='my_greenplum_airflow_connection_id')
	records = postgres_hook.get_records(sql=sql)
	return records[0][0]	

# Convert the extracted info as JSON
def func_bqparition_json(**kwargs):
	ti = kwargs['ti']
	partition = ti.xcom_pull(task_ids='get_gppartition_info')
	print(partition)
	if partition[0] == 'nopartition':
		partition_qry = None	
	elif partition[0] == 'timePartitioning':
		partition_qry = """ 
			"timePartitioning": {{
			"type": "{}",
			"field": "{}"
	 	}}""".format(partition[2],partition[1])
	elif partition[0] == 'rangePartitioning':
		start = partition[2].split(',')[0].split('=')[1]
		end = partition[2].split(',')[1].split('=')[1]
		interval = partition[2].split(',')[2].split('=')[1]
		partition_qry = """ 
		"rangePartitioning": {{
		"field": "{}",
  			"range": {{
			"start": {},
			"end": {},
			"interval": {} }}
		}}""".format(partition[1],start,end,interval)
	return partition_qry

# Merge parition JSON with Schema JSON for table resource:
def func_generate_ts(**kwargs):
	ti = kwargs['ti']
	get_partition = ti.xcom_pull(task_ids='gen_bqparition')
	get_schema = ti.xcom_pull(task_ids='get_pgschema')
	
	if get_partition is None:
		ts = """{{
		"schema": {{
			"fields": [{}]
		}}
		}}""".format(get_schema)
		
	else:
		ts = """{{
		{},
		"schema": {{
			"fields": [{}]
		}}
	}}""".format(get_partition,get_schema)
	return ts

# Create the table:
def func_appy_bq(**kwargs):
	ti = kwargs['ti']
	ts = ti.xcom_pull(task_ids='gen_table_resource')
	BigQueryHook().create_empty_table(
	project_id="mygcp-project",
	dataset_id='test',
	table_id="partition_demo",
	table_resource =json.loads(ts)
	)

# Tasks  
gpparition_info = PythonOperator(
			task_id="get_gppartition_info",
			python_callable=func_partition_info,
			op_kwargs={'schema':schema_name, 'table':table_name},
			provide_context=True,
			dag=dag
		)
get_pgschema = PythonOperator(
			task_id="get_pgschema",
			python_callable=func_get_pgschema,
			op_kwargs={'schema':schema_name, 'table':table_name},
			provide_context=True,
			dag=dag
		)
gen_bqpatition = PythonOperator(
			task_id="gen_bqparition",
			python_callable=func_bqparition_json,
			provide_context=True,
			dag=dag
		)
gen_tableresource = PythonOperator(
			task_id="gen_table_resource",
			python_callable=func_generate_ts,
			provide_context=True,
			dag=dag
		)
bq_create = PythonOperator(
			task_id="apply_to_bq",
			python_callable=func_appy_bq,
			provide_context=True,
			dag=dag
		)

gpparition_info >> get_pgschema >> gen_bqpatition >> gen_tableresource >> bq_create

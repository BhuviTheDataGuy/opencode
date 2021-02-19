from airflow import DAG
import json
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.hooks.bigquery  import BigQueryHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator

'''
>> Make sure BQ and PostgreSQL connections are created on airflow <<

CHANGE THE FOLLOWING VARIABLES:
pg_conn - Postgres connection name in airflow
bq_conn - BigQuery connection name in airflow
list    - comma seperated table names (format: schema_name.table_name)
target_dataset - BQ dataset name
gcp_project - GCP Project
'''

pg_conn = 'mig_ddl_pgconn'
bq_conn = 'mig_ddl_bqconn'
list = 'public.tbl1,public.tbl2'
target_dataset = 'bq_dataset'
gcp_project = 'myproject'
table_list = list.split(',')

#DAG details
docs = """
**Purpose**

This DAG will migrate any Postgres/GreenPlum table structure to BQ.
"""
# Add args and Dag
default_args = {
    'owner': 'DBteam',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 0
    }
dag = DAG(
    'admin_migrate_ddl',
    default_args=default_args,
    description='create the table structure',
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    doc_md = docs
)
def get_pgschema(tablename,**kwargs):
    table_schema = tablename.split('.')[0]
    table_name = tablename.split('.')[1]
    sql="""
        with cte as (
        select
            '{{"mode": "'
            || case when is_nullable = 'YES' then 'NULLABLE'
            else 'REQUIRED'
        end || '", "name": "' || column_name || '", "type": "'
        || case when data_type = 'bigint' then 'INT64'
        when data_type = 'bigserial' then 'INT64'
        when data_type = 'bit' then 'INT64'
        when data_type = 'bit varying' then 'INT64'
        when data_type = 'boolean' then 'BOOL'
        when data_type = 'bytea' then 'BYTEA'
        when data_type = 'character' then 'STRING'
        when data_type = 'character varying' then 'STRING'
        when data_type = 'date' then 'DATE'
        when data_type = 'decimal' then 'FLOAT64'
        when data_type = 'double precision' then 'FLOAT64'
        when data_type = 'integer' then 'INT64'
        when data_type = 'money' then 'FLOAT64'
        when data_type = 'real' then 'FLOAT64'
        when data_type = 'serial' then 'INT64'
        when data_type = 'smallint' then 'INT64'
        when data_type = 'text' then 'STRING'
        when data_type = 'time without time zone' then 'TIME'
        when data_type = 'time with time zone' then 'STRING'
        when data_type = 'timestamp without time zone' then 'DATETIME'
        when data_type = 'timestamp withtime zone' then 'TIMESTAMP'
        when data_type = 'uuid' then 'STRING'
        when data_type = 'numeric' then 'FLOAT64'
        else 'STRING'
        end || '"}}' as type
        from
        information_schema.columns
        where
        table_schema = '{}'
        and table_name = '{}'
        order by
        ordinal_position asc)
        select
            '[' || string_agg(type, ', ')|| ']' as schema
        from
            cte;
    """.format(table_schema,table_name)
    
    postgres_hook = PostgresHook(postgres_conn_id=pg_conn)
    records = postgres_hook.get_records(sql=sql)
    print(records[0][0])
    return records[0][0]

def bq_create(tablename,bqdataset,**kwargs):
    table_schema = tablename.split('.')[0]
    table_name = tablename.split('.')[1]
    ti = kwargs['ti']
    target_dataset = bqdataset
    schema = json.loads(str(ti.xcom_pull(task_ids='getschema_{}'.format(tablename))))
    create = BigQueryCreateEmptyTableOperator (
        task_id='create_bq_{}'.format(tablename),
        project_id = gcp_project,
        dataset_id=target_dataset,
        table_id=table_name,
        schema_fields=schema,
        bigquery_conn_id=bq_conn
        )
    print(schema)
    create.execute(context=kwargs)

for table_name in table_list:

    get_schema = PythonOperator(
            task_id="getschema_{}".format(table_name),
            python_callable=get_pgschema,
            op_kwargs={'tablename':table_name},
            provide_context=True,
            dag=dag
        )
    bqcreate = PythonOperator(
            task_id="bqcreate_{}".format(table_name),
            python_callable=bq_create,
            op_kwargs={'tablename':table_name, 'bqdataset':target_dataset},
            provide_context=True,
            dag=dag
        )
    get_schema >> bqcreate


# Author: Bhuvanesh
# Blog: https://thedataguy.in/
# Version: 1.0

import json
from airflow import DAG
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow.contrib.hooks.bigquery_hook  import BigQueryHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from customoperator.custom_PostgresToGCSOperator import  custom_PostgresToGCSOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator


#DAG details
docs = """
**Purpose**

This DAG to sync the tables incrementally(insert and update).

**DAG Location:**

`gs://europe-west3-bucket/dags/mergeload/generate_dag_mergeload.py`

`gs://europe-west3-bucket/dags/customoperator/custom_PostgresToGCSOperator.py`

**Config File:** `gs://europe-west3-bucket/dags/mergeload/config.json`

"""
# Add args and Dag
default_args = {
    'owner': 'DBteam',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': ['deteam@domain.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
    }

now =str('{{ execution_date.strftime("%Y%m%d%H%M") }}')

# Get the list of tables with their properties
with open('/home/airflow/gcs/dags/mergeload/config.json','r') as conf:
    config = json.loads(conf.read())

# Function for getting the max timestamp
def get_max_ts(dag,tablename,schedule,**kwargs):
    dataset = tablename.split('.')[1]
    table_name = tablename.split('.')[2]
    max_identifier = config[schedule][tablename][0]
    pg_conn = config[schedule][tablename][1]
    bq_hook = BigQueryHook(bigquery_conn_id='bigquery_default',location='europe-west3',use_legacy_sql=False)
    bqmaxts = bq_hook.get_records(sql="select max_value from bqadmin.tablesync_meta where tablename='{}' and datasource_dbconn='{}'".format(dataset + '.' + table_name,pg_conn))
    result_bqmaxts = bqmaxts[0][0]
    print(result_bqmaxts)
    postgres_hook = PostgresHook(postgres_conn_id=pg_conn)
    with open('/home/airflow/gcs/dags/mergeload/maxts_sql/maxts_' + dataset + '_' + table_name + '_' + pg_conn + '.sql','r') as sqlfile:
        pg_maxts_query=str(sqlfile.read())
    pgmaxts = postgres_hook.get_records(sql=pg_maxts_query.format(max_identifier,max_identifier,result_bqmaxts))
    print(pgmaxts[0][0])
    return bqmaxts[0][0],pgmaxts[0][0]
    
# Function to export the pg data to GCS
def pgexport(ts,dag,tablename,schedule, **kwargs):
    table_schema = tablename.split('.')[1]
    table_name = tablename.split('.')[2]
    sync_interval = schedule
    postgres_conn = config[schedule][tablename][1]
    ti = kwargs['ti']
    max_ts = ti.xcom_pull(task_ids='get_maxts_{}_{}'.format(tablename,postgres_conn))
    print(max_ts)
    bqmaxts = max_ts[0]
    pgmaxts = max_ts[1]
    print(bqmaxts)
    print(pgmaxts)
    with open('/home/airflow/gcs/dags/mergeload/export_sql/source_export_' + table_schema + '_' + table_name + '_' + postgres_conn + '.sql','r') as sqlfile:
        export_query=str(sqlfile.read())
    export_tables = custom_PostgresToGCSOperator(
        task_id='export_{}'.format(table_name),
        postgres_conn_id=postgres_conn,
        google_cloud_storage_conn_id='google_cloud_storage_default',
        sql=export_query.format(bqmaxts,pgmaxts),
        export_format='csv',
        field_delimiter='|',
        approx_max_file_size_bytes=50000000,
        bucket='prod-data-sync-bucket',
        filename='mergeload/data/' + sync_interval + '/' + ts + '/' + table_schema + '_' + table_name + '_' + postgres_conn + '/' + table_schema + '_' + table_name + '_{}.csv',
        #schema_filename='mergeload/schema/' + sync_interval + '/' + table_schema + '_' + table_name +'/' + table_schema + '_' + table_name + '.json',
        dag=dag
        )
    export_tables.execute(None)

# Import the GCS data to BQ database
def bqimport(ts,dag,tablename,schedule,**kwargs):
    pg_conn = config[schedule][tablename][1]
    table_schema = tablename.split('.')[1]
    table_name = tablename.split('.')[2]
    export_datetime = ts
    sync_interval = schedule
    bqload = GoogleCloudStorageToBigQueryOperator(
        task_id='func_bqload_{}'.format(table_name),
        bucket='prod-data-sync-bucket',
        destination_project_dataset_table=table_schema + '.tbl_' + table_name,
        create_disposition='CREATE_IF_NEEDED',
        source_format='csv',
        field_delimiter='|',
        autodetect=False,
        schema_object='json_schema/' + table_schema + '_' + table_name + '.json',
        source_objects=['mergeload/data/' + sync_interval + '/' + ts + '/' + table_schema + '_' + table_name + '_' + pg_conn + '/*.csv'],
        quote_character='"',
        allow_quoted_newlines=True,
        allow_jagged_rows=True,
        write_disposition='WRITE_APPEND',
        bigquery_conn_id='bigquery_default',
        google_cloud_storage_conn_id='google_cloud_storage_default',
        skip_leading_rows=1,
        dag=dag
        )
    bqload.execute(None)

def metatable_update(dag,tablename,schedule,**kwargs):
    dataset = tablename.split('.')[1]
    table_name = tablename.split('.')[2]
    bq_hook = BigQueryHook(bigquery_conn_id='bigquery_default',location='europe-west3',use_legacy_sql=False)
    pg_conn = config[schedule][tablename][1]
    ti = kwargs['ti']
    pgmax_ts = str(ti.xcom_pull(task_ids='get_maxts_{}_{}'.format(tablename,pg_conn))[1])
    bq_hook.run(sql="update bqadmin.tablesync_meta set max_value='{}' where tablename='{}' and datasource_dbconn='{}'".format(pgmax_ts,dataset + '.' + table_name,pg_conn))
    return 'Executed the update query'

for schedule, tables in config.items(): 
    
    if schedule == '2mins':
        cron_time = '*/2 * * * *'
    if schedule == '10mins':
        cron_time = '*/10 * * * *'

    dag_id = 'merge_every_{}'.format(schedule)

    dag = DAG(
        dag_id ,
        default_args=default_args,
        description='Incremental load - Every {}'.format(schedule),
        schedule_interval=cron_time,
        catchup=False,
        max_active_runs=1,
        concurrency=6,
        doc_md = docs
    )
    # Dummy Operators
    start = DummyOperator(task_id='start', dag=dag)
    end = DummyOperator(task_id='end', dag=dag)

    # Looping over the tables to create the tasks for 
    # each table in the current schedule
    for table_name, table_config in tables.items():
        max_ts = PythonOperator(
            task_id="get_maxts_{}_{}".format(table_name,table_config[1]),
            python_callable=get_max_ts,
            op_kwargs={'tablename':table_name, 'schedule': schedule, 'dag': dag},
            provide_context=True,
            dag=dag
        )

        export_gcs = PythonOperator(
            task_id='export_gcs_{}_{}'.format(table_name,table_config[1]),
            python_callable=pgexport,
            op_kwargs={'tablename':table_name,'schedule': schedule, 'ts':now, 'dag': dag},
            provide_context=True,
            dag=dag
        )

        bq_load = PythonOperator(
            task_id='bq_load_{}_{}'.format(table_name,table_config[1]),
            python_callable=
            bqimport,
            op_kwargs={'tablename':table_name,'schedule': schedule, 'ts':now, 'dag': dag},
            provide_context=True,
            dag=dag
        )  

        update_meta =   PythonOperator(
            task_id="update_tablemeta_{}_{}".format(table_name,table_config[1]),
            python_callable=metatable_update,
            op_kwargs={'tablename':table_name, 'schedule': schedule, 'dag': dag},
            provide_context=True,
            dag=dag
        )
        
        # The pipeline
        start >> max_ts 
        max_ts >> export_gcs >> bq_load >> update_meta
        update_meta >> end

    # DAG is created among the global objects
    globals()[dag_id] = dag


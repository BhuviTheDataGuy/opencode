"""
Author: bhuvi
Version: 1.0
"""
from airflow import DAG
import json
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.hooks.bigquery import  BigQueryHook

# Add args and Dag
default_args = {
	'owner': 'DBteam',
	'depends_on_past': False,
	'start_date': days_ago(1)
	}
dag = DAG(
	'parition_demo',
	default_args=default_args,
	description='create the table structure',
	schedule_interval=None,
	catchup=False,
	max_active_runs=1
)

tableresource = """{

	"schema": {
		"fields": [
			[{
					"mode": "NULLABLE",
					"name": "name",
					"type": "STRING"
				},
				{
					"mode": "NULLABLE",
					"name": "id",
					"type": "INT64"
				}
			]
		]
	},
	"rangePartitioning": {
		"field": "id",
		"range": {
			"start": "1",
			"end": "100",
			"interval": "10"
		}
	}
}"""
def bqcreate():
	bqcreate = BigQueryHook().create_empty_table(
	project_id="project-staging",
	dataset_id='test',
	table_id="partitiontable",
	table_resource = json.loads(tableresource)
	)
	

bqcreate = PythonOperator(
			task_id="bqcreate",
			python_callable=bqcreate,
			dag=dag
		)
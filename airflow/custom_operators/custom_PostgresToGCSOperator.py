# Custom PostgreSQL to GCS operator
# Created date: 2020-11-02
# Version: 1.0
# Modified by: Bhuvanesh
"""
Version history:

2020-11-02: Added more data type in the mapping
2020-11-02: Json conversion function modified to support DATE, TIME, TIMESTAMP data types
"""

"""
PostgreSQL to GCS operator.
"""

import datetime
import json
import time
from decimal import Decimal
from typing import Dict

import pendulum

from airflow.providers.google.cloud.transfers.sql_to_gcs import BaseSQLToGCSOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.decorators import apply_defaults


class custom_PostgresToGCSOperator(BaseSQLToGCSOperator):
    """
    Copy data from Postgres to Google Cloud Storage in JSON or CSV format.
    :param postgres_conn_id: Reference to a specific Postgres hook.
    :type postgres_conn_id: str
    """

    ui_color = '#a0e08c'

    type_map = {
        20: 'INT64',
        20: 'INT64',
        1560: 'INT64',
        1560: 'INT64',
        16: 'BOOL',
        17: 'BYTEA',
        1043: 'STRING',
        1043: 'STRING',
        1082: 'DATE',
        1700: 'FLOAT64',
        701: 'FLOAT64',
        23: 'INT64',
        790: 'FLOAT64',
        700: 'FLOAT64',
        23: 'INT64',
        21: 'INT64',
        25: 'STRING',
        1083: 'TIME',
        1266: 'STRING',
        1114: 'DATETIME',
        1184: 'TIMESTAMP',
    }

    @apply_defaults
    def __init__(self, *, postgres_conn_id='postgres_default', **kwargs):
        super().__init__(**kwargs)
        self.postgres_conn_id = postgres_conn_id

    def query(self):
        """
        Queries Postgres and returns a cursor to the results.
        """
        hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(self.sql, self.parameters)
        return cursor

    def field_to_bigquery(self, field) -> Dict[str, str]:
        return {
            'name': field[0],
            'type': self.type_map.get(field[1], "STRING"),
            'mode': 'REPEATED' if field[1] in (1009, 1005, 1007, 1016) else 'NULLABLE',
        }

    def convert_type(self, value, schema_type):
        if isinstance(value, (datetime.datetime, datetime.date)):
            return str(value)
        if isinstance(value, datetime.time):
            return str(value)
        if isinstance(value, dict):
            return json.dumps(value)
        if isinstance(value, Decimal):
            return float(value)
        return value

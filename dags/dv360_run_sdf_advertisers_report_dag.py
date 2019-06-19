###########################################################################
#
#  Copyright 2018 Google Inc.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#
###########################################################################
"""Example DAG which runs DV360 report."""
import sys
sys.path.append('..')
from datetime import datetime
from datetime import timedelta
from airflow import DAG
from airflow import models
from operators.dv360 import dv360_run_query_operator


def yesterday():
  return datetime.today() - timedelta(days=1)


default_args = {
    'owner': 'airflow',
    'start_date': yesterday(),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=10),
}

conn_id = 'gmp_reporting'
dag_name = 'dv360_run_sdf_advertisers_report_dag'
query_id = models.Variable.get('dv360_sdf_advertisers_report_id')
dag = DAG(dag_name, catchup=False, default_args=default_args)

run_query_task = dv360_run_query_operator.DV360RunQueryOperator(
    task_id='run_dv360_report',
    conn_id=conn_id,
    depends_on_past=False,
    query_id=query_id,
    dag=dag)

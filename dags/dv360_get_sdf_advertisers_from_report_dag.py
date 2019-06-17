###########################################################################
#
#  Copyright 2019 Google Inc.
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
"""Example DAG which extracts Advertiser IDs from a DV360 report."""
import sys
sys.path.append('..')
from datetime import datetime
from datetime import timedelta
from airflow import DAG
from airflow import models
from operators.dv360 import dv360_get_report_file_path_operator
from operators.dv360 import dv360_get_sdf_advertisers_from_report_operator


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
dag_name = 'dv360_get_sdf_advertisers_from_report_dag'
report_type = 'sdf_advertisers'
query_id = models.Variable.get('dv360_sdf_advertisers_report_id')
gcs_bucket = models.Variable.get('gcs_bucket')

dag = DAG(
    dag_name,
    catchup=False,
    default_args=default_args,
    schedule_interval='@daily')

get_path_task = dv360_get_report_file_path_operator.DV360GetReportFileOperator(
    task_id='get_dv360_report_path',
    conn_id=conn_id,
    depends_on_past=False,
    query_id=query_id,
    report_type=report_type,
    dag=dag)

get_advertisers_task = dv360_get_sdf_advertisers_from_report_operator.DV360GetSDFAdvertisersFromReportOperator(
    task_id='get_advertisers_from_report',
    conn_id=conn_id,
    depends_on_past=False,
    report_type=report_type,
    dag=dag)

get_path_task >> get_advertisers_task

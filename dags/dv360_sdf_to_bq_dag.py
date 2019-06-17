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
"""Example DAG with tasks for uploading DV360 SDFs to BigQuery.

DAG requirements:
  Airflow variables:
    sdf_file_types: A comma separated list of sdf file types you want to
      process (e.g.:LINE_ITEM,AD).
    sdf_api_version: version for SDF API call (e.g.: 4.2).
    number_of_advertisers_per_sdf_api_call: number of how many advertiser IDs
      will be passed in a single API call (e.g: 10).
    dv360_sdf_advertisers: A dictionary where key is partner ID and value is a
      list of advertisers under this partner (e.g.:{"234340": ["2335631",
      "2099336],"100832": ["1592931"]}). You can use
      dv360_get_sdf_advertisers_from_report_dag.py to populate this variable
      from a report you've created.
    sdf_bq_dataset: Name of BQ data set where SDF tables will be created.
    cloud_project_id: Your cloud project ID (e.g:orchestra-sandbox).
    gcs_bucket: GCS bucket where DV360 SDF files will be stored for processing.
      Just the bucket name, no *gs://* prefix
"""
import sys
sys.path.append('..')
from datetime import datetime
from datetime import timedelta
from airflow import DAG
from airflow import models
from operators.dv360 import dv360_sdf_to_bq_operator
import logging
import json
logger = logging.getLogger(__name__)


def yesterday():
  return datetime.today() - timedelta(days=1)


def group_advertisers(l, n):
  for i in range(0, len(l), n):
    yield l[i:i + n]


default_args = {
    'owner': 'airflow',
    'start_date': yesterday(),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=10)
}

dag_name = 'dv360_sdf_uploader_to_bq_dag'

conn_id = 'gmp_reporting'
filter_type = 'ADVERTISER_ID'
file_types = models.Variable.get('sdf_file_types').split(',')
api_version = models.Variable.get('sdf_api_version')
group_size = int(models.Variable.get('number_of_advertisers_per_sdf_api_call'))
advertisers_per_partner = models.Variable.get('dv360_sdf_advertisers')
advertisers_per_partner = json.loads(advertisers_per_partner)
bq_dataset = models.Variable.get('sdf_bq_dataset')
cloud_project_id = models.Variable.get('cloud_project_id')
gcs_bucket = models.Variable.get('gcs_bucket')
dag = DAG(
    dag_name,
    catchup=False,
    default_args=default_args,
    schedule_interval='@daily')

tasks = []
first = True
for partner, advertisers in advertisers_per_partner.items():
  advertiser_groups = group_advertisers(advertisers, group_size)
  for group_number, group in enumerate(advertiser_groups):
    message = 'RUNNING REQUESTS FOR A PARTNER: %s, ADVERTISERS: %s' % (
        partner, group)
    logger.info(message)
    task_id = 'upload_sdf_%s_%s_%s' % (partner, group_number, group[0])
    write_disposition = 'WRITE_APPEND'
    if first:
      write_disposition = 'WRITE_TRUNCATE'
      first = False
    task = dv360_sdf_to_bq_operator.DV360SDFToBQOperator(
        task_id=task_id,
        conn_id=conn_id,
        depends_on_past=False,
        cloud_project_id=cloud_project_id,
        gcs_bucket=gcs_bucket,
        bq_dataset=bq_dataset,
        write_disposition=write_disposition,
        filter_ids=group,
        api_version=api_version,
        file_types=file_types,
        filter_type=filter_type,
        dag=dag)
    tasks.append(task)

tasks[0] >> tasks[1:]

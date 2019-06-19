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
"""Example DAG which creates DV360 report."""
import sys
sys.path.append("..")
from datetime import datetime
from datetime import timedelta
from airflow import DAG
from airflow import models
from operators.dv360 import dv360_create_query_by_json_operator
import json


def yesterday():
  return datetime.today() - timedelta(days=1)


default_args = {
    "owner": "airflow",
    "start_date": yesterday(),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=10),
}

conn_id = "gmp_reporting"
dag_name = "dv360_create_sdf_advertisers_report_dag"
output_var = "dv360_sdf_advertisers_report_id"
body = {
    "kind": "doubleclickbidmanager#query",
    "metadata": {
        "title": "Advertiser IDs",
        "dataRange": "LAST_30_DAYS",
        "format": "CSV",
        "running": False,
        "reportCount": 0,
        "googleCloudStoragePathForLatestReport": "",
        "latestReportRunTimeMs": "0",
        "googleDrivePathForLatestReport": "",
        "sendNotification": False
    },
    "params": {
        "type": "TYPE_GENERAL",
        "groupBys": ["FILTER_ADVERTISER", "FILTER_PARTNER"],
        "filters": [],
        "metrics": ["METRIC_IMPRESSIONS"],
        "includeInviteData": True
    },
    "schedule": {
        "frequency": "DAILY",
        "endTimeMs": "1861873200000",
        "nextRunMinuteOfDay": 0,
        "nextRunTimezoneCode": "Europe/London"
    },
    "timezoneCode": "Europe/London"
}

partner_ids = models.Variable.get("partner_ids").split(",")

# Add partner ID filters using partner_id variable
for partner_id in partner_ids:
  body.get("params").get("filters").append({
      "type": "FILTER_PARTNER",
      "value": partner_id
  })

body = json.dumps(body)
dag = DAG(
    dag_name, catchup=False, default_args=default_args, schedule_interval=None)
create_query_task = dv360_create_query_by_json_operator.DV360CreateQueryOperator(
    task_id="create_dv360_report",
    conn_id=conn_id,
    depends_on_past=False,
    body=body,
    output_var=output_var,
    dag=dag)

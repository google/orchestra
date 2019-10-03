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
from datetime import datetime
from datetime import timedelta
from airflow import DAG
from airflow import models
from google.gmp.operators.gmp_dv360_operator import DisplayVideo360CreateReportOperator


CONN_ID = "gmp_reporting"
REPORT = """{
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
        "filters": [
        {%- for partner in params.partners %}
            {% if not loop.first %}, {% endif -%}
            {"type": "FILTER_PARTNER", "value": {{ partner }}}
        {%- endfor -%}
        ],
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
}"""


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


dag = DAG(
    "dv360_create_sdf_advertisers_report_dag",
    default_args=default_args,
    schedule_interval=None)
partner_ids = models.Variable.get("partner_ids").split(",")
create_query_task = DisplayVideo360CreateReportOperator(
    task_id="create_dv360_report",
    gcp_conn_id=CONN_ID,
    report=REPORT,
    params={"partners": partner_ids},
    dag=dag)

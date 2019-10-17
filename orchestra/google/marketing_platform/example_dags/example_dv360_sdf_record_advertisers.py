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
from orchestra.google.marketing_platform.operators.display_video_360 import (
  GoogleDisplayVideo360CreateReportOperator,
  GoogleDisplayVideo360RunReportOperator,
  GoogleDisplayVideo360DeleteReportOperator,
  GoogleDisplayVideo360RecordSDFAdvertiserOperator
)
from orchestra.google.marketing_platform.sensors.display_video_360 import (
  GoogleDisplayVideo360ReportSensor
)


CONN_ID = "gmp_reporting"
PARTNER_IDS = models.Variable.get("partner_ids").split(",")
REPORT = """{
    "kind": "doubleclickbidmanager#query",
    "metadata": {
        "title": "Advertiser IDs",
        "dataRange": "LAST_30_DAYS",
        "format": "CSV",
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
        "frequency": "ONE_TIME",
    }
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
    "dv360_record_sdf_advertisers_dag",
    default_args=default_args,
    schedule_interval=None)

create_report = GoogleDisplayVideo360CreateReportOperator(
    task_id="create_report",
    gcp_conn_id=CONN_ID,
    report=REPORT,
    params={"partners": PARTNER_IDS},
    dag=dag)
query_id = "{{ task_instance.xcom_pull('create_report', key='query_id') }}"

run_report = GoogleDisplayVideo360RunReportOperator(
    task_id="run_report",
    gcp_conn_id=CONN_ID,
    query_id=query_id,
    dag=dag)

wait_for_report = GoogleDisplayVideo360ReportSensor(
    task_id="wait_for_report",
    gcp_conn_id=CONN_ID,
    query_id=query_id,
    dag=dag)
report_url = "{{ task_instance.xcom_pull('wait_for_report', key='report_url') }}"

record_advertisers = GoogleDisplayVideo360RecordSDFAdvertiserOperator(
    task_id='record_advertisers',
    conn_id=CONN_ID,
    report_url=report_url,
    variable_name='dv360_sdf_advertisers',
    dag=dag)

delete_report = GoogleDisplayVideo360DeleteReportOperator(
    task_id="delete_report",
    gcp_conn_id=CONN_ID,
    query_id=query_id,
    dag=dag)

create_report >> run_report >> wait_for_report >> record_advertisers >> delete_report

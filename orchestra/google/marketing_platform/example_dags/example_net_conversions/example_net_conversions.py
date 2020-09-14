"""
DAG ID: net_conversions_orchestra_example
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator

from orchestra.google.marketing_platform.example_dags.example_net_conversions.files_helper import (
    load_config,
    load_docs,
)
from orchestra.google.marketing_platform.operators.search_ads_360 import (
    GoogleSearchAds360DownloadReportOperator,
    GoogleSearchAds360InsertConversionOperator,
    GoogleSearchAds360InsertReportOperator,
)
from orchestra.google.marketing_platform.utils.search_ads_360_conversions import (
    GoogleSearchAds360NetConversionsCalculateOperator,
)

default_args = {
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2020, 9, 15, 12, 00),
}

dag = DAG(
    "net_conversions_orchestra_example",
    default_args=default_args,
    catchup=False,
    schedule_interval=timedelta(hours=2),
    params=load_config(__file__),
    doc_md=load_docs(__file__),
)


gcs_bucket = Variable.get("gcs_bucket")
report_id = "{{ ti.xcom_pull(task_ids='insert_report',key='report_id') }}"
conversions_file = (
    "{{ ti.xcom_pull(task_ids='process_conversions',key='conversions_file') }}"
)

start = DummyOperator(task_id="start", dag=dag)


insert_report = GoogleSearchAds360InsertReportOperator(
    task_id="insert_report",
    gcp_conn_id="search_adds_360_conn_id",
    report="report/body.json",
    params={
        "agencyId": dag.params["agencyId"],
        "advertiserId": dag.params["advertiserId"],
        **dag.params["custom_dimensions"],
    },
    dag=dag,
)

download_report = GoogleSearchAds360DownloadReportOperator(
    task_id="download_report",
    gcp_conn_id="search_adds_360_conn_id",
    report_id=report_id,
    destination_object=dag.params["report_file"],
    destination_bucket=gcs_bucket,
    dag=dag,
)

calculate_net_conversions = GoogleSearchAds360NetConversionsCalculateOperator(
    task_id="process_conversions",
    gcp_conn_id="search_adds_360_conn_id",
    report_file=dag.params["report_file"],
    conversions_file=dag.params["conversions_file"],
    dag=dag,
)

insert_net_conversions = GoogleSearchAds360InsertConversionOperator(
    task_id="insert_conversions",
    gcp_conn_id="search_adds_360_conn_id",
    conversions_file=conversions_file,
    dag=dag,
)

end = DummyOperator(task_id="end", dag=dag)

(
    start
    >> insert_report
    >> download_report
    >> calculate_net_conversions
    >> insert_net_conversions
    >> end
)

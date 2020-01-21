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

"""Example DAG with tasks for listing DCM reports and report file
"""
from datetime import timedelta
from airflow import (
    DAG,
    models
)
from airflow.utils import dates
from orchestra.google.marketing_platform.operators.google_analytics import (
  GoogleAnalyticsDeletePreviousDataUploadsOperator,
  GoogleAnalyticsModifyFileHeadersDataImportOperator,
  GoogleAnalyticsDataImportUploadOperator
)
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
# TODO(rebeccasg): update the references to these operators once composer
#  version updates

default_args = {
    'owner': 'airflow',
    "start_date": dates.days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=10)
}

conn_id = 'dt'
bq_dataset = models.Variable.get('bq_dataset')
gcs_bucket = models.Variable.get('gcs_bucket')
sql = models.Variable.get('sql')
output_table = models.Variable.get('output_table')
keep_header_on_gcs_export = models.Variable.get('keep_header_on_gcs_export')
gcs_filename = models.Variable.get('gcs_filename')
ga_data_import_custom_dimensions_header_mapping = \
  models.Variable.get('ga_data_import_custom_dimensions_header_mapping',
                      deserialize_json=True)
ga_info = models.Variable.get('ga_info', deserialize_json=True)
ga_account_id = ga_info['ga_account_id']
ga_web_property_id = ga_info['ga_web_property_id']
ga_custom_data_source_id = ga_info['ga_custom_data_source_id']

dag = DAG(
    'bq_to_ga360',
    default_args=default_args,
    schedule_interval=timedelta(1))

run_bq_query = BigQueryOperator(
    task_id='run_bq_query',
    sql=sql,
    destination_dataset_table='%s.%s' % (bq_dataset, output_table),
    use_legacy_sql=False,
    write_disposition='WRITE_TRUNCATE',
    bigquery_conn_id=conn_id,
    dag=dag
)

export_bq_data_to_gcs = BigQueryToCloudStorageOperator(
    task_id='export_bq_data_to_gcs',
    bigquery_conn_id=conn_id,
    gcs_conn_id=conn_id,
    source_project_dataset_table='%s.%s' % (bq_dataset, output_table),
    destination_cloud_storage_uris=["gs://{0}/{1}".format(gcs_bucket, gcs_filename)],
    export_format='CSV',
    print_header=keep_header_on_gcs_export,
    dag=dag
)

modify_column_headers_for_ga = GoogleAnalyticsModifyFileHeadersDataImportOperator(
    task_id='format_column_headers_for_ga',
    gcp_conn_id=conn_id,
    storage_bucket=gcs_bucket,
    storage_name_object=gcs_filename,
    custom_dimension_header_mapping=ga_data_import_custom_dimensions_header_mapping,
    dag=dag
)

upload_data_to_ga = GoogleAnalyticsDataImportUploadOperator(
    task_id='upload_data_to_ga',
    gcp_conn_id=conn_id,
    storage_bucket=gcs_bucket,
    storage_name_object=gcs_filename,
    account_id=ga_account_id,
    web_property_id=ga_web_property_id,
    custom_data_source_id=ga_custom_data_source_id,
    dag=dag
)

delete_previous_ga_uploads = GoogleAnalyticsDeletePreviousDataUploadsOperator(
    task_id='delete_previous_ga_uploads',
    gcp_conn_id=conn_id,
    account_id=ga_account_id,
    web_property_id=ga_web_property_id,
    custom_data_source_id=ga_custom_data_source_id,
    dag=dag
)

run_bq_query >> export_bq_data_to_gcs >> modify_column_headers_for_ga >> upload_data_to_ga >> delete_previous_ga_uploads

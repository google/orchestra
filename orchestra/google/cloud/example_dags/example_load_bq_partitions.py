#
# Copyright 2019 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""
Example Airflow DAG showing how to load data into a date-partitioned table.
"""

from airflow import DAG
from airflow.utils import dates
from orchestra.google.cloud.operators.gcp_bigquery_operator import (
    BigQueryPartitionLoadOperator
)


DESTINATION_TABLE = "test_bq_dataset.partitioned_results"
PARTITION_FIELD = "date"
PARTITION_VALUES = """
  SELECT
    date
  FROM
    `test_bq_dataset.campaign_performance`
  GROUP BY
    date
"""
SQL = """
  SELECT
    date,
    advertiser,
    campaign,
    impressions,
    clicks
  FROM
    `test_bq_dataset.campaign_performance`
  WHERE
    date = @date
"""

default_args = {"start_date": dates.days_ago(1)}

with DAG(
    "example_load_bq_partitions",
    default_args=default_args,
    schedule_interval=None
) as dag:
    BigQueryPartitionLoadOperator(
        task_id="import_data",
        sql=SQL,
        partition_field_name=PARTITION_FIELD,
        partition_field_values=PARTITION_VALUES,
        destination_dataset_table=DESTINATION_TABLE
    )

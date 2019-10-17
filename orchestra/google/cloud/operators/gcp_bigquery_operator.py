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

"""Collection of custom BigQuery operators.
"""

import datetime
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.hooks.bigquery_hook import BigQueryHook


class BigQueryPartitionLoadOperator(BigQueryOperator):
  """Operator used for loading query results into a date partitioned table.

  Operator will load the data produced by the date parameterized query for each
  of the partion dates provided.

  Attributes:
    sql: The sql to be executed when loading each partition. (templated)
        Can receive a sql string or a reference to a sql template file. Template
        references are recoginized by a string ending in '.sql'.
    partition_field_name: The name of the date query parameter to iterate on.
    partition_field_type: The data type for the date partition field in the
        source query. (default: 'DATE')
    partition_field_date_format: The date format for the date partition field in
        the source query. (default: '%Y-%m-%d')
    partition_field_values: List of dates to load the partitions for.
        (templated)
        Can receive a list, a sql string, or a reference to a sql template file.
        Template references are recoginized by a string ending in '.sql'.
    destination_dataset_table: Reference to a date partitioned table to load in
        ``(<project>.|<project>:)<dataset>.<table>`` format. (templated)
    write_disposition: Specifies the action that occurs if the destination
        partition already exists. (default: 'WRITE_TRUNCATE')
    time_partitioning: Configure time partitioning fields i.e. partition by
        field and type as per API specifications. (default: DAY partioning on
        'date' field)
    bigquery_conn_id: The connection ID to use when fetching connection info.
    delegate_to: The account to impersonate, if any.
    use_legacy_sql: Set to True to use legacy sql. (default: False)
    allow_large_results: Whether to allow large results. (default: True)
  """

  template_fields = ['sql', 'destination_dataset_table', 'labels',
                     'partition_field_values']
  template_ext = ['.sql']

  def __init__(self,
      sql,
      partition_field_name,
      destination_dataset_table,
      write_disposition='WRITE_TRUNCATE',
      time_partitioning={
          'type': 'DAY',
          'field': 'date'
      },
      partition_field_type='DATE',
      partition_field_date_format='%Y-%m-%d',
      partition_field_values=[],
      bigquery_conn_id='google_cloud_default',
      delegate_to=None,
      use_legacy_sql=False,
      allow_large_results=True,
      *args,
      **kwargs):
    super(BigQueryPartitionLoadOperator, self).__init__(
        sql=sql,
        destination_dataset_table=destination_dataset_table,
        write_disposition=write_disposition,
        time_partitioning=time_partitioning,
        bigquery_conn_id=bigquery_conn_id,
        delegate_to=delegate_to,
        use_legacy_sql=use_legacy_sql,
        allow_large_results=allow_large_results,
        *args,
        **kwargs)
    self.partition_field_name = partition_field_name
    self.partition_field_type = partition_field_type
    self.partition_field_date_format = partition_field_date_format
    self.partition_field_values = partition_field_values

  def _load_bq_cursor(self):
    if self.bq_cursor is None:
      hook = BigQueryHook(
          bigquery_conn_id=self.bigquery_conn_id,
          use_legacy_sql=self.use_legacy_sql,
          delegate_to=self.delegate_to
      )
      conn = hook.get_conn()
      self.bq_cursor = conn.cursor()

  def _get_partition_values(self):
    if not isinstance(self.partition_field_values, str):
      return self.partition_field_values

    values = []

    self._load_bq_cursor()
    self.bq_cursor.execute(self.partition_field_values)
    query_results = self.bq_cursor.fetchall()

    for query_result in query_results:
      values.append(query_result[0])

    return values

  def execute(self, context):
    destination_table = self.destination_dataset_table

    for partition_field_value in self._get_partition_values():
      date = datetime.datetime.strptime(
          partition_field_value, self.partition_field_date_format)
      destination_table_partition = (
          '%s$%s' % (destination_table, date.strftime('%Y%m%d')))
      self.destination_dataset_table = destination_table_partition

      self.query_params = [{
          'name': self.partition_field_name,
          'parameterType': {'type': self.partition_field_type},
          'parameterValue': {'value': partition_field_value}
      }]

      super(BigQueryPartitionLoadOperator, self).execute(context)

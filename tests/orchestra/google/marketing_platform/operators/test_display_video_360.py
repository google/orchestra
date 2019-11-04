# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
import json
import time
import unittest
import sys

from unittest import mock
from random import randint
from orchestra.google.marketing_platform.operators.display_video_360 import (
    GoogleDisplayVideo360ERFToBigQueryOperator
)

CONN_ID = 'google_cloud_default'
TASK_ID = 'test-dv360-operator'
ENTITY_TYPE = 'test'
TEST_BUCKET = 'test-bucket'
TEST_PROJECT = 'test-project'
DELIMITER = '.csv'
PREFIX = 'TEST'
MOCK_FILES = ["TEST1.csv", "TEST2.csv", "TEST3.csv"]
TEST_OBJECT = 'dir1/test-object'
LOCAL_FILE_PATH = '/home/airflow/gcp/test-object'


@mock.patch('orchestra.google.marketing_platform.utils.erf_utils.download_and_transform_erf')
@mock.patch('airflow.contrib.hooks.bigquery_hook.BigQueryHook')
@mock.patch('airflow.contrib.hooks.gcs_hook.GoogleCloudStorageHook')
class TestGoogleDisplayVideo360ERFToBigQueryOperator(unittest.TestCase):
    def setUp(self):
        # Will generate new self.rand_erf_file_name and self.test_erf_ndj values before each test suite.
        # These values are mocked to verify they are being passed properly into the bq cursor run_load
        # call as well as the proper file names are used when calling gcs_hook hook to delete the files.
        self.rand_erf_file_name = '%s_%s_%d.json' % (randint(1, 1000000), ENTITY_TYPE,
                                                     time.time() * 1e+9)
        self.test_erf_ndj = 'gs://%s/%s' % (TEST_BUCKET, self.rand_erf_file_name)

    # Tests operator when it is passed a single partner value
    def test_execute_single_partner(self, mock_gcs, mock_bq, mock_download_and_transform_erf):
        test_schema = []
        task = GoogleDisplayVideo360ERFToBigQueryOperator(
            task_id=TASK_ID,
            gcp_conn_id=CONN_ID,
            report_body=json.dumps({}),
            yesterday=False,
            entity_type=ENTITY_TYPE,
            file_creation_date=None,
            cloud_project_id=None,
            bq_table=None,
            schema=[],
            gcs_bucket=TEST_BUCKET,
            erf_bucket=TEST_BUCKET,
            partner_ids=['000'],
            write_disposition='WRITE_TRUNCATE',
        )
        # setup mock hooks and mock erf method call.
        mock_download_and_transform_erf.return_value = self.rand_erf_file_name
        task.bq_hook = mock_bq
        task.gcs_hook = mock_gcs

        task.execute(None)

        # Assert bq cursor was instantiated
        mock_bq.get_conn.return_value \
            .cursor.assert_called_once()
        # Assert bq cursor run_load called and verify
        # write_disposition="WRITE_TRUNCATE"
        mock_bq.get_conn.return_value \
            .cursor.return_value \
            .run_load.assert_called_once_with(
                None,
                test_schema,
                [self.test_erf_ndj],
                source_format='NEWLINE_DELIMITED_JSON',
                write_disposition='WRITE_TRUNCATE')
        # Assert erf_utils.download_and_transform_erf called
        mock_download_and_transform_erf.assert_called_once_with(
            mock.ANY, '000')
        # Assert bq delete called
        task.gcs_hook.delete.assert_called_once_with(
            TEST_BUCKET, self.rand_erf_file_name)

    # Tests operator when it is passed a multiple (>=2) partner values
    def test_execute_multi_partner(self, mock_gcs, mock_bq, mock_download_and_transform_erf):
        test_schema = []
        task = GoogleDisplayVideo360ERFToBigQueryOperator(
            task_id=TASK_ID,
            gcp_conn_id=CONN_ID,
            report_body=json.dumps({}),
            yesterday=False,
            entity_type=None,
            file_creation_date=None,
            cloud_project_id=None,
            bq_table=None,
            schema=test_schema,
            gcs_bucket=TEST_BUCKET,
            erf_bucket=TEST_BUCKET,
            partner_ids=['000', '111'],
            write_disposition='WRITE_TRUNCATE',
        )
        # Make sure to setup mocks for each hook used in the operator
        mock_download_and_transform_erf.return_value = self.rand_erf_file_name
        task.bq_hook = mock_bq
        task.gcs_hook = mock_gcs
        task.execute(None)

        # Assert bq cursor instantiated twice.
        cursor_calls = [mock.call(), mock.call()]
        mock_bq.get_conn.return_value \
            .cursor.assert_has_calls(cursor_calls, any_order=True)

        # Assert bq cursor run_load called twice and verify
        # write_disposition="WRITE_TRUNCATE" for the first call and
        # write_disposition="WRITE_APPEND" for each subsequent call
        run_load_calls = [
            mock.call(
                None,
                test_schema,
                mock.ANY,
                source_format='NEWLINE_DELIMITED_JSON',
                write_disposition='WRITE_TRUNCATE'),
            mock.call(
                None,
                test_schema,
                mock.ANY,
                source_format='NEWLINE_DELIMITED_JSON',
                write_disposition='WRITE_APPEND')
        ]
        mock_bq.get_conn.return_value \
            .cursor.return_value \
            .run_load.assert_has_calls(run_load_calls, any_order=False)

        # Assert erf_utils.download_and_transform_erf called twice
        mock_erf_transform_calls = [
            mock.call(mock.ANY, '000'),
            mock.call(mock.ANY, '111')
        ]
        mock_download_and_transform_erf.assert_has_calls(mock_erf_transform_calls, any_order=True)

        # Assert bq delete called twice
        mock_bq_delete_calls = [
            mock.call(TEST_BUCKET, self.rand_erf_file_name),
            mock.call(TEST_BUCKET, self.rand_erf_file_name)
        ]
        task.gcs_hook.delete.assert_has_calls(mock_bq_delete_calls, any_order=True)

    # Tests operator when it is passed a no partner values
    def test_execute_no_partner(self, mock_gcs, mock_bq, mock_download_and_transform_erf):
        test_schema = []
        task = GoogleDisplayVideo360ERFToBigQueryOperator(
            task_id=TASK_ID,
            gcp_conn_id=CONN_ID,
            report_body=json.dumps({}),
            yesterday=False,
            entity_type=None,
            file_creation_date=None,
            cloud_project_id=None,
            bq_table=None,
            schema=test_schema,
            gcs_bucket=TEST_BUCKET,
            erf_bucket=TEST_BUCKET,
            partner_ids=[],
            write_disposition='WRITE_TRUNCATE',
        )
        # Make sure to setup mocks for each hook used in the operator
        task.bq_hook = mock_bq
        task.gcs_hook = mock_gcs
        task.execute(None)

        # Assert bq cursor hook was not called
        mock_bq.get_conn.return_value \
            .cursor.assert_not_called()
        # Assert bq cursor run_load was not called
        mock_bq.get_conn.return_value \
            .cursor.return_value \
            .run_load.assert_not_called()
        # Assert erf_utils.download_and_transform_erf was not called
        mock_download_and_transform_erf.assert_not_called()
        # Assert gcs_hook delete was not called
        task.gcs_hook.delete.assert_not_called()


if __name__ == '__main__':
    suite = unittest.TestLoader().loadTestsFromModule(sys.modules[__name__])
    unittest.TextTestRunner(verbosity=3).run(suite)

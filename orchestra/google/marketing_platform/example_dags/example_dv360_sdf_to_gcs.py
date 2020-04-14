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
import os
import airflow
from datetime import datetime, timedelta
from airflow import DAG , models
from airflow.models import Variable
from airflow.operators import UnzipOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.file_to_gcs import FileToGoogleCloudStorageOperator
from orchestra.google.marketing_platform.utils.schema.erf import (
  Entity_Schema_Lookup
)
from orchestra.google.marketing_platform.operators.display_video_360 import (
  GoogleDisplayVideo360SDFdownloadTaskCreateOperator,
  GoogleDisplayVideo360MediaDownloadOperator
)
from orchestra.google.marketing_platform.sensors.display_video_360 import (
	GoogleDisplayVideo360SDFdownloadTaskOperationGetSensor,
)


def yesterday():
  return datetime.today() - timedelta(days=1)

def rename_sdf_files(advertiser_id, sdf_unziped_path, sdf_zip_path, today_date):
	print('Rename Sdf and move to new folder ....')
	# check for the holding directory to be there
	if not os.path.exists(sdf_store_path):
		os.makedirs(sdf_store_path)
	# get all the data files
	for file in os.listdir(sdf_unziped_path):
		print('Renaming ' + str(file))
		filename = file.split('.')[0]
		new_file_name = file.replace("SDF-", "SDF_{}_{}_".format(advertiser_id,today_date))
		os.rename(
				sdf_unziped_path+file,
				sdf_store_path + new_file_name 
				)
	print("Remove old files / directory")
	os.removedirs(sdf_unziped_path)
	os.remove(sdf_zip_path)
	return True

today_date = datetime.now().strftime("%Y%m%d")

# Add your advertisers IDs as array
advertiser_IDs = [XXXXXX,XXXXXX]
data_folder = "/home/airflow/gcs/data/"
sdf_store_path = data_folder +"SDFs/"
gcs_bucket = 'YOUR_BUCKET'


fileTypes = ["FILE_TYPE_AD", "FILE_TYPE_AD_GROUP", "FILE_TYPE_CAMPAIGN", "FILE_TYPE_LINE_ITEM","FILE_TYPE_INSERTION_ORDER"]
sdf_file_output = ["AdGroupAds","AdGroups","Campaigns","LineItems","InsertionOrders"]

file_creation_date = yesterday()
file_creation_date = file_creation_date.strftime('%Y%m%d')
default_args = {
	'owner': 'airflow',
	'start_date': yesterday(),
	'email_on_failure': False,
	'email_on_retry': False,
	'retries': 1,
	'retry_delay': timedelta(seconds=10)
}

dag = DAG(
	'sdfv2',
	default_args=default_args,
	schedule_interval=timedelta(1))

# Create a taskflow for each advertiser_Id
for advertiser_id in advertiser_IDs:

	sdf_file = "sdf_{}_{}".format(advertiser_id, today_date)
	sdf_zip_path = "{}{}.zip".format(data_folder, sdf_file)
	sdf_unziped_path = "{}{}/".format(data_folder, sdf_file)

	# Create the SDF task
	create_download_task = GoogleDisplayVideo360SDFdownloadTaskCreateOperator(
		task_id="create_download_task_%s" % advertiser_id,
		advertiser_id=advertiser_id,
		file_types=fileTypes,
		filter_type="FILTER_TYPE_NONE",
		dag=dag)
	operation_name = "{{ task_instance.xcom_pull('create_download_task_%s', key='sdfdownloadtasks_operation') }}" % advertiser_id

	# Check once the file is ready to be downloaded
	# ping the API every 30 sec untill it's done
	wait_download_task = GoogleDisplayVideo360SDFdownloadTaskOperationGetSensor(
		task_id="wait_download_task_%s" % advertiser_id,
		operation_name=operation_name,
		dag=dag)
	resourceName = "{{ task_instance.xcom_pull('wait_download_task_%i', key='resourceName') }}" % advertiser_id

	# Download the SDF File ( Zip media )
	download_sdf = GoogleDisplayVideo360MediaDownloadOperator(
		task_id="download_sdf_%s" % advertiser_id,
		resourceName=resourceName,
		filePath=sdf_zip_path,
		dag=dag)

	# unzip the downloaded file
	unzip_task = UnzipOperator(
		task_id="unzip_task_%s" % advertiser_id,
		path_to_zip_file=sdf_zip_path,
		path_to_unzip_contents=sdf_unziped_path,
		dag=dag)


	# TASK: move the files from inside the unziped folder to the local folder "data/SDFs"
	rename_files = PythonOperator(
		task_id="rename_files_%s" % advertiser_id,
		python_callable=rename_sdf_files,
		op_kwargs={	"advertiser_id": advertiser_id,
					"sdf_unziped_path": sdf_unziped_path,
					"sdf_zip_path": sdf_zip_path,
					"today_date": today_date},
		dag=dag)

	#  TASK: Upload files to GCS and delete the Local File
	for sdf_file_type in sdf_file_output:
		file_name = 'SDF_{}_{}_{}.csv'.format(advertiser_id,today_date,sdf_file_type)
		local_file_path = sdf_store_path + file_name
		upload_file = FileToGoogleCloudStorageOperator(
			task_id="upload_file_{}_{}".format(sdf_file_type,advertiser_id),
			src=local_file_path,
			dst="data/{}".format(file_name),
			bucket=gcs_bucket,
			dag=dag)
		delete_file = BashOperator(
			task_id="delete_local_file_{}_{}".format(sdf_file_type,advertiser_id),
			bash_command='rm {}'.format(local_file_path),
			dag=dag,
		)
		# Concatenate the tasks
		rename_files >> upload_file >> delete_file

	# CONCATENATE the tasks
	create_download_task >> wait_download_task >> download_sdf >> unzip_task >> rename_files


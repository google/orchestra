# Orchestra

Orchestra is a framework for importing and exporting Display & Video 360 data via Apache Airflow running on Google Cloud Composer.

Currently we support exporting:

* Entity Read Files
* DV360 Reports
* Campaign Manager Reports

And Importing to

* BigQuery
* Google Cloud Storage

### Is Orchestra right for your project?

Orchestra is designed for importing data on a scheduled basis.

It is not designed for single use imports.

If you are using it is worth making sure you are comfortable handling the overhead of Composer

https://cloud.google.com/composer/docs/

And Apache Airflow

https://airflow.apache.org/


### Getting started

The main hurdle to getting started is permissions for DV360.

You will need to add your projects service account to three areas depending on which data you wish to access.

#### Entity Read Files

You will need to add either your projects default service worker or another connector with the correct permissions to the Google Group managing this resource.

You can find your groups name in DV360 at:

Settings > Basic Details > Entity Read Files Configuration  > Entity Read Files Read Google Group

This is only required for Private Entity Read Files.

If you wish to download entity read files from multiple Partners you will need to add the service account to each group.

You may also find it easier to replace multiple Google Groups with a single Google Group containing existing Google Groups in this case.

#### CM

In order to use the authenticated CM calls in CM you will need to create a User in DCM from

ADMIN > User Profiles

This user will need Read and Write Access permissions.

Add a connector for CM

#### DV360

Under User Management Create a New User with the service account email and give it Read & Write Access.

Add a connector for DV360

### An Example Workflow for setting up Entity Read File imports to BigQuery

The ideal way to use Orchestra is to create DAGs from the operators that import or export data.

Create a new environment in Google Cloud Composer or add the following to an existing Airflow Project.

An example can be seen in

Update the following variables:

Variable | Value
---------|---------
gce_zone | Your Google Compute Engine Zone.
gcs_bucket| The full path for your Dag Folder.
partner_ids| A comma separated list of IDs for the partners you wish to add from DV360.
cloud_project_id| The full id of your project.

Copy the file for the multiple_partners_erf_upload_bq_dag.py dag, the dv360_multi_file_upload_erf.py operator and the entire schema and utils folders required to your dag folder in Google Cloud Storage.

The Worflow should start running automatically and be scheduled daily.

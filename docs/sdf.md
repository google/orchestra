# Structured Data Files

Below we will explain how to set up a workflow which will import your [DV360 Structured Data Files (SDFs)](https://developers.google.com/bid-manager/guides/structured-data-file/format) to BigQuery.

### Create a new Airflow Connection or update an existing one

If you haven’t created an Airflow Connection for GMP APIs follow [Create the Airflow Connection to GMP reporting](#create-the-airflow-connection-to-gmp-reporting) step to create one. Make sure that in the last step, the following scopes are added:

https://www.googleapis.com/auth/doubleclickbidmanager,
https://www.googleapis.com/auth/devstorage.full_control,
https://www.googleapis.com/auth/bigquery

### Create an SDF advertisers report

This report will contain all active advertiser IDs with their Partner IDs which will be used to retrieve SDFs via API. To create a new SDF advertisers report, in Airflow, please manually run:

_example_dv360_create_sdf_advertisers_report_dag_

The above DAG will create a scheduled DV360 report which will run daily. After it’s successfully completed, you should see that _example_dv360_sdf_advertisers_report_id_ Airflow variable has updated with a newly created report ID.

Note: These scheduled reports expire on the 1st of January 2029.

### Run the SDF advertisers report

Once you’ve created the SDF advertisers report, please manually run the following DAG:

_example_dv360_run_sdf_advertisers_report_dag_

After it’s completed, manually run:

_example_dv360_get_sdf_advertisers_from_report_dag_

After it’s completed, you should be able to verify that _dv360_sdf_advertisers_ Airflow variable now contains relevant partner and advertiser IDs which will be used to retrieve SDFs. The above DAG will be automatically configured to run daily.

### Upload SDFs to BigQuery

Please manually run the following DAG:

_example_dv360_sdf_uploader_to_bq_dag_

To upload SDFs to BigQuery. The process will use the dictionary stored in the _dv360_sdf_advertisers_ Airflow variable to make API requests and store responses in your BigQuery dataset.

Once the DAG has completed successfully, you will find new tables in your BigQuery dataset. Tables will correspond to SDF types you’ve configured to retrieve in the _sdf_file_types_ Airflow variable (e.g. if you’ve configured “LINE_ITEMS”, you should see a table called “SDFLineItem”). The above DAG will be automatically configured to run daily.

To sum up, we’ve scheduled two DAGs which run daily and independently from each other. The first DAG downloads a report and updates an Airflow variable with your partner and advertiser IDs. The second DAG fetches Structured Data Files using partner and advertiser IDs from the Airflow variable and uploads them to BigQuery.

### Variables
The Orchestra project will require several variables to run.

These can be set via the **Admin** section in the **Airflow UI** (accessible from the list of Composer Environments, clicking on the corresponding link under "Airflow Web server").



![alt_text](images/VariableNames.png "image_tooltip")



<table>
  <tr>
   <td><strong>Area</strong>
   </td>
   <td><strong>Variable Name</strong>
   </td>
   <td><strong>Value</strong>
   </td>
   <td><strong>Needed For</strong>
   </td>
  </tr>
  <tr>
   <td>Cloud Project
   </td>
   <td><strong>gce_zone</strong>
   </td>
   <td><a href="https://cloud.google.com/compute/docs/regions-zones/">Your Google Compute Engine Zone </a>(you can find it under "Location" in the list of Composer Environments)
   </td>
   <td>All
   </td>
  </tr>
  <tr>
   <td>Cloud Project
   </td>
   <td><strong>gcs_bucket</strong>
   </td>
   <td>The Cloud Storage bucket for your Airflow DAGs (you can find a link to the bucket in the Environments page - see Image1)
   </td>
   <td>All
   </td>
  </tr>
  <tr>
   <td>Cloud Project
   </td>
   <td><strong>cloud_project_id</strong>
   </td>
   <td>The Project ID you can find in your GCP console homepage.
   </td>
   <td>All
   </td>
  </tr>
  <tr>
   <td>DV360
   </td>
   <td><strong>dv360_sdf_advertisers</strong>
   </td>
   <td>Dictionary of partners (keys) and advertisers (values) which will be used to download SDFs. Initially you can set up the value to: {"partner_id": ["advertiser_id1", “advertiser_id2”]} and use the dv360_get_sdf_advertisers_from_report_dag dag to update it programmatically.
   </td>
   <td>SDFs
   </td>
  </tr>
  <tr>
   <td>DV360
   </td>
   <td><strong>dv360_sdf_advertisers_report_id</strong>
   </td>
   <td>DV360 report ID which will be used to get a list of all active partners and advertisers. Initially, you can set up the value as: 1 and use the dv360_create_sdf_advertisers_report_dag dag to update it programmatically.
   </td>
   <td>SDFs, Reports
   </td>
  </tr>
  <tr>
   <td>DV360
   </td>
   <td><strong>number_of_advertisers_per_sdf_api_call</strong>
   </td>
   <td>Number of advertiser IDs which will be included in each call to DV360 API to retrieve SDFs. Set up the value to: 1
   </td>
   <td>SDFs
   </td>
  </tr>
  <tr>
   <td>DV360
   </td>
   <td><strong>sdf_api_version</strong>
   </td>
   <td>SDF Version (column names, types, order) in which the entities will be returned. Set up the value to: 4.2 (no other versions are currently supported).
   </td>
   <td>SDFs
   </td>
  </tr>
  <tr>
   <td>BigQuery
   </td>
   <td><strong>sdf_bq_dataset</strong>
   </td>
   <td>The name of the BigQuery dataset you wish to use to store SDFs.
   </td>
   <td>SDFs
   </td>
  </tr>
  <tr>
   <td>BigQuery
   </td>
   <td><strong>sdf_file_types</strong>
   </td>
   <td>Comma separated value of SDF types that will be returned (e.g. LINE_ITEM, AD_GROUP). Currently, this solution supports: LINE_ITEM, AD_GROUP, AD, INSERTION_ORDER and CAMPAIGN.
   </td>
   <td>SDFs
   </td>
  </tr>
</table>

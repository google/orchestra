# Orchestra

- [Overview ](#overview-)
- [Setting up your Orchestra environment in GCP ](#setting-up-your-orchestra-environment-in-gcp-)
    - [Billing ](#billing-)
    - [APIs ](#apis-)
    - [Create a Composer environment ](#create-a-composer-environment-)
- [Service Accounts ](#service-accounts-)
    - [Setting up a service account ](#setting-up-a-service-account-)
    - [Default Service Account ](#default-service-account-)
    - [Creating a new user for your service account in DV360 ](#creating-a-new-user-for-your-service-account-in-dv360-)
    - [DV360 Entity Read File Authorization ](#dv360-entity-read-file-authorization-)
    - [Multiple Partners ](#multiple-partners-)
- [Configuring Orchestra ](#configuring-orchestra-)
    - [Variables ](#variables-)
    - [Adding Workflows ](#adding-workflows-)
    - [](#)
- [GMP Reporting](#gmp-reporting)
    - [Create the Airflow Connection to GMP reporting](#create-the-airflow-connection-to-gmp-reporting)
    - [Creating a DV360 report ](#creating-a-dv360-report-)
    - [More DV360 Operators ](#more-dv360-operators-)
- [Additional info ](#additional-info-)
    - [Deleting an environment ](#deleting-an-environment-)

# Overview
**_Composer_** is a Google Cloud managed version of [Apache Airflow](https://airflow.apache.org/), an open source project for managing ETL workflows. We use it for this solution as you are able to deploy your code to production simply by moving files to Google Cloud Storage. It also provides Monitoring, Logging and software installation, updates and bug fixes for Airflow are fully managed.

It is recommended that you install this solution through the Google Cloud Platform UI.

We recommend familiarising yourself with Composer [here](https://cloud.google.com/composer/docs/).

**_Orchestra_** is an open source project, built on top of Composer, for managing common Display and Video 360 ETL tasks such as downloading Entity Read Files and uploading them to BigQuery.

It is available on [github](https://github.com/google/orchestra).

Below we will explain how to set up an environment for Composer, which files to use from Orchestra and how to grant access to your DV360 account to your Cloud Project.

This will create a fully managed workflow that - in our example - will import your required Entity Read Files to BigQuery.


# Setting up your Orchestra environment in GCP

### Billing
Composer and Big Query - two of the main Google Cloud Platform tools which Orchestra is based on - will require a GCP Project with a valid billing account.

See [this article](https://cloud.google.com/billing/docs/how-to/manage-billing-account) for more information Google Cloud Billing.


### APIs
In you GCP Project menu (or directly through [this link](https://console.cloud.google.com/apis/library?project=_&_ga=2.115478214.-1781341949.1549548145)) access the API Librar so that you can enable the following APIs:



*   Cloud Composer
*   Cloud Dataproc
*   Cloud Storage APIs
*   BigQuery


### Create a Composer environment
[Follow these steps to create a Composer environment](https://cloud.google.com/composer/docs/how-to/managing/creating) in Google Cloud Platform - please note that it can take up to 20/30 minutes.

For your installation you **must** set your Python version to 2, and we are assuming you are using the default service account.

Environment Variables, Tags and Configuration Properties (airflow.cfg) can all be left as standard and you can use the default values for number of nodes, machine types and disk size (you can use a smaller disk size if you want to save some costs).


# Service Accounts

### Setting up a service account
Google Cloud uses service accounts to automate tasks between services. This includes other Google services such as DV360 and CM.

You can see full documentation for Service Accounts here:

https://cloud.google.com/iam/docs/service-accounts


### Default Service Account
By default you will see in the IAM section of your Project a default service account for Composer ("Cloud Composer Service Agent") and a default service account for Compute Engine ("Compute Engine Service Agent") - with their respective email addresses.

These service accounts have access to all Cloud APIs enabled for your project, making them a good fit for Orchestra. We recommend you use in particular the Compute Engine Service Account (because it is the one used by the individual Compute Engine virtual machines that will run your tasks) as the main "Orchestra" service account.

If you wish to use another account, you will have to give it access to BigQuery and full permissions for the Storage APIs.


### Creating a new user for your service account in DV360
Your Service Account will need to be setup as a DV360 user so that it can access the required data from your DV360 account.

You need to have partner-level access to your DV360 account to be able to add a new user; [follow the simple steps to create a new user in DV360](https://support.google.com/displayvideo/answer/2723011?hl=en), using this configuration:



*   Give this user **the email of the service account you wish to use**.
*   Select all the advertisers you want to be able to access
*   Give** Read&Write** permissions
*   Save!


### DV360 Entity Read File Authorization
Entity Read Files are large json files showing the state of an account. These are held in Google Cloud Storage. Access is granted via a **Google Group**.

This is found in

**Settings > Basic Details > Entity Read Files Configuration  > Entity Read Files Read Google Group**

You should add the service account to the Entity **Read** Files Read Google Group.


![alt_text](docs/images/erf.png "image_tooltip")

Add the **service account** email to this Google Group to allow it to read private entity read files.

You can find more info on Entity Read Files access here: https://developers.google.com/bid-manager/guides/entity-read/overview.


### Multiple Partners
If you are intending to use many google groups, it is also possible to set up a single Google Group containing all other Google Groups. You can then Add the Service account to this Google Group to grant access to all accounts at once


# Configuring Orchestra
You have now set up the Composer environment in GCP and granted the proper permissions to its default Service Account. \
You're ready to configure Orchestra!


### Variables
The Orchestra project will require several variables to run.

These can be set via the **Admin** section in the **Airflow UI** (accessible from the list of Composer Environments, clicking on the corresponding link under "Airflow Web server").



![alt_text](docs/images/VariableNames.png "image_tooltip")



<table>
  <tr>
   <td><strong>Area</strong>
   </td>
   <td><strong>Variable Name</strong>
   </td>
   <td><strong>Value</strong>
   </td>
  </tr>
  <tr>
   <td>Cloud Project
   </td>
   <td><strong>gce_zone</strong>
   </td>
   <td><a href="https://cloud.google.com/compute/docs/regions-zones/">Your Google Compute Engine Zone </a>(you can find it under "Location" in the list of Composer Environments)
   </td>
  </tr>
  <tr>
   <td>Cloud Project
   </td>
   <td><strong>gcs_bucket</strong>
   </td>
   <td>The Cloud Storage bucket for your Airflow DAGs (you can find a link to the bucket in the Environments page - see Image1)
   </td>
  </tr>
  <tr>
   <td>Cloud Project
   </td>
   <td><strong>cloud_project_id</strong>
   </td>
   <td>The Project ID you can find in your GCP console homepage.
   </td>
  </tr>
  <tr>
   <td>BigQuery
   </td>
   <td><strong>erf_bq_dataset</strong>
   </td>
   <td>The name of the BigQuery Dataset you wish to use - see image2 and documentation <a href="https://cloud.google.com/bigquery/docs/datasets">here</a>.
   </td>
  </tr>
  <tr>
   <td>DV360
   </td>
   <td><strong>partner_ids</strong>
   </td>
   <td>The list of partners ids from DV360, used for Entity Read Files, comma separated.
   </td>
  </tr>
  <tr>
   <td>DV360
   </td>
   <td><strong>private_entity_types</strong>
   </td>
   <td>A comma separated list of Private Entity Read Files you would like to import.
   </td>
  </tr>
  <tr>
   <td>DV360
   </td>
   <td><strong>sequential_erf_dag_name</strong>
   </td>
   <td>The name of your dag as it will show up in the UI. Name it whatever makes sense for you (alphanumeric characters, dashes, dots and underscores exclusively).
   </td>
  </tr>
</table>


Image1:


![alt_text](docs/images/DagFolder.png "image_tooltip")




![alt_text](docs/images/FolderName.png "image_tooltip")


Image2:


![alt_text](docs/images/DataSet.png "image_tooltip")



### Adding Workflows
As with any other Airflow deployment, you will need DAG files describing your Workflows to schedule and run your tasks; plus, you'll need hooks, operators and other libraries to help building those tasks.

You can find the core files for Orchestra in [our github repository:](https://github.com/google/orchestra) clone the repo (or directly download the files) and you will obtain the following folders:



*   **dags:** includes a sample DAG file to upload multiple partners ERF files from the Cloud Storage Bucket to BigQuery
*   **hooks:** includes the hooks needed to connect to the reporting APIs of GMP platforms (CM and DV360)
*   **operators**: includes two subfolders for basic operators for CM and DV360 APIs, respectively
*   **schema:** includes files describing the structure of most CM and DV360 entities (can be useful when creating new report or to provide the schema to create a BQ table)
*   **utils**: a general purpose folder to include utility files

You can then design the dags you wish to run and add them to the **dags** folder.

Upload all the DAGs and other required files to the DAGs Storage Folder that you can access from the Airflow UI.


###



![alt_text](docs/images/buckets.png "image_tooltip")


This will automatically generate the DAGs and schedule them to run (you will be able to see them in the Airflow UI).

From now, you can use (the Composer-managed instance of) Airflow as you normally would - including the different available functionalities for scheduling, troubleshooting, …

With the sample DAG provided, if all proper accesses have been granted to the Service Account, you will be able to see the results directly in BigQuery: in the Dataset you've selected in the corresponding Variable, you will find different tables for all the Entity Read Files entities that you've chosen to import.

Congratulations!


# GMP Reporting

The example workflow we've just set up (importing Entity Read Files from Cloud Storage to Big Query) doesn't require access to DV360 (or in general GMP) reports, but that's a task that you might end up needing in other workflows. For instance, rather (or in addition to) Entity Read Files data, you might want to add aggregated performance data to BigQuery.

In order to be able to do this, you'll need to **setup a Connection to GMP reporting** (i.e. specifying your Service Accounts credentials to be used to leverage GMP APIs) and then **create a report** (and collect its results).


### Create the Airflow Connection to GMP reporting

An [Airflow Connection](https://airflow.apache.org/howto/manage-connections.html) to the GMP Reporting API is needed for the tasks which will collect DV360 (or CM) reports.

First of all, you will need to enable your Service Account to access GMP Reporting API (and the DV360 Reporting API in particular):



1.  From the _API & Services > Library_ menu in the GCP console, **look for and enable the DoubleClick Bid Manager API** (DoubleClick Bid Manager is the former name of DV360)
    1.  If necessary, also enable the DCM/DFA Reporting And Trafficking API and/or the _DoubleClick Search API _for CM and SA360 reporting respectively.
1.  From the IAM & admin > Service Accounts menu in the GCP console, **look for the default Compute Engine Service Account **(or your custom Service Account if you aren't using the default one) and click on the three-dots button under "Action" to **_Create a key_**. Pick the JSON option and store the file securely.
1.  **Upload the JSON keyfile** you've just downloaded to the Storage Bucket linked to your Composer environment (the same bucket where you're uploading DAG and other python files, but in another subfolder - e.g. "data")

You are now ready to access the Connections list in the Airflow UI (_Admin > Connections_) and click on _Create_.

Use the following values (please note that the list of fields changes depending on the "Connection Type" you select, so don't worry if you don't see these exact fields initially):


<table>
  <tr>
   <td><strong>Field</strong>
   </td>
   <td><strong>Value</strong>
   </td>
  </tr>
  <tr>
   <td><strong>Conn Id</strong>
   </td>
   <td>gmp_reporting
   </td>
  </tr>
  <tr>
   <td><strong>Conn Type</strong>
   </td>
   <td>Google Cloud Platform
   </td>
  </tr>
  <tr>
   <td><strong>Project Id</strong>
   </td>
   <td>[Your Cloud Project ID]
   </td>
  </tr>
  <tr>
   <td><strong>Keyfile Path</strong>
   </td>
   <td>The path to the JSON file you've uploaded in the Storage Bucket during the previous steps. In particular, if you have uploaded your keyfile in a <em>data</em> folder, enter:
<p>
"/home/airflow/gcs/data/[keyfile_name].json"
   </td>
  </tr>
  <tr>
   <td><strong>Keyfile JSON  </strong>
   </td>
   <td>[empty]
   </td>
  </tr>
  <tr>
   <td><strong>Scopes (comma separated)</strong>
   </td>
   <td>https://www.googleapis.com/auth/doubleclickbidmanager
<p>
Or, if necessary, also add other scopes such as:
<p>
https://www.googleapis.com/auth/dfareporting
<p>
https://www.googleapis.com/auth/doubleclicksearch
   </td>
  </tr>
</table>



### Creating a DV360 report
You can follow these **simple steps to have your Service Account create a DV360 report**, so that a subsequent task can, following our example, collect the report result and push it to BigQuery.

**_It's important that the Service Account creates the report because if you create it directly in the DV360 UI the Service Account won't be able to access the resulting files!_**

The Service Account needs to have read access to the Partners/Advertisers you're running reports for.

In this example below we are providing a DAG file (_dv360_create_report_dag.py_) which will let you manually launch the corresponding **DV360_Create_Query **DAG and will create a new report, but in order to do that you must first configure which kind of report you want to create.

To do this, you will need to add and populate a specific Variable in the Airflow UI, called **dv360_report_body**, which corresponds to the "body" of the DV360 query to be created.

Comprehensive documentation on how this object can be populated with Filters, Dimensions (GroupBys), Metrics can be found [here](https://developers.google.com/bid-manager/v1/queries#resource), and we suggest you first test your request through the API Explorer: https://developers.google.com/apis-explorer/#p/doubleclickbidmanager/v1/doubleclickbidmanager.queries.createquery

(clicking on the small downside arrow on the right you can switch from "Structured editor" to "Freeform editor", which allows you to directly copy-and-paste the JSON structure).

Here's an example of a basic request body:


```
{
  "kind": "doubleclickbidmanager#query",
  "metadata": {
    "title": "myTest",
    "dataRange": "LAST_30_DAYS",
    "format": "CSV",
    "sendNotification": false
  },
  "params": {
    "type": "TYPE_GENERAL",
    "groupBys": [
      "FILTER_ADVERTISER",
      "FILTER_INSERTION_ORDER"
    ],
    "filters": [
      {
        "type": "FILTER_PARTNER",
        "value": "12345678"
      }
    ],
    "metrics": [
      "METRIC_IMPRESSIONS",
      "METRIC_CLICKS"
    ],
    "includeInviteData": true
  },
  "schedule": {
    "frequency": "DAILY",
    "nextRunMinuteOfDay": 0,
    "nextRunTimezoneCode": "Europe/London"
  }
}

```


You can then (manually) launch your DAG from the Airflow UI: identify the DAG named "DV360_Create_Query" in the list and launch it clicking on the "Trigger Dag" link (play-button like icon).

Once the DAG has completed successfully, you will find a new variable called **dv360_latest_report_id** in the list of Variables, populated with the ID of the generated report that you can use in the following steps of your pipeline.


### More DV360 Operators
We're providing other DV360 operators, to be used in your DAGs, so that you're able to run reports, check their status and read their results:


<table>
  <tr>
   <td><strong>Operator</strong>
   </td>
   <td><strong>Function</strong>
   </td>
  </tr>
  <tr>
   <td><strong>dv360_run_query_operator</strong>
   </td>
   <td>Takes a query ID and runs the report (useful when you haven't set up the report to run on a schedule)
   </td>
  </tr>
  <tr>
   <td><strong>dv360_get_report_file_path_operator</strong>
   </td>
   <td>Given a query ID, collects the latest file path of the resulting report file and stores it in an XCOM variable.
   </td>
  </tr>
  <tr>
   <td><strong>dv360_download_report_by_file_path</strong>
   </td>
   <td>Reads the report file path from a XCOM variable and downloads it to a Cloud Storage bucket.
   </td>
  </tr>
  <tr>
   <td><strong>dv360_upload_bq_operator</strong>
   </td>
   <td>Loads a report CSV file from Cloud Storage, inferes the schema and uploads the data to a BigQuery table. Note: the BigQuery dataset needs to exist.
   </td>
  </tr>
</table>



# Additional info

### Deleting an environment
Full details can be found [here](https://cloud.google.com/composer/docs/how-to/managing/updating#deleting_an_environment). Please note that files created by Composer are not automatically deleted and you will need to remove them manually or they will still incur. Same thing applies to the BigQuery datasets.

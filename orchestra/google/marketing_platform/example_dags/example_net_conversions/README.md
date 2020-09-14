# Example Net conversions

This DAG pulls conversions from SA360 using Google Search Ads API and uploads a copy to a new floodlight activity with a new revenue calculated deducting internal costs.

## Set up the solution

The solution works with SA360 conversions (Transaction) data. Every conversion should contain four custom dimensions: the conversion Id (conversionId), the product Ids included in the transaction (productIds), the quantity purchased for every product Id (productQuantities), and the price paid for every single product (productNetPrices). We need to create that custom dimensions in SA360 before. The custom dimensions productIds, productQuantities, and productNetPrices will be strings, but they will contain information related to multiples products, so the order for every product information should be the same. We use to separate every product information the pipe `|` character.

For example:

**conversionId:** "12345"<br>
**productIds:** "A|B|C|D"<br>
**productQuantities:** "1|2|1|3"<br>
**productNetPrices:** "5|7|10|5"<br>

The conversion "12345" contains 4 different products: the product "A" was bought 1 time with a price of 5 €, the product "B" was bought 2 times with a price of 7 €, the product "C" was bought 1 time with a price of 10 €, the product "D" was bought 3 times with a price of 5 €.

To avoid overwriting conversions, we're going to upload the calculated conversions to a new floodlight activity, so we need to create that floodlight activity before.

We want to calculate the net revenue, so we need to know the internal cost of the products included in the conversion. In this solution, we are using Google Sheets for that. We have a Google Sheet with a tab where we have all the products Ids and the internal cost associated with every product Id.

The idea is to regularly pull new conversions, calculate the net value of every conversion, and upload the "new" conversions to a specific floodlight activity.

In the file [config.yaml](/orchestra/google/marketing_platform/example_dags/example_net_conversions/config.yaml) we define the configuration values mentioned before.

**Values related to SA360**

```yaml
agencyId: The agency id to pull the conversions from
advertiserId: The advertiser id to pull the conversions from
custom_dimensions:
    conversionId: Custom dimension name given to the conversion Id
    productIds: Custom dimension name given to the product Ids
    productQuantities: Custom dimension name given to the quantity purchased for every product Id
    productNetPrices: Custom dimension name given to the price paid for every single product
floodlight_activity: The floodlight activity to upload the calculated conversions
```

**Values related to Google Sheets**
```yaml
sheet_key: The Google Sheet Key where to find the internal cost of the products
products_tab_name: The Google SheetTab Name where to find the internal cost of the products
column_names:
    productId: Name of the column where the product Ids are stored
    internalCost: Name of the column where the internal costs are stored
```

**Values related to the DAG**
```yaml
report_file: Path to save the GA360 report with the conversions
conversions_file: Path to save the calculated conversions
```

### Variables

This Dags needs a variable named `gcs_bucket` with the value of the Cloud Storage bucket (without gs://) for your Airflow DAGs (you can find a link to the bucket in the Environments page).

### Connection

To make the DAG work, you need to create a new connection named `search_adds_360_conn_id` in airflow. The connection should have the following configuration:

  - Conn Id: search_adds_360_conn_id
  - Conn Type: Google Cloud Platform
  - Project Id: your-project-id
  - Keyfile JSON: content from the service account key file performing the API call. `Make sure to give permission to the service account to access Google Search Ads data and the Google Sheet with the product information.`
  - Scopes (comma separated): https://www.googleapis.com/auth/cloud-platform,https://www.googleapis.com/auth/doubleclicksearch,https://www.googleapis.com/auth/spreadsheets


## Business use case

[Profitable Sales - SA360 & GCP Automation (pdf)](./docs/Profitable_Sales_SA360_GCP_Automation.pdf)

### Who is the client and what is their challenge?
High-end suit retailer with high return rates and product costs

### What is the objective
Optimize bids towards profit instead of revenue growth

### Implementation steps in CM/SA360
1. Set up a separate floodlight activity in CM to be used for the profit data upload. There will be two conversions firing simultaneously for the sales event:
    * original online sales floodlight with the gross revenue value passed;
    * uploaded sales floodlight with the profit value passed;
2. Pass profit Margin to SA360 to a new separate floodlight activity (can use API or pass a value and amend in the system)
3. Set up ROAS / ERS bid strategy
4. SA360 will optimize profit in the same way as revenue

## Outside dependencies

In this solution, we use Google Sheets to know the internal cost of the products included in the conversions. We have a Google Sheet with a tab where we have all the products Ids and the internal cost associated with every product Id.
We make use of the GoogleSheetsHook included with orchestra code to get the sheet values.

Once we have the values from the sheet, we process them using pandas package, so the package should be included as a dependency. If you're using Google Cloud Composer, the pandas package is already included, is you are not using a managed version of Airflow make sure to include the pandas package.

### Setting up Google Sheets
We need a Google Sheet with a tab and two columns. The name of the columns are up to you but make sure to update the values in [config.yaml](/orchestra/google/marketing_platform/example_dags/example_net_conversions/config.yaml) with the Google Sheet values.

The service account used in the connection `search_adds_360_conn_id` should have access to the Google Sheet, so make sure to give it read access before.
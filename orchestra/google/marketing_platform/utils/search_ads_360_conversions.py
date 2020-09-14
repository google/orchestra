"""search_ads_360_conversions.py.py"""
import json
import logging
from pathlib import Path

import pandas as pd
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from dateutil import parser
from orchestra.google.gsuite.hooks.gsuite_sheets_hook import GoogleSheetsHook


class GoogleSearchAds360NetConversionsCalculateOperator(BaseOperator):
    """Process the conversions to calculate the net converisons

    Attributes:
        report_file: The report file downloaded from SA360.
        conversions_file: The file to store the conversions to upload.
    """

    template_fields = ["report_file", "conversions_file"]
    gsheet_hook = None

    @apply_defaults
    def __init__(
        self,
        *args,
        report_file,
        conversions_file,
        gcp_conn_id="google_cloud_default",
        **kwargs,
    ):
        super().__init__(*args, **kwargs)

        self.report_file = report_file
        self.conversions_file = conversions_file
        self.gcp_conn_id = gcp_conn_id

    def execute(self, context):
        """
        Calculates the real revenue for the conversions created since the last
        time this was executed
        """

        if self.gsheet_hook is None:
            self.gsheet_hook = GoogleSheetsHook(gcp_conn_id=self.gcp_conn_id)

        report_file = context["dag"].params["base_folder"] + self.report_file
        conversions_file = context["dag"].params["base_folder"] + self.conversions_file

        conversions_df = self.get_unprocessed_conversions_df(
            report_file, context["dag"].params["floodlight_activity"]
        )

        products_df = self.get_sheet_tab_as_df(
            context["dag"].params["sheet_key"],
            context["dag"].params["products_tab_name"],
        )

        conversions = self.recalculate_conversions(
            conversions_df,
            products_df,
            context["dag"].params["floodlight_activity"],
            context["dag"].params["custom_dimensions"],
            context["dag"].params["column_names"],
        )

        conversions_file = Path(conversions_file)
        conversions_file.parent.mkdir(parents=True, exist_ok=True)
        conversions_file.write_text(json.dumps(conversions))

        context["task_instance"].xcom_push("conversions_file", str(conversions_file))

    def get_unprocessed_conversions_df(self, file, prefix):
        """
        It gets unprocessed conversions since last import

        Args:
            file (string): file to process
            prefix (string): prefix added to processed conversions

        Returns:
            pandas.dataframe
        """

        df = pd.read_csv(file, delimiter=",")
        df["advertiserConversionId"] = df["advertiserConversionId"].astype(str)
        df["advertiserConversionId"] = df["advertiserConversionId"].str.replace(
            prefix, ""
        )
        df.drop_duplicates(subset="advertiserConversionId", keep=False, inplace=True)

        return df

    def get_sheet_tab_as_df(self, sheet_key, tab_name):
        """
        It convert a Google Sheet tab into a Pandas dataframe. The first row
        of the sheet should be the header and contains the columns to use.

        Args:
            sheet_key (string): Google Sheet Id
            tab_name (string): Google Sheet Tab Name

        Returns:
            pandas.dataframe
        """

        sheet = self.gsheet_hook.getSheetValues(sheet_key, sheet_range=tab_name)
        headers = sheet.pop(0)
        df = pd.DataFrame(sheet)

        result_df = df.loc[:, 0 : len(headers) - 1]
        result_df.columns = headers

        return result_df

    def recalculate_conversions(
        self, conversions_df, products_df, fa_id, dimensions, columns,
    ):
        """
        It creates a list of conversions to be uploaded/created since the last
        upload.

        Args:
            conversions_df (pandas.dataframe): unprocessed conversions
            products_df (pandas.dataframe): products collection.
            fa_id (string): Floodlight activity Id to apply the conversions.
            dimensions (dict): A dictionary with the names of the custom
                dimensions names that need to be present in the conversion
            columns (dict): A dictionary with the columns that need to be
                present in the products_df

        Returns:
            list: list of conversions to be uploaded/created
        """
        # pylint: disable=too-many-locals
        conversions = []
        for _, conversion in conversions_df.iterrows():
            if not self.conversion_is_valid(conversion, dimensions):
                logging.info(
                    "The conversion %s is not valid", conversion["conversionId"],
                )
                continue

            productIds = str(conversion[dimensions["productIds"]]).split("|")
            productQuantities = str(conversion[dimensions["productQuantities"]]).split(
                "|"
            )
            productNetPrices = str(conversion[dimensions["productNetPrices"]]).split(
                "|"
            )

            net_revenues = []
            for index, productId in enumerate(productIds):
                if productId == "nan":
                    continue

                products = products_df[
                    products_df[columns["productId"]].astype(str) == str(productId)
                ]
                if len(products) == 0:
                    logging.info(
                        "The productId %s is not in the product sheet", productId
                    )
                    continue

                internal_cost = products.iloc[0][columns["internalCost"]]
                net_revenue = self.calculate_net_revenue(
                    float(internal_cost),
                    int(productQuantities[index]),
                    float(productNetPrices[index]),
                )
                net_revenues.append(net_revenue)

            conversions.append(
                self.generate_new_conversion(conversion, fa_id, net_revenues)
            )

        return conversions

    def conversion_is_valid(self, conversion, dimensions):
        """
        Cheks if the conversion has all the necesary fields to be processed

        Args:
            conversion (pandas.core.series.Series): The conversion that needs
                to be checked
            dimensions (dict): A dictionary with the names of the custom
                fields that need to be present in the conversion

        Returns:
            Bool: True if the conversion is valid
        """

        return (
            conversion[dimensions["productIds"]]
            and conversion[dimensions["productQuantities"]]
            and conversion[dimensions["productNetPrices"]]
        )

    def calculate_net_revenue(self, internal_cost, productQuantities, productNetPrices):
        """
        Calculates the net revenue for a product

        Args:
            internal_cost (float): The cost of the product
            productQuantities (int): The amount of the product in this conversion
            productNetPrices (float): The amount paid by the customer

        Returns:
            float: The net revenue for the product
        """

        net_revenue = (productNetPrices - internal_cost) * productQuantities
        if net_revenue < 0:
            net_revenue = 0

        return net_revenue

    def generate_new_conversion(self, conversion, fa_id, net_revenues):
        """
        Generates the new conversion with the net revenue

        Args:
            conversion (pandas.core.series.Series): The conversion that needs to
            be updated
            fa_id (string): The floodlight activity id
            net_revenues (list): The net revenues for this conversion

        Returns:
            pandas.core.series.Series: The new conversion including the net revenue
        """

        return {
            "conversionId": f"{fa_id}{conversion['conversionId']}",
            "conversionTimestamp": parser.parse(
                conversion["conversionTimestamp"]
            ).timestamp()
            * 1000,
            "agencyId": conversion["agencyId"],
            "advertiserId": conversion["advertiserId"],
            "campaignId": conversion["campaignId"],
            "adGroupId": conversion["adGroupId"],
            "adId": conversion["adId"],
            "clickId": conversion["conversionVisitExternalClickId"],
            "dsConversionId": conversion["conversionId"],
            "state": conversion["status"],
            "type": conversion["conversionType"],
            "segmentationName": fa_id,
            "floodlightOrderId": conversion["floodlightOrderId"],
            "deviceType": conversion["deviceSegment"],
            "segmentationType": "FLOODLIGHT",
            "revenueMicros": int(sum(net_revenues) * 1_000_000),
            "currencyCode": "EUR",
        }

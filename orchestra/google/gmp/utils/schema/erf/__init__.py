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


from orchestra.google.gmp.utils.schema.erf.ExchangeSettings import ExchangeSettings_Schema
from orchestra.google.gmp.utils.schema.erf.UniversalChannel import UniversalChannel_Schema
from orchestra.google.gmp.utils.schema.erf.CostTrackingPixel import CostTrackingPixel_Schema
from orchestra.google.gmp.utils.schema.erf.PartnerRevenueModel import PartnerRevenueModel_Schema
from orchestra.google.gmp.utils.schema.erf.CustomAffinity import CustomAffinity_Schema
from orchestra.google.gmp.utils.schema.erf.UniversalSite import UniversalSite_Schema
from orchestra.google.gmp.utils.schema.erf.FrequencyCap import FrequencyCap_Schema
from orchestra.google.gmp.utils.schema.erf.Creative import Creative_Schema
from orchestra.google.gmp.utils.schema.erf.LineItem import LineItem_Schema
from orchestra.google.gmp.utils.schema.erf.Pixel import Pixel_Schema
from orchestra.google.gmp.utils.schema.erf.EntityCommonData import EntityCommonData_Schema
from orchestra.google.gmp.utils.schema.erf.UserList import UserList_Schema
from orchestra.google.gmp.utils.schema.erf.InventorySource import InventorySource_Schema
from orchestra.google.gmp.utils.schema.erf.FreeFormTarget import FreeFormTarget_Schema
from orchestra.google.gmp.utils.schema.erf.DeviceCriteria import DeviceCriteria_Schema
from orchestra.google.gmp.utils.schema.erf.SupportedExchange import SupportedExchange_Schema
from orchestra.google.gmp.utils.schema.erf.DataPartner import DataPartner_Schema
from orchestra.google.gmp.utils.schema.erf.UserListPricing import UserListPricing_Schema
from orchestra.google.gmp.utils.schema.erf.TargetUnion import TargetUnion_Schema
from orchestra.google.gmp.utils.schema.erf.Target import Target_Schema
from orchestra.google.gmp.utils.schema.erf.Language import Language_Schema
from orchestra.google.gmp.utils.schema.erf.ApprovalStatus import ApprovalStatus_Schema
from orchestra.google.gmp.utils.schema.erf.InsertionOrder import InsertionOrder_Schema
from orchestra.google.gmp.utils.schema.erf.Budget import Budget_Schema
from orchestra.google.gmp.utils.schema.erf.UserListAdvertiserPricing import UserListAdvertiserPricing_Schema
from orchestra.google.gmp.utils.schema.erf.Browser import Browser_Schema
from orchestra.google.gmp.utils.schema.erf.Advertiser import Advertiser_Schema
from orchestra.google.gmp.utils.schema.erf.PartnerCosts import PartnerCosts_Schema
from orchestra.google.gmp.utils.schema.erf.Campaign import Campaign_Schema
from orchestra.google.gmp.utils.schema.erf.GeoLocation import GeoLocation_Schema
from orchestra.google.gmp.utils.schema.erf.SelectionTarget import SelectionTarget_Schema
from orchestra.google.gmp.utils.schema.erf.Isp import Isp_Schema
from orchestra.google.gmp.utils.schema.erf.Partner import Partner_Schema
from orchestra.google.gmp.utils.schema.erf.TargetList import TargetList_Schema
from orchestra.google.gmp.utils.schema.erf.AppCollection import AppCollection_Schema
from orchestra.google.gmp.utils.schema.erf.FloodlightActivity import FloodlightActivity_Schema
from orchestra.google.gmp.utils.schema.erf.NegativeKeywordList import NegativeKeywordList_Schema

Entity_Schema_Lookup = {
    'ExchangeSettings':ExchangeSettings_Schema,
    'UniversalChannel':UniversalChannel_Schema,
    'CostTrackingPixel':CostTrackingPixel_Schema,
    'PartnerRevenueModel':PartnerRevenueModel_Schema,
    'CustomAffinity':CustomAffinity_Schema,
    'UniversalSite':UniversalSite_Schema,
    'FrequencyCap':FrequencyCap_Schema,
    'Creative':Creative_Schema,
    'LineItem':LineItem_Schema,
    'Pixel':Pixel_Schema,
    'EntityCommonData':EntityCommonData_Schema,
    'UserList':UserList_Schema,
    'InventorySource':InventorySource_Schema,
    'FreeFormTarget':FreeFormTarget_Schema,
    'DeviceCriteria':DeviceCriteria_Schema,
    'SupportedExchange':SupportedExchange_Schema,
    'DataPartner':DataPartner_Schema,
    'UserListPricing':UserListPricing_Schema,
    'TargetUnion':TargetUnion_Schema,
    'Target':Target_Schema,
    'Language':Language_Schema,
    'ApprovalStatus':ApprovalStatus_Schema,
    'InsertionOrder':InsertionOrder_Schema,
    'Budget':Budget_Schema,
    'UserListAdvertiserPricing':UserListAdvertiserPricing_Schema,
    'Browser':Browser_Schema,
    'Advertiser':Advertiser_Schema,
    'PartnerCosts':PartnerCosts_Schema,
    'Campaign':Campaign_Schema,
    'GeoLocation':GeoLocation_Schema,
    'SelectionTarget':SelectionTarget_Schema,
    'Isp':Isp_Schema,
    'Partner':Partner_Schema,
    'TargetList':TargetList_Schema,
    'AppCollection':AppCollection_Schema,
    'FloodlightActivity':FloodlightActivity_Schema,
    'NegativeKeywordList':NegativeKeywordList_Schema,
}
